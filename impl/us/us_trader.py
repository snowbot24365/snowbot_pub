import logging
from datetime import datetime, date, timedelta
import time
import pandas as pd
import yfinance as yf # [추가] yfinance 임포트
import pytz

from core.base_trader import BaseTrader
from core.definition import MarketType
from impl.us.us_fetcher import UsFetcher
from config.settings import get_settings_manager
from config.database import get_session, TradeHistory, EvaluationResult, UserBuyTarget, Holdings
from trading.simulator import SimulationEngine
from types import SimpleNamespace
from sqlalchemy import func

logger = logging.getLogger(__name__)

class UsTrader(BaseTrader):
    """
    미국 주식 자동매매 구현체
    """
    def __init__(self):
        super().__init__(MarketType.US)
        self.settings_manager = get_settings_manager()
        self.settings = self.settings_manager.settings
        
        # 실행 모드 확인 (simulation / real_trading)
        self.mode = self.settings.execution_mode_us
        
        if self.mode == "simulation":
            self.simulator = SimulationEngine(market_type="US")

        # API 모드 확인 (real / mock)
        if self.settings.api.kis_trading_account_mode_us == "real":
            self.api_mode = "real"
        else:
            self.api_mode = "mock"
        
        self.fetcher = UsFetcher(mode=self.api_mode) 
        self.trade_cfg = self.settings.trading.us

    def format_ticker_for_yfinance(self, ticker):
        """
        yfinance용 티커 포맷 변환
        예: 'BRK/B' -> 'BRK-B', 'BF.B' -> 'BF-B'
        """
        if not ticker:
            return ticker
            
        # 특수문자를 하이픈(-)으로 변경
        # 보통 BRK.B 또는 BRK/B 형태로 들어오는 경우가 많음
        return ticker.replace("/", "-").replace(".", "-")

    # [Helper] yfinance 기반 Pivot 계산
    def get_pivot_points(self, ticker: str) -> dict:
        """yfinance로 전일 데이터를 가져와 피봇 지지선(S1, S2) 등을 계산"""
        try:
            ex_ticker = self.format_ticker_for_yfinance(ticker)
            # 전일 데이터가 필요하므로 최소 2일치 데이터를 요청 (주말/휴일 고려 5일 권장되나 2d로 시도)
            hist = yf.Ticker(ex_ticker).history(period="5d", interval="1d", timeout=3) # 안전하게 5일치 요청
            
            if len(hist) < 2:
                return {'pp': pd.NA, 's1': pd.NA, 's2': pd.NA}

            # 전일 데이터 추출 (마지막 행이 오늘(장중)일 수 있으므로 확인 필요)
            # yfinance는 장중에는 오늘 날짜가 마지막 행에 포함됨.
            # 확실하게 '전일(Completed)' 데이터를 가져오기 위해 뒤에서 두번째 사용
            prev_day = hist.iloc[-2]
            
            high = float(prev_day['High'])
            low = float(prev_day['Low'])
            close = float(prev_day['Close'])

            # 피봇 포인트 계산
            pivot = (high + low + close) / 3
            
            # 지지선 (Support) 계산
            s1 = (2 * pivot) - high
            s2 = pivot - (high - low)
            
            return {'pp': pivot,'s1': s1, 's2': s2}

        except Exception as e:
            logger.warning(f"티커 '{ticker}'의 피봇 포인트 조회 실패: {e}")
            return {'pp': pd.NA, 's1': pd.NA, 's2': pd.NA}

    # [BaseTrader 추상 메서드 구현]
    def get_current_price(self, code: str) -> float:
        data = self.fetcher.get_current_price(code)
        return data['price'] if data else 0.0

    def get_balance(self) -> dict:
        return self._get_account_balance()

    def buy_order(self, code: str, qty: int, price: float) -> bool:
        if self.mode != "simulation":
            res = self.fetcher.send_order(
                "buy", code, qty, price, 
                self._get_account_no(), self._get_account_cd()
            )
            return res['success']
        else:
            res = self.simulator.buy(code, qty, price, 'auto')
            return res.success

    def sell_order(self, code: str, qty: int, price: float) -> bool:
        if self.mode != "simulation":
            res = self.fetcher.send_order(
                "sell", code, qty, price, 
                self._get_account_no(), self._get_account_cd()
            )
            return res['success']
        else:
            res = self.simulator.sell(code, qty, price, 'auto')
            return res.success

    def run(self) -> str:
        logs = []
        if self.mode != "simulation" and not self.fetcher.is_configured():
            return "US Trader 실패: API 설정이 필요합니다."

        try:
            time.sleep(0.2)
            balance = self._get_account_balance()
            if not balance: return "US Trader 실패: 계좌 정보를 가져올 수 없습니다."
            
            sell_logs = self._process_selling(balance['holdings'])
            logs.extend(sell_logs)
            
            if self.trade_cfg.buy_enabled:
                if self.mode == "simulation":
                    balance = self._get_account_balance()
                buy_logs = self._process_buying(balance['deposit'], balance['holdings'])
                logs.extend(buy_logs)
            else:
                logs.append("US 매수 비활성화됨")
            
            return "\n".join(logs)
        except Exception as e:
            logger.error(f"US Run Error: {e}")
            return f"오류 발생: {e}"

    def _get_account_balance(self):
        if self.mode == "simulation":
            info = self.simulator.get_account_info()
            holdings = []
            for h in info.holdings:
                holdings.append({
                    'pdno': h.item_cd,
                    'prdt_name': h.item_nm,
                    'hldg_qty': h.qty,
                    'evlu_pfls_rt': h.profit_rate,
                    'evlu_amt': h.eval_amt,
                    'pchs_avg_pric': h.avg_price,
                    'prpr': h.current_price
                })
            return {'deposit': info.balance, 'holdings': holdings}
        else:
            return self.fetcher.get_account_balance(
                self._get_account_no(), self._get_account_cd()
            )

    def _process_selling(self, holdings):
        """
        매도 로직 수행 
        (익절 도달 시 -> 당일 고가 대비 하락폭 체크 후 매도/홀딩 결정)
        """
        logs = []
        if not holdings:
            return logs
            
        # 설정 로드
        cfg = self.trade_cfg
        
        sell_up = cfg.sell_up_rate       # 익절률 (예: 10%)
        sell_down = cfg.sell_down_rate   # 손절률 (예: -5%)
        
        # [설정]
        sell_split_rate = getattr(cfg, 'sell_split_rate', 100.0) # 분할 매도
        sell_hold_rate = getattr(cfg, 'sell_hold_rate', 0.0)     # 물타기 보류 비율
        use_loss_cut = getattr(cfg, 'use_loss_cut', True)        # 손절 사용 여부
        max_buy_amt = getattr(cfg, 'max_buy_amount', 1000)
        
        # [신규] 트레일링 스탑 설정
        use_ts = getattr(cfg, 'trailing_stop_enabled', False)
        ts_rate = getattr(cfg, 'trailing_stop_rate', 3.0) # 고점 대비 3% 하락 시 매도

        with get_session() as session:
            # 현재 보유하지 않은 종목은 DB에서 정리
            current_codes = [h.get('pdno') for h in holdings]
            session.query(Holdings).filter(Holdings.item_cd.notin_(current_codes)).delete(synchronize_session=False)

            for item in holdings:
                item_cd = item.get('pdno')
                item_nm = item.get('prdt_name')
                qty = int(item.get('hldg_qty', 0))
                profit_rate = float(item.get('evlu_pfls_rt', 0)) # 수익률
                eval_amt = float(item.get('evlu_amt', 0))
                current_price = float(item.get('prpr', 0))
                
                if qty <= 0: continue
                
                # 매도 수량 계산
                target_qty = int(qty * (sell_split_rate / 100))
                if target_qty < 1: target_qty = 1

                action = None
                reason = ""
                
                # 물타기 구간 체크
                is_watering_zone = False
                if sell_hold_rate > 0:
                    hold_threshold = max_buy_amt * (sell_hold_rate / 100)
                    if eval_amt < hold_threshold:
                        is_watering_zone = True

                # =========================================================
                # [A] Holdings 테이블 Merge (Insert/Update) 및 최고가 갱신
                # =========================================================
                # TS 계산을 위한 기준 고가 (초기값: 현재가 또는 매수가)
                base_high_price = current_price

                try:
                    # 1. DB 조회
                    db_holding = session.query(Holdings).filter_by(item_cd=item_cd).first()
                    
                    if db_holding:
                        # [Update] 기존 기록 있음 -> 최고가 비교 및 갱신
                        stored_highest = db_holding.highest_price
                        
                        # 핵심: 기존 최고가 vs 현재가 중 더 큰 값 선택
                        new_highest = max(stored_highest, current_price)
                        
                        db_holding.current_price = current_price
                        db_holding.highest_price = new_highest
                        db_holding.quantity = qty
                        db_holding.updated_at = datetime.now()
                        
                        base_high_price = new_highest # 갱신된 최고가를 TS 기준으로 사용
                        
                    else:
                        # [Insert] 신규 기록 -> 현재가를 최고가로 설정
                        new_holding = Holdings(
                            item_cd=item_cd,
                            market_type=getattr(self, 'market_type', 'US'),
                            item_nm=item_nm,
                            quantity=qty,
                            current_price=current_price,
                            highest_price=current_price, # 첫 기록 시 최고가 = 현재가
                            buy_date=date.today().strftime('%Y%m%d'),
                            updated_at=datetime.now()
                        )
                        session.add(new_holding)
                        base_high_price = current_price
                    
                    # 변경사항 즉시 커밋 (다음 루프나 재시작 시 반영되도록)
                    session.commit()

                except Exception as e:
                    logger.error(f"Holdings DB Sync Error ({item_nm}): {e}")
                    session.rollback()
                    # [Fail-Safe] DB 오류 시 API 당일 고가 사용 시도
                    p_data = self.fetcher.get_current_price(item_cd)
                    if p_data: base_high_price = float(p_data.get('high', current_price))
                
                # [디버깅 로그 1] 전체 설정값 확인
                # print(f">>> [DEBUG 매도설정] 익절:{sell_up}%, 손절:{sell_down}%, TS사용:{use_ts}({ts_rate}%), 손절기능:{use_loss_cut}, 물타기보류:{sell_hold_rate}%")
                logs.append(f"[🔍매도Task -  종목: {item_nm}] 수익률:{profit_rate}% | 평가금:{eval_amt:.0f} (물타기존:{is_watering_zone}, 기준:{hold_threshold:.0f})")
                # =========================================================
                # [1] 익절 조건 판단 (트레일링 스탑 로직 포함)
                # =========================================================
                if profit_rate >= sell_up:

                    if is_watering_zone:
                        logs.append(f"         └─ [⏩매도Task -  익절패스] 매도 보류 구간임") # 로그 추가
                        pass
                    
                    # A. 트레일링 스탑(TS) 사용 시
                    elif use_ts:
                        if base_high_price > 0 and current_price > 0:
                            # 고점 대비 하락률 계산
                            drop_rate = ((base_high_price - current_price) / base_high_price) * 100
                            
                            logs.append(f"         └─ [🔍TS감시] {item_nm}: 최고가 {base_high_price:,.2f} 대비 -{drop_rate:.2f}% (설정: -{ts_rate}%)")

                            if drop_rate >= ts_rate:
                                action = "SELL"
                                reason = f"TS 발동 (최고가 {base_high_price:,.2f} 대비 -{drop_rate:.2f}% 하락)"
                            else:
                                logs.append(f"🔒 [TS홀딩] {item_nm} ({profit_rate:.2f}%) - 고점대비 -{drop_rate:.2f}% (아직 견조함)")
                                continue 
                        else:
                            # 가격 정보 오류 시 안전하게 익절로 처리할지, 홀딩할지 결정 (여기선 홀딩)
                            logs.append(f"🔒 [TS홀딩] {item_nm} - 가격정보 오류")
                            continue

                    # B. 트레일링 스탑 미사용 시 (기존 로직)
                    else:
                        action = "SELL"
                        reason = f"익절 조건 도달 ({profit_rate:.2f}% >= {sell_up}%)"

                # =========================================================
                # [2] 손절 조건 판단
                # =========================================================
                elif profit_rate <= sell_down:
                    if not use_loss_cut:
                        logs.append(f"         └─ [⏩매도Task -  손절패스] 손절기능 OFF 상태") # 로그 추가
                        pass
                    elif is_watering_zone:
                        logs.append(f"         └─ [⏩매도Task -  손절패스] 매도 보류 구간임") # 로그 추가
                        pass
                    else:
                        action = "SELL"
                        reason = f"손절 조건 도달 ({profit_rate:.2f}% <= {sell_down}%)"
                
                # [디버깅 로그 4] 최종 결정 확인
                if action == "SELL":
                    logs.append(f"    └─ [🚀매도Task -  결정] 매도 실행! 사유: {reason}")
                
                # =========================================================
                # [3] 매도 실행
                # =========================================================
                if action == "SELL":
                    if self.mode != "simulation":
                        success = self.sell_order(item_cd, target_qty, 0)
                        if success:
                            split_msg = f"(분할 {sell_split_rate}%)" if sell_split_rate < 100 else "(전량)"
                            msg = f"[💰매도Task - US매도주문{split_msg}] {item_nm} {target_qty}주 - {reason}"
                            logs.append(msg)
                            # logger.info(msg)
                            self._save_trade_history(item_cd, 'sell', target_qty, 0, reason)
                        else:
                            logs.append(f"❌매도Task -  [US매도실패] {item_nm}")
                    else:
                        sim_result = self.simulator.sell(item_cd, target_qty, trade_source="auto")
                        success = sim_result.success
                        
                        if not success:
                            logger.error(f"Sim Sell Fail: {sim_result.message}")                    
        return logs
    
    # [수정] 미국 주식용 날짜 계산 함수
    def get_trading_date(self, market_type='US'):
        now = datetime.now()
        
        if market_type == 'US':
            # 미국장은 한국 시간 09시 이전(새벽~아침)에 실행 중이라면 
            # '어제' 날짜를 거래일(Trading Day)로 간주해야 함
            if now.hour < 9:
                return (now - timedelta(days=1)).strftime('%Y%m%d')
                
        # 한국장(KR)이거나 09시 이후라면 현재 날짜 사용
        return now.strftime('%Y%m%d')

    def _process_buying(self, deposit, holdings):
        """
        매수 로직 수행:
        1. 보유 종목 수 제한 확인 (신규 진입 시에만 적용)
        2. 추가 매수(물타기/불타기) 대상 식별
        3. 매수 후보 선정 (사용자 + 보유종목 + 알고리즘)
        """
        logs = []
        
        current_holdings_count = len(holdings)
        limit_count = self.trade_cfg.limit_count
        max_per_trade = self.trade_cfg.max_buy_amount # 종목당 최대 매수 금액
        
        # ---------------------------------------------------------
        # [Step 1] 추가 매수 가능 종목 식별 (보유 중 & 최대금액 미만)
        # ---------------------------------------------------------
        # API 잔고 데이터에서 '평가금액'을 확인하여 추가 매수 여력이 있는지 확인
        fillable_codes = set()
        for h in holdings:
            code = h.get('pdno')
            # 평가금액 (없으면 0 처리)
            eval_amt = float(h.get('evlu_amt', 0))
            
            # 현재 평가금액이 설정된 최대 금액보다 작으면 추가 매수 후보
            if eval_amt < max_per_trade:
                fillable_codes.add(code)

        # ---------------------------------------------------------
        # [Step 2] 매수 중단 조건 확인
        # ---------------------------------------------------------
        # 보유 종목이 꽉 찼고, 추가 매수할 종목도 없다면 매수 로직 종료
        if current_holdings_count >= limit_count and not fillable_codes:
            logs.append(f"⛔매수Task - 매수 생략: 최대 보유 종목 수 도달 ({current_holdings_count}/{limit_count}) 및 추가매수 대상 없음")
            return logs

        # 예산 계산 (기존 로직 유지)
        budget_by_rate = int(deposit * (self.trade_cfg.buy_rate / 100))
        target_amount_base = min(max_per_trade, budget_by_rate)
        
        if target_amount_base < 10:
            logs.append("⛔매수Task - US 매수 생략: 예산 부족")
            return logs

        # ---------------------------------------------------------
        # [Step 3] 매수 후보 선정
        # ---------------------------------------------------------
        # today_str = date.today().strftime('%Y%m%d')
        today_str = self.get_trading_date('US')
        
        with get_session() as session:
            # A. 당일 매수 완료 종목 제외
            today_bought_codes = {
                row[0] for row in session.query(TradeHistory.item_cd).filter(
                    TradeHistory.trade_date == today_str,
                    TradeHistory.market_type == 'US',
                    TradeHistory.trade_type.in_(['buy', 'B'])
                ).all()
            }

            # B. [우선순위 1] 사용자 지정 매수 대상 조회
            user_targets = session.query(UserBuyTarget).filter_by(market_type='US').all()
            
            # C. [우선순위 2] 보유 중인 종목 중 추가 매수 가능 종목 (DB 조회 X)
            # [수정] EvaluationResult 조회 없이 잔고 정보를 바탕으로 직접 후보 객체 생성
            holding_candidates = []
            if fillable_codes:
                for h in holdings:
                    code = h.get('pdno')
                    # API 응답이 문자열일 수 있으므로 int로 변환하고, 없으면 0으로 처리
                    try:
                        current_qty = int(h.get('hldg_qty', 0))
                    except (ValueError, TypeError):
                        current_qty = 0

                    # [수정] 코드가 목록에 있고 AND 수량이 0보다 클 때만 처리
                    if code in fillable_codes and current_qty > 0:
                        # SQLAlchemy 객체와 호환되도록 SimpleNamespace 사용
                        # 점수는 없으므로 0점 처리하지만, 매수 로직은 진행됨
                        cand = SimpleNamespace(
                            item_cd=code,
                            item_nm=h.get('prdt_name'),
                            total_score=0,      # 평가 점수 없음
                            market_type='US'    # 시장 구분
                        )
                        holding_candidates.append(cand)

            # D. [우선순위 3] 알고리즘 평가 우수 종목 조회
            # (보유 종목 수가 꽉 찼다면 신규 종목은 조회할 필요 없음 -> fillable_codes가 없으면 신규 진입 불가)
            algo_targets = []
            
            latest_us_date = session.query(func.max(EvaluationResult.base_date))\
            .filter(EvaluationResult.market_type == 'US').scalar()

            # 신규 진입이 가능한 슬롯이 남아있을 때만 알고리즘 종목 조회
            if len(holdings) < self.trade_cfg.limit_count:
                algo_targets = session.query(EvaluationResult).filter(
                    EvaluationResult.base_date == latest_us_date,
                    EvaluationResult.market_type == 'US',
                    EvaluationResult.total_score >= self.settings.evaluation.us.min_total_score,
                    EvaluationResult.is_buy_candidate == True
                ).order_by(EvaluationResult.total_score.desc()).limit(self.trade_cfg.limit_count * 3).all()
            
            # E. 후보 리스트 병합 (우선순위: 사용자 > 보유종목 > 알고리즘)
            candidates = []
            seen_codes = set()
            
            # 1) 사용자 타겟
            for t in user_targets:
                candidates.append(t)
                seen_codes.add(t.item_cd)
                
            # 2) 보유 종목 (추가 매수) - [수정됨] 직접 생성한 객체 리스트 사용
            for t in holding_candidates:
                if t.item_cd not in seen_codes:
                    candidates.append(t)
                    seen_codes.add(t.item_cd)
            
            # 3) 알고리즘 타겟 (신규 진입)
            for t in algo_targets:
                if t.item_cd not in seen_codes:
                    candidates.append(t)
                    seen_codes.add(t.item_cd)
            
            # ---------------------------------------------------------
            # [Step 4] 매수 실행 루프
            # ---------------------------------------------------------
            buy_count = 0
            slots_available = limit_count - current_holdings_count
            for cand in candidates:
                if cand.item_cd in today_bought_codes: continue
                
                # [중요] 신규 종목 진입 제한 확인
                # 현재 보유중이지 않은 종목(신규)인데 슬롯이 없다면 건너뜀
                is_new_stock = cand.item_cd not in fillable_codes # fillable에 없으면 신규로 간주
                if is_new_stock and slots_available <= 0:
                    continue

                time.sleep(0.1)
                
                current_price = 0.0
                if self.mode == "simulation":
                    p_data = self.fetcher.get_current_price(cand.item_cd)
                    if p_data: current_price = float(p_data['price'])
                else:
                    curr_info = self.fetcher.get_stock_info(cand.item_cd)
                    if curr_info:
                        current_price = float(curr_info.get('price', 0))
                
                if current_price <= 0:
                    for market in ['NAS', 'NYS', 'AMS']:
                        curr_info = self.fetcher.get_current_price_market(cand.item_cd, market)
                        
                        if curr_info:
                            current_price = float(curr_info.get('price', 0))
                            # 정상적인 가격을 가져왔다면 더 이상 다른 거래소를 조회할 필요 없이 탈출
                            if current_price > 0:
                                break

                if current_price <= 0: 
                    logs.append(f"⏩매수Task - [{cand.item_nm}] 현재가를 가져오지 못해 스킵")
                    continue
                if current_price < 1.0: 
                    logs.append(f"⏩매수Task - [{cand.item_nm}] 동전주 스킵 (현재가: {current_price:,.0f})")
                    continue
                
                # =========================================================
                # [수정] yfinance 기반 Pivot 지지선 확인
                # =========================================================
                buy_criteria = getattr(self.trade_cfg, 'buy_price_criteria', 'current')
                if buy_criteria == 'current':
                    pass
                else:
                    try:
                        pivot_data = self.get_pivot_points(cand.item_cd)
                                                # 피벗 데이터가 확보된 경우 기준가 비교
                        if pivot_data:
                            target_price = 0
                            criteria_name = ""

                            # 설정에 따른 기준가(상한선) 결정
                            if buy_criteria == 'pvt':
                                target_price = pivot_data['pp']
                                criteria_name = "피벗(Pivot)"
                            elif buy_criteria == 'pvt_sup1':
                                target_price = pivot_data['s1']
                                criteria_name = "1차지지(S1)"
                            elif buy_criteria == 'pvt_sup2':
                                target_price = pivot_data['s2']
                                criteria_name = "2차지지(S2)"
                            elif buy_criteria == 'pvt_avg':
                                target_price = (pivot_data['pp'] + pivot_data['s1'] + pivot_data['s2']) / 3
                                criteria_name = "지지선평균(Avg)"
                            
                            # [판단] 현재가가 기준가보다 높으면 매수 보류
                            # (즉, 기준가 이하로 떨어져야 매수)
                            if target_price > 0 and current_price > target_price:
                                msg = f"✋매수Task - [US매수보류] {cand.item_nm} - 현재가({current_price:,.0f}) > {criteria_name}({target_price:,.0f})"
                                logs.append(msg)
                                # logger.info(msg)
                                continue # 다음 종목으로 넘어감
                        else:
                            # 피벗 데이터 계산 실패 시
                            # 보수적인 접근: 데이터를 못 구했으므로 매수 스킵? 
                            # 혹은 로그 남기고 진행? (여기선 로그 남기고 진행으로 처리)
                            logger.warning(f"⏩매수Task - US피벗 데이터 부족으로 기준가 체크 건너뜀: {cand.item_nm}")

                            
                    except Exception as e:
                        logger.error(f"⚠️매수Task - US매수 기준가(Pivot) 체크 중 오류: {e}")
                        continue
                # =========================================================
                # H. 매수 수량 계산
                # =========================================================
                current_invested = 0
                for h in holdings:
                    if h.get('pdno') == cand.item_cd:
                        current_invested = float(h.get('evlu_amt', 0))
                        break
                
                # 남은 한도 계산
                amount_limit = max_per_trade - current_invested
                real_target_amount = min(target_amount_base, amount_limit)
                qty = int(real_target_amount // current_price)
                if qty <= 0: 
                    logs.append(f"⏩매수Task - [{cand.item_nm}] 매수 가능 수량 부족으로 스킵 (계산수량: {qty}, 가용예산: {real_target_amount:,.0f}, 현재가: {current_price:,.0f})")
                    continue

                # =========================================================
                # [NEW] I. 사전 주문 가능 여부 체크 (실전 모드)
                # =========================================================
                if self.mode != "simulation":
                    # check_buy_limit_us 호출
                    # 거래소코드(exchange_cd)와 단가(current_price) 필수
                    purchasable_info = self.fetcher.check_buy_limit_us(
                        account_no=self._get_account_no(),
                        account_cd=self._get_account_cd(),
                        stock_code=cand.item_cd,
                        price=current_price
                    )
                    
                    if purchasable_info:
                        # 외화 기준 최대 가능 수량 (통합증거금이면 max_qty_integ 사용 고려)
                        max_qty = int(purchasable_info.get('max_qty', 0))
                        
                        if qty > max_qty:
                            if max_qty > 0:
                                msg = f"⚠️매수Task - [US수량조정] {cand.item_nm} ({qty} -> {max_qty}) / 사유: 증거금(USD) 부족"
                                logs.append(msg)
                                qty = max_qty
                            else:
                                msg = f"✋매수Task - [US매수불가] {cand.item_nm} - 주문 가능 수량 0 (USD 부족)"
                                logs.append(msg)
                                continue
                
                if self.mode == "simulation":
                    res = self.simulator.buy(cand.item_cd, qty, current_price, 'auto')
                    if res.success:
                        msg = f"📈매수Task - [US시뮬매수] {cand.item_nm} {qty}주"
                        logs.append(msg)
                        # logger.info(msg)
                        today_bought_codes.add(cand.item_cd)
                        buy_count += 1
                else:
                    res = self.fetcher.send_order(
                        "buy", cand.item_cd, qty, current_price,
                        self._get_account_no(), self._get_account_cd()
                    )
                    if res['success']:
                        msg = f"📈매수Task - [US매수성공] {cand.item_nm} {qty}주"
                        logs.append(msg)
                        logger.info(msg)
                        score = getattr(cand, 'total_score', 0)
                        self._save_trade_history(cand.item_cd, 'buy', qty, current_price, f"점수{score}/US/Pivot지지")
                        today_bought_codes.add(cand.item_cd)
                        buy_count += 1
                    else:
                        logs.append(f"❌매수Task - [US매수실패] {res.get('message')}")
                        
        if buy_count == 0 and not logs:
            logs.append("⛔매수Task - 매수 대상 종목 없음")

        return logs

    def _get_account_no(self):
        return self.settings.api.kis_real_account_no_us if self.api_mode == "real" else self.settings.api.kis_mock_account_no_us

    def _get_account_cd(self):
        return self.settings.api.kis_real_account_cd_us if self.api_mode == "real" else self.settings.api.kis_mock_account_cd_us

    def _save_trade_history(self, item_cd, trade_type, qty, price, reason):
        try:
            utc_now = datetime.now(pytz.utc)
            # 2. US/Eastern (뉴욕 시간)으로 변환 (썸머타임 자동 반영)
            us_dt = utc_now.astimezone(pytz.timezone('US/Eastern'))
            # 3. 포맷팅
            trade_time = us_dt.strftime('%H%M%S')
            with get_session() as session:
                history = TradeHistory(
                    market_type='US',
                    item_cd=item_cd,
                    trade_date=self.get_trading_date('US'),
                    trade_time=trade_time,
                    trade_type=trade_type,
                    quantity=qty,
                    price=price,
                    amount=qty * price,
                    trade_source="auto",
                    trade_reason=reason,
                    created_at=datetime.now()
                )
                session.add(history)
                session.commit()
        except Exception as e:
            logger.error(f"US DB 기록 실패: {e}")