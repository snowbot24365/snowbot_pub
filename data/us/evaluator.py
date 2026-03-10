"""
미국 주식 평가 로직 모듈 (US Evaluator)
- 설정(Config) 기반 점수 계산 적용
- 안전망(Safety Net) 검증 로직 추가
- SRIM, Pivot, 적정주가 분석 통합
"""

import logging
import numpy as np
import pandas as pd
import yfinance as yf
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Callable, List
from sqlalchemy import func
import time

from config.settings import get_settings_manager
from config.database import (
    get_session, ItemMst, ItemPrice, FinancialSheet, ItemEquity, EvaluationResult
)

logger = logging.getLogger(__name__)

# =========================================================
# 1. 데이터 구조 (DTO)
# =========================================================

@dataclass
class SwingData:
    """평가에 필요한 데이터 집합 (점수 계산용)"""
    item_cd: str = ""
    item_nm: str = ""
    stck_clpr: float = 0.0
    
    # MAs (이동평균선)
    ma5: float = 0.0
    ma10: float = 0.0
    ma20: float = 0.0
    ma30: float = 0.0
    ma60: float = 0.0
    ma120: float = 0.0
    
    # Financials
    revenue_growth: float = 0.0 # grs
    profit_margin: float = 0.0  # bsop_prfi_inrt
    roe_val: float = 0.0
    lblt_rate: float = 999.0    # 부채비율 (debt_ratio)
    thtr_ntin: float = 0.0      # 당기순이익 (net_income)
    rsrv_rate: float = 0.0      # 유보율
    cf_oa: float = 0.0          # 영업현금흐름
    
    # Activity (자산회전율 변화율 - US용)
    asset_turnover_growth: float = -99.0 
    
    # Equity / Valuation
    shares: int = 1             # 상장주식수
    market_cap: int = 0         # 시가총액 (hts_avls)
    per: float = 0.0
    pbr: float = 0.0
    eps: float = 0.0            # SRIM용
    dividend_yield: float = 0.0

    # 수급 분석용 (미국 주식용)
    institutional_ownership: float = 0.0 # 기관 보유 비중 (%)
    
    # Price Rate (고점/저점 대비 등락률)
    high_rate: float = -99.0    # w52_high 대비
    low_rate: float = -99.0     # w52_low 대비
    frgn_rate: float = 0.0      # 외국인 소진율
    
    w52_high: float = 0.0
    w52_low: float = 0.0

    # KPI 지표 (RSI, OBV)
    rsi_14: float = 50.0 
    obv_trend: int = 0          # KR용 (1, -1, 0)
    obv_val: float = 0.0        # US용 (절대값)
    obv_ma: float = 0.0         # US용 (이평)
    
    # Buy (Volume / Supply)
    acml_vol: int = 0           # 거래량
    avg_volume: int = 0         # 평균 거래량
    frgn_hldn_qty: int = 0 
    frgn_ntby_qty: int = 0
    pgtr_ntby_qty: int = 0

@dataclass
class EvaluationScore:
    """평가 결과 점수"""
    item_cd: str = ""
    item_nm: str = ""
    total_score: int = 0
    
    sheet_score: int = 0
    trend_score: int = 0
    price_score: int = 0
    kpi_score: int = 0
    buy_score: int = 0
    avls_score: int = 0
    per_score: int = 0
    pbr_score: int = 0
    
    is_buy_candidate: bool = False
    
    # Analysis Result
    srim_price: float = 0.0
    srim_pass: bool = False
    fair_value: float = 0.0
    pivot_s1: float = 0.0
    pivot_s2: float = 0.0
    pivot_r1: float = 0.0
    pivot_r2: float = 0.0
    
    # Safety Net Status
    cashflow_pass: bool = False
    activity_pass: bool = False
    dividend_pass: bool = False
    roe_pass: bool = False
    safety_fail_reason: str = ""


# =========================================================
# 2. 미국 주식 점수 계산기 (UsEvaluator)
# =========================================================

class UsEvaluator:
    def __init__(self):
        self.settings_manager = get_settings_manager()

    def evaluate(self, d: SwingData) -> EvaluationScore:
        score = EvaluationScore(item_cd=d.item_cd, item_nm=d.item_nm)
        
        # [설정 로드] 시장별 설정값 가져오기
        eval_conf = self.settings_manager.settings.evaluation.us
        
        # 각 항목별 점수 계산 (메서드 분리)
        score.sheet_score = self._cal_sheet_score(d, eval_conf)
        score.trend_score = self._cal_trend_score(d, eval_conf)
        score.price_score = self._cal_price_score(d, eval_conf)
        score.kpi_score   = self._cal_kpi_score(d, eval_conf)
        score.buy_score   = self._cal_buy_score(d, eval_conf)
        score.avls_score  = self._cal_avls_score(d, eval_conf)
        score.per_score   = self._cal_per_score(d, eval_conf)
        score.pbr_score   = self._cal_pbr_score(d, eval_conf)
        
        # [총점 계산] 가중치 적용
        w = eval_conf 
        w_sum = (
            score.sheet_score * w.weight_sheet +
            score.trend_score * w.weight_trend + 
            score.price_score * w.weight_price + 
            score.kpi_score   * w.weight_kpi + 
            score.buy_score   * w.weight_buy + 
            score.avls_score  * w.weight_avls + 
            score.per_score   * w.weight_per + 
            score.pbr_score   * w.weight_pbr
        )
        
        # 가중치 합계
        total_weight = (
            w.weight_sheet + w.weight_trend + w.weight_price +
            w.weight_kpi + w.weight_buy + w.weight_avls +
            w.weight_per + w.weight_pbr
        )
        
        # 점수 정규화: (가중평균) * (항목 수 8)
        # 이렇게 하면 가중치를 3.0으로 높여도 만점은 항상 40점으로 유지됨
        if total_weight > 0:
            final_score = (w_sum / total_weight) * 8
        else:
            final_score = 0
            
        # 반올림하여 정수형 저장
        score.total_score = int(round(final_score))
                
        return score

    # --- 개별 점수 계산 메서드 ---

    def _cal_sheet_score(self, d: SwingData, conf) -> int:
        """재무 안정성 점수"""
        pt = 0
        if d.revenue_growth >= conf.threshold_grs: pt += 1
        if d.profit_margin >= conf.threshold_bsop_prfi_inrt: pt += 1
        if d.lblt_rate <= conf.threshold_lblt_rate: pt += 1
        if d.thtr_ntin > 0: pt += 1
        if d.roe_val >= 5.0: pt += 1 # 최소 ROE 기준
        return pt

    def _cal_trend_score(self, d: SwingData, conf) -> int:
        """추세 점수"""
        pt = 0
        p = d.stck_clpr
        
        if conf.trend_alignment == "REGULAR":
            # 정배열 모드 (상승 추세)
            if p >= d.ma5: pt += 1
            if d.ma10 > d.ma5: pt += 1
            if d.ma20 > d.ma10: pt += 1
            if d.ma60 > d.ma20: pt += 1
            # 5번째 조건: 장기 정배열 등
            if d.ma60 > d.ma30: pt += 1 
        else:
            # 기본 모드 (레퍼런스 로직)
            if d.ma5 > p: pt += 1
            if d.ma10 > d.ma5: pt += 1
            if d.ma20 > d.ma10: pt += 1
            if d.ma30 > d.ma20: pt += 1
            if d.ma60 > d.ma30: pt += 1
            
        return min(5, pt)

    def _cal_price_score(self, d: SwingData, conf) -> int:
        """가격 메리트 점수 (낙폭과대 선호)"""
        score = 0
        p = d.stck_clpr
        
        # 1. 고점 대비 하락률 (High Rate Score)
        if d.w52_high > 0:
            high_rate = (d.w52_high - p) / d.w52_high * 100
            bench = conf.high_rate_benchmark
            step = conf.high_rate_step
            
            if high_rate > bench: score = 5
            elif high_rate > bench - step: score = 4
            elif high_rate > bench - (step*2): score = 3
            elif high_rate > bench - (step*3): score = 2
            elif high_rate > bench - (step*4): score = 1
        
        # 2. 저점 대비 상승률 (Low Rate Penalty)
        penalty = 0
        if d.w52_low > 0:
            low_rate = (p - d.w52_low) / d.w52_low * 100
            l_bench = conf.low_rate_benchmark
            l_step = conf.low_rate_step
            
            if low_rate > l_bench + (l_step*2): penalty = 3
            elif low_rate > l_bench + l_step: penalty = 2
            elif low_rate > l_bench: penalty = 1
            
        return max(0, score - penalty)

    def _cal_kpi_score(self, d: SwingData, conf) -> int:
        """보조지표(RSI, OBV) 점수"""
        rsi_score = 0
        if d.rsi_14 > 70: rsi_score = -2
        elif d.rsi_14 < 30: rsi_score = 2
        
        obv_score = 0
        if d.obv_val > 0: obv_score = 2
        elif d.obv_val < 0: obv_score = -2
        
        total = rsi_score + obv_score
        
        if total == 4:
            total += 1
            
        return min(5, max(0, total))

    def _cal_buy_score(self, d: SwingData, conf) -> int:
        """
        기관 보유율과 거래량으로 수급 점수 계산 (Reference Logic 반영)
        - institutional_ownership: 기관 보유 비중 (%)
        - volume_rate: 평균 거래량 대비 현재 거래량 (%)
        """
        # 1. 기관 보유율 (percent)
        inst_own = d.institutional_ownership # 0~100

        # 2. 거래량 비율 (volume_rate)
        volume_rate = 0.0
        if d.avg_volume > 0:
            volume_rate = (d.volume / d.avg_volume) * 100

        # 3. 점수 계산 (Reference Logic)
        if volume_rate > 150 and inst_own > 70: return 5
        if volume_rate > 150 or inst_own > 70: return 4
        if volume_rate > 100 and inst_own > 50: return 3
        if volume_rate > 100 or inst_own > 50: return 2
        return 1

    def _cal_avls_score(self, d: SwingData, conf) -> int:
        """시가총액 점수"""
        target_cap = d.market_cap / 1_000_000 # 백만달러 환산
        bench = conf.avls_benchmark
        step = conf.avls_step
        
        if target_cap >= bench: return 5
        elif target_cap >= bench - step: return 4
        elif target_cap >= bench - (step*2): return 3
        elif target_cap >= bench - (step*3): return 2
        return 1

    def _cal_per_score(self, d: SwingData, conf) -> int:
        """PER 점수"""
        if d.per <= 0: return 0
        bench = conf.per_benchmark
        step = conf.per_step
        
        if 0 < d.per < bench: return 5
        elif d.per < bench + step: return 4
        elif d.per < bench + (step*2): return 3
        elif d.per < bench + (step*3): return 2
        return 1

    def _cal_pbr_score(self, d: SwingData, conf) -> int:
        """PBR 점수"""
        if d.pbr <= 0: return 0
        bench = conf.pbr_benchmark
        step = conf.pbr_step
        
        if 0 < d.pbr < bench: return 5
        elif d.pbr < bench + step: return 4
        elif d.pbr < bench + (step*2): return 3
        elif d.pbr < bench + (step*3): return 2
        return 1

    # --- 안전망 및 SRIM 계산 ---

    def check_safety_nets(self, d: SwingData, score: EvaluationScore) -> Dict[str, bool]:
        """안전망(필터링) 검사 및 결과 반환"""
        # 종합 판단
        is_safe = True
        reasons = []

        t_set = self.settings_manager.settings.trading.us
        
        # 1. SRIM
        if t_set.use_srim_filter and not score.srim_pass:
            pass # 외부에서 처리됨 (score.srim_pass는 이미 계산됨)

        # 2. ROE
        if d.roe_val >= 8.0: score.roe_pass = True
        
        # 3. 배당
        if d.dividend_yield > 0: score.dividend_pass = True
        
        # 4. 현금흐름
        if d.cf_oa > 0: score.cashflow_pass = True
        
        # 5. 활동성
        if d.asset_turnover_growth != -99.0 and d.asset_turnover_growth >= -0.05:
            score.activity_pass = True
        elif d.asset_turnover_growth == -99.0:
            score.activity_pass = False # No data, assume pass
        else:
            score.activity_pass = False
            
        if t_set.use_srim_filter and not score.srim_pass: 
            is_safe = False
            reasons.append("SRIM 고평가")
        if t_set.use_roe_filter and not score.roe_pass: 
            is_safe = False
            reasons.append(f"ROE 미달({d.roe_val:.1f}%)")
        if t_set.use_dividend_filter and not score.dividend_pass: 
            is_safe = False
            reasons.append("배당 미지급")
        if t_set.use_cashflow_filter and not score.cashflow_pass: 
            is_safe = False
            reasons.append("영업현금흐름 적자")
        if t_set.use_activity_filter and not score.activity_pass:
            is_safe = False
            reasons.append(f"활동성 둔화({d.asset_turnover_growth*100:.1f}%)")
            
        score.safety_fail_reason = ", ".join(reasons)
        return is_safe

    def calculate_srim(self, eps, roe, bond_yield) -> float:
        """SRIM 적정주가 계산"""
        if eps <= 0 or roe <= 0: return 0.0
        
        roe_val = min(roe, 20.0) / 100.0 
        req_yield = bond_yield + 0.045 # Risk Premium
        
        growth = roe_val * 0.7 
        if req_yield <= growth: 
            growth = req_yield * 0.7
        
        fair_price = eps / (req_yield - growth)
        return fair_price * 0.9 # 안전마진

    def calculate_pivot(self, high, low, close):
        """피봇 포인트 계산"""
        pivot = (high + low + close) / 3
        r1 = (2 * pivot) - low
        r2 = pivot + (high - low)
        s1 = (2 * pivot) - high
        s2 = pivot - (high - low)
        return {"s1": s1, "s2": s2, "r1": r1, "r2": r2}


# =========================================================
# 3. 통합 실행 서비스 (UsEvaluationService)
# =========================================================

class UsEvaluationService:
    def __init__(self):
        self.evaluator = UsEvaluator()
        self.settings_manager = get_settings_manager()

    def get_treasury_yield(self):
        """미국 10년물 국채 수익률 조회"""
        try:
            tnx = yf.Ticker("^TNX")
            hist = tnx.history(period="5d")
            if not hist.empty:
                return hist['Close'].iloc[-1] / 100.0
        except: pass
        return 0.045 # 기본값

    def _calculate_technicals_from_yf(self, hist: pd.DataFrame) -> Dict[str, float]:
        """
        yfinance history DataFrame을 기반으로 기술적 지표 계산
        """
        if hist.empty:
            return {}

        df = hist.copy()
        # yfinance Close는 수정주가일 수 있음 (Auto adjusted)
        # 지표 계산
        close = df['Close']
        
        # 1. 이동평균선
        ma_vals = {}
        for ma in [5, 10, 20, 30, 60, 120]:
            ma_vals[f'ma{ma}'] = close.rolling(window=ma).mean().iloc[-1]
            
        # 2. RSI (14)
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi_14 = 100 - (100 / (1 + rs)).iloc[-1]
        
        # 3. OBV
        # yfinance Volume은 int64
        direction = np.sign(close.diff()).fillna(0)
        obv_series = (direction * df['Volume']).cumsum()
        obv_val = obv_series.iloc[-1]
        obv_ma = obv_series.rolling(window=14).mean().iloc[-1]
        
        return {
            **ma_vals, # ma5, ma10... spread
            'rsi_14': rsi_14,
            'obv_val': obv_val,
            'obv_ma': obv_ma,
            'avg_volume': df['Volume'].tail(20).mean()
        }

    def _calculate_asset_turnover_growth(self, ticker, years=3) -> float:
        """
        [US 전용] 자산회전율 변화율 (yfinance 사용)
        :param ticker: yfinance.Ticker 객체
        """
        try:
            # 1. 재무 데이터 가져오기 (이미 ticker는 생성되어 있음)
            bs = ticker.balance_sheet     # 대차대조표 (자산)
            inc = ticker.financials       # 손익계산서 (매출)
            
            if bs.empty or inc.empty:
                return -99.0

            # 2. 공통 날짜 추출 및 정렬 (과거 -> 현재)
            common_dates = sorted(list(set(bs.columns) & set(inc.columns)))
            
            # 최소 2년치 데이터 필요 (성장률 계산 위해)
            if len(common_dates) < 2:
                return -99.0
            
            # 최근 N년 데이터만 사용
            target_dates = common_dates[-(years+1):]
            
            turnovers = []
            
            for d in target_dates:
                try:
                    # 총자산 (Total Assets)
                    assets = bs.loc['Total Assets', d]
                    
                    # 매출액 (Total Revenue) - 키 값 변동 대응
                    if 'Total Revenue' in inc.index:
                        revenue = inc.loc['Total Revenue', d]
                    elif 'Operating Revenue' in inc.index:
                        revenue = inc.loc['Operating Revenue', d]
                    else:
                        revenue = 0
                    
                    if assets > 0:
                        turnovers.append(revenue / assets)
                        
                except KeyError:
                    continue

            if len(turnovers) < 2:
                return -99.0
            
            # 3. 성장률 계산 ( (최근 / 과거) - 1 )
            first = turnovers[0]
            last = turnovers[-1]
            
            if first <= 0: return -99.0
            
            return (last / first) - 1.0

        except Exception as e:
            return -99.0

    def run_evaluation(
        self, 
        base_date: date, 
        target_data_date: Optional[str] = None,
        auto_detect_data_date: bool = False, 
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        log_callback: Optional[Callable[[str], None]] = None
    ):
        def log(msg, level="INFO"):
            if log_callback: log_callback(msg)
            else: 
                if level == "ERROR": logger.error(msg)
                else: logger.info(msg)

        data_date_str = target_data_date
        with get_session() as session:
            # 평가 대상 종목 조회 (ItemMst)
            if not data_date_str:
                data_date_str = session.query(func.max(ItemMst.base_date))\
                    .filter(ItemMst.market_type == 'US').scalar()
            
            items = session.query(ItemMst).filter(
                ItemMst.base_date == data_date_str, 
                ItemMst.market_type == 'US'
            ).all()
            
            total_count = len(items)

            if not items:
                log("평가 대상 데이터가 없습니다.")
                return {'total_evaluated': 0}

            bond_yield = self.get_treasury_yield()
            min_score = self.settings_manager.settings.evaluation.us.min_total_score
            
            log(f"US 평가 시작 (대상: {len(items)}개, 국채금리: {bond_yield*100:.2f}%)")
            
            processed_count = 0
            buy_candidates = 0
            
            for idx, item in enumerate(items):
                processed_count += 1
                if progress_callback:
                    progress_callback(processed_count, total_count, f"{item.item_cd} 평가 중...")
                try:
                    # [핵심 변경] DB ItemPrice 대신 yfinance history 사용
                    yf_ticker = yf.Ticker(item.item_cd)
                    try:
                        info = yf_ticker.info
                    except Exception:
                        info = {}
                    # MA120, RSI 등을 위해 넉넉히 1년~2년치 데이터 요청
                    hist = yf_ticker.history(period="2y") 
                    
                    if hist.empty:
                        # 데이터 없으면 스킵
                        continue
                    
                    # 1. 기술적 지표 계산 (history 기반)
                    tech_data = self._calculate_technicals_from_yf(hist)
                    
                    # 현재값 및 전일값 (Pivot용) 추출
                    curr_row = hist.iloc[-1]
                    prev_row = hist.iloc[-2] if len(hist) > 1 else curr_row
                    
                    # 2. 재무/지분 정보 조회 (DB)
                    fs = session.query(FinancialSheet).filter_by(
                        item_cd=item.item_cd, base_date=data_date_str
                    ).first()
                    
                    eq = session.query(ItemEquity).filter_by(
                        item_cd=item.item_cd, market_type='US'
                    ).first()
                    
                    # 3. 활동성 지표 계산
                    at_growth = self._calculate_asset_turnover_growth(yf_ticker)

                    # 4. SwingData 매핑
                    d = SwingData(item_cd=item.item_cd, item_nm=item.itms_nm)
                    
                    # 시세 정보
                    d.stck_clpr = float(curr_row['Close'])
                    d.volume = int(curr_row['Volume'])
                    
                    # 기술적 지표 매핑
                    d.ma5 = tech_data.get('ma5', 0)
                    d.ma10 = tech_data.get('ma10', 0)
                    d.ma20 = tech_data.get('ma20', 0)
                    d.ma30 = tech_data.get('ma30', 0)
                    d.ma60 = tech_data.get('ma60', 0)
                    d.ma120 = tech_data.get('ma120', 0)
                    d.rsi_14 = tech_data.get('rsi_14', 50)
                    d.obv_val = tech_data.get('obv_val', 0)
                    d.obv_ma = tech_data.get('obv_ma', 0)
                    d.avg_volume = int(tech_data.get('avg_volume', 0))
                    
                    d.asset_turnover_growth = at_growth

                    # 재무 정보 매핑
                    if fs:
                        d.revenue_growth = fs.grs or 0
                        d.profit_margin = fs.bsop_prfi_inrt or 0
                        d.roe_val = fs.roe_val or 0
                        d.debt_ratio = fs.lblt_rate or 0
                        d.net_income = fs.thtr_ntin or 0
                        d.cf_oa = fs.cf_oa or 0
                    
                    # 지분/지표 정보 매핑
                    if eq:
                        d.per = eq.per or 0
                        d.pbr = eq.pbr or 0
                        d.market_cap = eq.hts_avls or 0
                        d.w52_high = eq.w52_hgpr or 0
                        d.w52_low = eq.w52_lwpr or 0
                        d.shares = eq.lstn_stcn or 1
                        d.eps = eq.eps or 0
                        d.dividend_yield = eq.dividend_yield or 0
                    
                    # 기관 투자자 보유 비율
                    d.institutional_ownership = info.get('heldPercentInstitutions', 0) * 100 or 0.0

                    # 5. 점수 평가
                    score = self.evaluator.evaluate(d)
                    
                    # 6. SRIM & Pivot
                    score.srim_price = self.evaluator.calculate_srim(d.eps, d.roe_val, bond_yield)
                    score.srim_pass = (score.srim_price > d.stck_clpr)
                    
                    pivots = self.evaluator.calculate_pivot(
                        prev_row['High'], prev_row['Low'], prev_row['Close']
                    )
                    score.pivot_s1 = pivots['s1']
                    score.pivot_s2 = pivots['s2']
                    score.pivot_r1 = pivots['r1']
                    score.pivot_r2 = pivots['r2']
                    
                    # 7. 안전망 체크
                    is_safe = self.evaluator.check_safety_nets(d, score)
                    
                    if score.total_score >= min_score and is_safe:
                        score.is_buy_candidate = True
                        buy_candidates += 1
                    else:
                        score.is_buy_candidate = False
                    
                    # 8. 결과 저장
                    self._save_result(session, base_date.strftime('%Y%m%d'), score, d.stck_clpr)
                    
                    # Rate Limit 방지 (필요 시)
                    if idx % 20 == 0: time.sleep(0.5)

                    if processed_count % 50 == 0:
                        session.commit()
                    
                except Exception as e:
                    log(f"Error evaluating {item.item_cd}: {e}", "DEBUG")
                    pass
            
            # 최종 커밋
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                log(f"최종 커밋 실패: {e}", "ERROR")
                
            log(f"US 평가 완료: {processed_count}건 처리됨 (매수후보: {buy_candidates}건)")
            return {'total_evaluated': processed_count, 'buy_candidates': buy_candidates}

    def _save_result(self, session, base_date, score, price):
        existing = session.query(EvaluationResult).filter_by(
            item_cd=score.item_cd, base_date=base_date, market_type='US'
        ).first()
        
        if not existing:
            existing = EvaluationResult(
                item_cd=score.item_cd, base_date=base_date, market_type='US'
            )
            session.add(existing)
            
        existing.item_nm = score.item_nm
        existing.total_score = score.total_score
        existing.sheet_score = score.sheet_score
        existing.trend_score = score.trend_score
        existing.price_score = score.price_score
        existing.kpi_score = score.kpi_score
        existing.buy_score = score.buy_score
        existing.avls_score = score.avls_score
        existing.per_score = score.per_score
        existing.pbr_score = score.pbr_score
        existing.is_buy_candidate = score.is_buy_candidate
        existing.current_price = price
        existing.srim_price = score.srim_price
        existing.srim_pass = score.srim_pass
        existing.cashflow_pass = score.cashflow_pass
        existing.activity_pass = score.activity_pass
        existing.dividend_pass = score.dividend_pass
        existing.roe_pass = score.roe_pass
        existing.updated_at = datetime.now()