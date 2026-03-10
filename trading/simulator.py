"""
시뮬레이션 엔진 (KR/US 분리 적용)
- 가상 계좌 관리 (시장별 분리)
- 가상 매수/매도 처리
- 수수료, 세금 계산 (시장별 설정 적용)
"""

import logging
from datetime import datetime, date
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
import uuid
import pytz

from config.settings import get_settings_manager
from config.database import (
    get_session, VirtualAccount, VirtualHolding, 
    TradeHistory, ItemMst
)
# [수정] 시장별 Fetcher 임포트
from impl.kr.kr_fetcher import KrFetcher
from impl.us.us_fetcher import UsFetcher

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    """주문 결과"""
    success: bool
    order_no: str = ""
    item_cd: str = ""
    item_nm: str = ""
    order_type: str = ""  # 'B' or 'S'
    qty: int = 0
    price: float = 0.0    # [수정] US는 소수점이 있으므로 float 권장
    amount: float = 0.0
    fee: float = 0.0
    tax: float = 0.0
    message: str = ""
    
    @property
    def total_amount(self) -> float:
        """총 거래금액 (수수료, 세금 포함)"""
        if self.order_type == 'B':
            return self.amount + self.fee
        else:
            return self.amount - self.fee - self.tax


@dataclass
class HoldingInfo:
    """보유 종목 정보"""
    item_cd: str
    item_nm: str
    qty: int
    avg_price: float
    current_price: float
    eval_amt: float
    profit: float
    profit_rate: float
    buy_date: str


@dataclass
class AccountInfo:
    """계좌 정보"""
    balance: float
    total_eval: float
    total_profit: float
    total_profit_rate: float
    holdings: List[HoldingInfo]


class SimulationEngine:
    """시뮬레이션 엔진"""
    
    def __init__(self, market_type: str = "KR"):
        self.market_type = market_type
        self.settings_manager = get_settings_manager()
        
        # [수정] 시장별 Fetcher 및 설정 로드
        if self.market_type == "US":
            self.fetcher = UsFetcher()
            self.trading_settings = self.settings_manager.settings.trading.us
        else:
            self.fetcher = KrFetcher()
            self.trading_settings = self.settings_manager.settings.trading.kr
        
        # 계좌 초기화
        self._initialize_account()
    
    def _initialize_account(self):
        """가상 계좌 초기화 (시장별)"""
        with get_session() as session:
            # [수정] market_type 필터 추가
            account = session.query(VirtualAccount).filter_by(market_type=self.market_type).first()
            
            if not account:
                initial_balance = self.trading_settings.initial_balance
                account = VirtualAccount(
                    market_type=self.market_type, # 시장 구분 저장
                    balance=initial_balance,
                    total_eval=initial_balance,
                    total_profit=0,
                    total_profit_rate=0.0
                )
                session.add(account)
                session.commit() # [수정] 즉시 커밋하여 생성 보장
                logger.info(f"[{self.market_type}] 가상 계좌 초기화: {initial_balance:,.2f}")
    
    def get_account_info(self) -> AccountInfo:
        """계좌 정보 조회"""
        with get_session() as session:
            # [수정] market_type 필터 추가
            account = session.query(VirtualAccount).filter_by(market_type=self.market_type).first()
            holdings = session.query(VirtualHolding).filter_by(market_type=self.market_type).all()
            
            holding_list = []
            balance = float(account.balance) if account else 0.0
            total_eval = balance
            total_cost = 0.0
            
            for h in holdings:
                # 현재가 업데이트
                price_info = self.fetcher.get_current_price(h.item_cd)
                
                # [수정] 가격 정보 처리 (US 소수점 대응)
                current_p = h.avg_price # 기본값
                if price_info:
                    current_p = float(price_info.get('price', h.avg_price))
                    
                h.current_price = current_p
                h.eval_amt = h.quantity * current_p
                
                cost = h.quantity * float(h.avg_price)
                h.profit = h.eval_amt - cost
                h.profit_rate = (h.profit / cost * 100) if cost else 0.0
                total_cost += cost
                
                holding_list.append(HoldingInfo(
                    item_cd=h.item_cd,
                    item_nm=h.item_nm or h.item_cd,
                    qty=h.quantity,
                    avg_price=float(h.avg_price),
                    current_price=float(h.current_price or 0),
                    eval_amt=float(h.eval_amt or 0),
                    profit=float(h.profit or 0),
                    profit_rate=float(h.profit_rate or 0.0),
                    buy_date=h.buy_date or ''
                ))
                
                total_eval += float(h.eval_amt or 0)
            
            # 총손익 계산
            initial_balance = self.trading_settings.initial_balance
            total_profit = total_eval - initial_balance
            total_profit_rate = (total_profit / initial_balance * 100) if initial_balance else 0.0
            
            return AccountInfo(
                balance=balance,
                total_eval=total_eval,
                total_profit=total_profit,
                total_profit_rate=total_profit_rate,
                holdings=holding_list
            )
    
    def get_balance(self) -> float:
        """예수금 잔고 조회"""
        with get_session() as session:
            account = session.query(VirtualAccount).filter_by(market_type=self.market_type).first()
            return float(account.balance) if account else 0.0
    
    def reset_account(self):
        """계좌 초기화 (리셋)"""
        initial_balance = self.trading_settings.initial_balance
        
        with get_session() as session:
            # [수정] 해당 시장의 보유 종목만 삭제
            session.query(VirtualHolding).filter_by(market_type=self.market_type).delete()
            
            # [수정] 해당 시장의 계좌만 초기화
            account = session.query(VirtualAccount).filter_by(market_type=self.market_type).first()
            if account:
                account.balance = initial_balance
                account.total_eval = initial_balance
                account.total_profit = 0
                account.total_profit_rate = 0.0
            else:
                account = VirtualAccount(
                    market_type=self.market_type,
                    balance=initial_balance,
                    total_eval=initial_balance,
                    total_profit=0,
                    total_profit_rate=0.0
                )
                session.add(account)
            session.commit()
        
        logger.info(f"[{self.market_type}] 계좌 초기화 완료: {initial_balance:,.2f}")
    
    def calculate_fee(self, amount: float) -> float:
        """수수료 계산 (시장별 설정 사용)"""
        if self.trading_settings.apply_fee:
            return amount * self.trading_settings.fee_rate
        return 0.0
    
    def calculate_tax(self, amount: float) -> float:
        """세금 계산 (매도 시에만, 시장별 설정 사용)"""
        if self.trading_settings.apply_fee: # 보통 설정에 apply_fee/tax 등이 있음
            return amount * self.trading_settings.tax_rate
        return 0.0
    
    def buy(self, item_cd: str, qty: int, price: float = 0, trade_source: str = "manual") -> OrderResult:
        """매수 주문"""
        result = OrderResult(
            success=False,
            item_cd=item_cd,
            order_type='B'
        )
        
        try:
            # 1. 현재가 조회
            if price == 0:
                price_info = self.fetcher.get_current_price(item_cd)
                if not price_info:
                    result.message = "현재가 조회 실패"
                    return result
                price = float(price_info['price'])
            
            result.price = price
            result.quantity = qty
            result.amount = price * qty
            result.fee = self.calculate_fee(result.amount)
            
            total_cost = result.amount + result.fee
            
            # 2. 잔고 확인
            with get_session() as session:
                account = session.query(VirtualAccount).filter_by(market_type=self.market_type).first()
                
                if not account or account.balance < total_cost:
                    result.message = f"잔고 부족 (필요: {total_cost:,.2f}, 보유: {account.balance if account else 0:,.2f})"
                    return result
                
                # 3. 종목명 조회
                item = session.query(ItemMst).filter(ItemMst.item_cd == item_cd).first()
                item_nm = item.itms_nm if item else item_cd
                result.item_nm = item_nm
                
                # 4. 잔고 차감
                account.balance -= total_cost
                
                # 5. 보유 종목 업데이트
                holding = session.query(VirtualHolding).filter(
                    VirtualHolding.item_cd == item_cd,
                    VirtualHolding.market_type == self.market_type # [수정] 시장 필터
                ).first()
                
                today_str = date.today().strftime('%Y%m%d')
                
                if holding:
                    # 기존 보유 종목 - 평균 매입가 계산
                    total_qty = holding.quantity + qty
                    total_cost_old = holding.quantity * float(holding.avg_price)
                    total_cost_new = result.amount
                    
                    # [수정] 평균단가 소수점 처리 (US 대응)
                    new_avg = (total_cost_old + total_cost_new) / total_qty
                    
                    holding.avg_price = new_avg
                    holding.quantity = total_qty
                    holding.current_price = price
                    holding.eval_amt = total_qty * price
                else:
                    # 신규 보유
                    holding = VirtualHolding(
                        market_type=self.market_type, # [수정] 시장 정보 저장
                        item_cd=item_cd,
                        item_nm=item_nm,
                        quantity=qty,
                        avg_price=price,
                        current_price=price,
                        eval_amt=result.amount,
                        profit=0,
                        profit_rate=0.0,
                        buy_date=today_str
                    )
                    session.add(holding)
                
                # 6. 거래 이력 저장
                order_no = f"SIM{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}"
                
                # [1] 시장별 현지 시간 계산 로직
                now_utc = datetime.now(pytz.utc) # 기준은 항상 UTC로 시작

                if self.market_type == 'US':
                    # 미국 동부 시간 (뉴욕) - 썸머타임 자동 반영
                    local_dt = now_utc.astimezone(pytz.timezone('America/New_York'))
                else:
                    # 한국 시간 (기본값)
                    local_dt = now_utc.astimezone(pytz.timezone('Asia/Seoul'))

                # 날짜와 시간을 현지 시간 기준으로 포맷팅
                local_date_str = local_dt.strftime('%Y%m%d')
                local_time_str = local_dt.strftime('%H%M%S')

                # [2] DB 저장
                history = TradeHistory(
                    market_type=self.market_type,
                    item_cd=item_cd,
                    trade_date=local_date_str,  # [수정] 현지 날짜 (미국은 한국보다 하루 늦을 수 있음)
                    trade_time=local_time_str,  # [수정] 현지 시간
                    trade_type='buy',
                    quantity=qty,
                    price=price,
                    amount=result.amount,
                    fee=result.fee,
                    tax=0,
                    trade_source=trade_source,
                    rmk=f"시뮬레이션 매수: {item_nm}"
                )
                session.add(history)
                
                result.success = True
                result.order_no = order_no
                result.message = f"매수 완료: {item_nm} {qty}주 @ {price:,.2f}"
                
                session.commit() # [수정] 전체 트랜잭션 커밋
                
                logger.info(f"[{self.market_type}] {result.message}")
                
        except Exception as e:
            result.message = f"매수 처리 오류: {str(e)}"
            logger.error(result.message)
        
        return result
    
    def sell(self, item_cd: str, qty: int, price: float = 0, trade_source: str = "manual") -> OrderResult:
        """매도 주문"""
        result = OrderResult(success=False, item_cd=item_cd, order_type='S')
        try:
            with get_session() as session:
                # 당일 중복 매도 체크
                if trade_source == "auto":
                    today = date.today().strftime('%Y%m%d')
                    duplicate_sell = session.query(TradeHistory).filter(
                        TradeHistory.trade_date == today,
                        TradeHistory.item_cd == item_cd,
                        TradeHistory.market_type == self.market_type, # [수정] 시장 필터
                        TradeHistory.trade_type == 'sell',
                        TradeHistory.trade_source == 'auto'
                    ).first()
                    
                    if duplicate_sell:
                        result.message = "금일 이미 매도한 종목입니다. (중복 매도 방지)"
                        return result

                # 보유 종목 확인
                holding = session.query(VirtualHolding).filter(
                    VirtualHolding.item_cd == item_cd,
                    VirtualHolding.market_type == self.market_type # [수정] 시장 필터
                ).first()
                
                if not holding:
                    result.message = "보유하지 않은 종목입니다."
                    return result
                
                result.item_nm = holding.item_nm or item_cd
                
                current_qty = holding.quantity
                if qty == 0: qty = current_qty
                
                if qty > current_qty:
                    result.message = f"보유 수량 부족 (보유: {current_qty}, 요청: {qty})"
                    return result
                
                # 가격 결정
                if price == 0:
                    price_info = self.fetcher.get_current_price(item_cd)
                    if not price_info:
                        result.message = "현재가 조회 실패"
                        return result
                    price = float(price_info['price'])
                    if price == 0:
                        result.message = "현재가 조회 실패 (0원)"
                        return result
                
                result.price = price
                result.qty = qty
                result.amount = price * qty
                result.fee = self.calculate_fee(result.amount)
                result.tax = self.calculate_tax(result.amount)
                
                # 손익 계산
                buy_amount = float(holding.avg_price) * qty
                sell_amount_net = result.amount - result.fee - result.tax
                profit = sell_amount_net - buy_amount
                profit_rate = (profit / buy_amount * 100) if buy_amount else 0.0
                
                # 계좌 업데이트
                account = session.query(VirtualAccount).filter_by(market_type=self.market_type).first()
                account.balance += sell_amount_net
                
                # 보유 수량 업데이트
                if qty >= current_qty:
                    session.delete(holding)
                else:
                    holding.quantity -= qty
                    holding.eval_amt = holding.quantity * price
                
                # 거래 이력 저장
                today_str = date.today().strftime('%Y%m%d')
                order_no = f"SIM{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}"

                # [1] 시장별 현지 시간 계산 로직
                now_utc = datetime.now(pytz.utc) # 기준은 항상 UTC로 시작

                if self.market_type == 'US':
                    # 미국 동부 시간 (뉴욕) - 썸머타임 자동 반영
                    local_dt = now_utc.astimezone(pytz.timezone('America/New_York'))
                else:
                    # 한국 시간 (기본값)
                    local_dt = now_utc.astimezone(pytz.timezone('Asia/Seoul'))

                # 날짜와 시간을 현지 시간 기준으로 포맷팅
                local_date_str = local_dt.strftime('%Y%m%d')
                local_time_str = local_dt.strftime('%H%M%S')
                
                history = TradeHistory(
                    market_type=self.market_type, # [수정] 시장 필터
                    item_cd=item_cd,
                    trade_date=local_date_str,
                    trade_time=local_time_str,
                    trade_type='sell',
                    quantity=qty,
                    price=price,
                    amount=result.amount,
                    fee=result.fee,
                    tax=result.tax,
                    profit=profit,
                    profit_rate=round(profit_rate, 2),
                    rmk=f"시뮬레이션 매도: {holding.item_nm}",
                    trade_source=trade_source
                )
                session.add(history)
                session.commit()
                
                result.success = True
                result.order_no = order_no
                result.message = f"매도 완료: {holding.item_nm} {qty}주 @ {price:,.2f} (손익: {profit:+,.2f})"
                
                logger.info(f"[{self.market_type}] {result.message}")

        except Exception as e:
            result.message = str(e)
            logger.error(f"Sim Sell Error: {e}")
            
        return result
    
    def get_holding(self, item_cd: str) -> Optional[HoldingInfo]:
        """특정 종목 보유 정보 조회"""
        with get_session() as session:
            holding = session.query(VirtualHolding).filter(
                VirtualHolding.item_cd == item_cd,
                VirtualHolding.market_type == self.market_type # [수정] 시장 필터
            ).first()
            
            if not holding:
                return None
            
            # 현재가 업데이트
            price_info = self.fetcher.get_current_price(item_cd)
            
            current_p = float(holding.avg_price)
            if price_info:
                current_p = float(price_info.get('price', holding.avg_price))
                
            holding.current_price = current_p
            holding.eval_amt = holding.quantity * current_p
            cost = holding.quantity * float(holding.avg_price)
            holding.profit = holding.eval_amt - cost
            holding.profit_rate = (holding.profit / cost * 100) if cost else 0.0
            
            return HoldingInfo(
                item_cd=holding.item_cd,
                item_nm=holding.item_nm or holding.item_cd,
                qty=holding.quantity,
                avg_price=float(holding.avg_price),
                current_price=float(holding.current_price or 0),
                eval_amt=float(holding.eval_amt or 0),
                profit=float(holding.profit or 0),
                profit_rate=float(holding.profit_rate or 0.0),
                buy_date=holding.buy_date or ''
            )
    
    def get_trade_history(self, limit: int = 100) -> List[Dict]:
        """거래 이력 조회"""
        with get_session() as session:
            histories = session.query(TradeHistory).filter(
                TradeHistory.market_type == self.market_type # [수정] 시장 필터
            ).order_by(
                TradeHistory.trade_date.desc(),
                TradeHistory.trade_time.desc()
            ).limit(limit).all()
            
            return [
                {
                    'id': h.id,
                    'item_cd': h.item_cd,
                    'trade_date': h.trade_date,
                    'trade_time': h.trade_time,
                    'trade_type': '매수' if h.trade_type == 'buy' else '매도',
                    'qty': h.quantity,
                    'price': float(h.price),
                    'amount': float(h.amount or 0),
                    'fee': float(h.fee or 0),
                    'tax': float(h.tax or 0),
                    'profit': float(h.profit or 0),
                    'profit_rate': float(h.profit_rate or 0),
                    'rmk': h.rmk or ''
                }
                for h in histories
            ]