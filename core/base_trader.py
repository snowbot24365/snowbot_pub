from abc import ABC, abstractmethod
from typing import Dict, Optional
from .definition import MarketType

class BaseTrader(ABC):
    """
    모든 국가별 트레이더가 상속받아야 할 기본 클래스
    """
    
    def __init__(self, market: MarketType):
        self.market = market

    @abstractmethod
    def get_current_price(self, code: str) -> float:
        """현재가 조회 (단일 종목)"""
        pass

    @abstractmethod
    def get_balance(self) -> Dict:
        """잔고 및 예수금 조회"""
        pass

    @abstractmethod
    def buy_order(self, code: str, qty: int, price: float) -> bool:
        """매수 주문"""
        pass

    @abstractmethod
    def sell_order(self, code: str, qty: int, price: float) -> bool:
        """매도 주문"""
        pass
    
    # 공통 로직 (국가 상관없이 동일한 계산식 등)
    def calculate_yield(self, buy_avg_price: float, current_price: float) -> float:
        """수익률 계산 (공통 메서드)"""
        if buy_avg_price == 0:
            return 0.0
        return (current_price - buy_avg_price) / buy_avg_price * 100