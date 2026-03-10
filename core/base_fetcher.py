from abc import ABC, abstractmethod
from typing import Dict, Optional

class BaseFetcher(ABC):
    """국가별 시세/잔고 조회를 위한 공통 추상 클래스"""

    @abstractmethod
    def get_current_price(self, code: str) -> Optional[Dict]:
        """현재가 조회 (price, open, high, low, volume 포함)"""
        pass

    @abstractmethod
    def get_account_balance(self, account_no: str, account_cd: str) -> Optional[Dict]:
        """계좌 잔고 조회"""
        pass
        
    @abstractmethod
    def get_stock_info(self, code: str) -> Optional[Dict]:
        """종목 상세 정보 조회"""
        pass