from enum import Enum, auto

class MarketType(Enum):
    """지원하는 주식 시장 타입 정의"""
    KR = "KR"   # 국내 주식
    US = "US"   # 미국 주식