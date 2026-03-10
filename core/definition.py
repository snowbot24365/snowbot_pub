from enum import Enum, auto

class MarketType(Enum):
    """지원하는 주식 시장 타입 정의"""
    KR = "KR"   # 국내 주식
    US = "US"   # 미국 주식

class LicenseLevel(Enum):
    """판매용 라이선스 등급 정의"""
    BASIC = "BASIC"       # 국내 전용
    PREMIUM = "PREMIUM"   # 국내 + 미국 (혼합형)