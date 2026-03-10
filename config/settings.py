"""
설정 관리 모듈
- 시장별(KR/US) 설정 분리 (API Key, Mode, Trading, Evaluation)
- JSON 파일 기반 영구 저장
"""

import json
import os
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from enum import Enum
from datetime import datetime

ROOT_DIR = Path(__file__).resolve().parent.parent

# 설정 파일 경로
CONFIG_DIR = ROOT_DIR / "config_data"
CONFIG_DIR.mkdir(exist_ok=True)
SETTINGS_FILE = CONFIG_DIR / "settings.json"

class Environment(Enum):
    """실행 환경 구분"""
    LOCAL = "local"
    PRODUCTION = "production"


class ExecutionMode(Enum):
    """실행 모드 구분"""
    SIMULATION = "simulation"
    REAL_TRADING = "real_trading"


@dataclass
class APISettings:
    """API 관련 설정 (국내/해외 Key 및 모드 완전 분리)"""
    opendart_api_key: str = ""
    
    # -------------------------------------------------------
    # 1. 국내 주식 (KR) 설정
    # -------------------------------------------------------
    # [KR] 모드 설정
    kis_api_mode_kr: str = "mock"             # 시세 조회용 (mock/real)
    kis_trading_account_mode_kr: str = "mock" # 주문용 (mock/real)
    
    # [KR] 모의투자 Key
    kis_mock_app_key_kr: str = ""
    kis_mock_app_secret_kr: str = ""
    kis_mock_account_no_kr: str = ""
    kis_mock_account_cd_kr: str = "01"
    
    # [KR] 실전투자 Key
    kis_real_app_key_kr: str = ""
    kis_real_app_secret_kr: str = ""
    kis_real_account_no_kr: str = ""
    kis_real_account_cd_kr: str = "01"

    kis_real_confirmed_kr: bool = False
    
    # -------------------------------------------------------
    # 2. 해외 주식 (US) 설정
    # -------------------------------------------------------
    # [US] 모드 설정
    kis_api_mode_us: str = "mock"             # 시세 조회용 (mock/real)
    kis_trading_account_mode_us: str = "mock" # 주문용 (mock/real)
    
    # [US] 모의투자 Key
    kis_mock_app_key_us: str = ""
    kis_mock_app_secret_us: str = ""
    kis_mock_account_no_us: str = ""
    kis_mock_account_cd_us: str = "01"
    
    # [US] 실전투자 Key
    kis_real_app_key_us: str = ""
    kis_real_app_secret_us: str = ""
    kis_real_account_no_us: str = ""
    kis_real_account_cd_us: str = "01"

    kis_real_confirmed_us: bool = False
    # -------------------------------------------------------
    # 3. 공통 설정
    # -------------------------------------------------------
    hts_user_id: str = "" # HTS ID (@...)


@dataclass
class DatabaseSettings:
    """데이터베이스 연결 설정"""
    db_type: str = "sqlite" # sqlite / oracle
    
    # Local
    sqlite_path: str = "stock_data.db"
    
    # Production
    oracle_user: str = ""
    oracle_password: str = ""
    oracle_dsn: str = ""
    oracle_wallet_path: str = ""


@dataclass
class CollectionSettings:
    """데이터 수집 설정"""
    # ==========================================
    # [국내 시장 (KR)]
    # ==========================================
    collect_kospi: bool = True
    collect_kosdaq: bool = True
    
    # KR 수집 모드 및 제한
    kr_collection_mode: str = "random_n"  # 'all' 또는 'random_n'
    kr_random_n_stocks: int = 10          # random_n 모드일 때 수집할 개수
    
    # KR 시장별 상위 N개 제한 (0 = 전체)
    kospi_top_n: int = 0
    kosdaq_top_n: int = 0
    
    # ==========================================
    # [미국 시장 (US)]
    # ==========================================
    collect_nasdaq: bool = False
    collect_nyse: bool = False
    collect_amex: bool = False
    
    # US 수집 모드 및 제한
    us_collection_mode: str = "random_n"  # 'all' 또는 'random_n'
    us_random_n_stocks: int = 10          # random_n 모드일 때 수집할 개수
    
    # US 시장별 상위 N개 제한 (0 = 전체)
    nasdaq_top_n: int = 0
    nyse_top_n: int = 0
    amex_top_n: int = 0
    
    max_retries: int = 3
    retry_delay_seconds: int = 5


# ==========================================
# [매매 설정 분리] MarketTradingSettings
# ==========================================
@dataclass
class MarketTradingSettings:
    """시장별 개별 매매 설정"""
    # [매수 설정]
    buy_enabled: bool = False
    buy_rate: float = 0.0      # 비중(%)
    max_buy_amount: int = 0 # 최대금액 (KR:원, US:달러)
    limit_count: int = 1       # 최대 보유 종목 수
    
    # 매수 기준가 설정 (피벗 지지선 활용 등)
    # 옵션: 'current'(현재가), 'pvt'(피벗), 'pvt_sup1'(1차지지), 'pvt_sup2'(2차지지)
    buy_price_criteria: str = "current"
    
    # 필터
    use_srim_filter: bool = True
    use_dividend_filter: bool = False
    use_cashflow_filter: bool = False
    use_activity_filter: bool = False
    use_roe_filter: bool = False
    
    # 매도
    sell_up_rate: float = 0.0    # 익절률
    sell_down_rate: float = 0.0 # 손절률
    use_loss_cut: bool = True     # 손절 기능 사용 여부
    sell_hold_rate: float = 0.0  # 매도 보류 비율 (물타기용)

    # 분할 매도 비율 (1회 매도 시 처분할 물량 %)
    # 100.0이면 전량 매도, 50.0이면 절반 매도
    sell_split_rate: float = 0.0
    
    # 트레일링 스탑
    trailing_stop_enabled: bool = False
    trailing_stop_rate: float = 5.0 # 고점 대비 하락률
    
    # 수수료 및 세금
    apply_fee: bool = True
    fee_rate: float = 0.00015 
    tax_rate: float = 0.0023
    
    # 시뮬레이션 초기 자본금
    initial_balance: int = 10000000

@dataclass
class TradingSettings:
    """전체 매매 설정"""
    # 한국 주식
    kr: MarketTradingSettings = field(default_factory=lambda: MarketTradingSettings(
        max_buy_amount=0,
        initial_balance=0,
        fee_rate=0.00015,
        tax_rate=0.0023,
        sell_up_rate=0.0,
        sell_down_rate=0.0
    ))
    
    # 미국 주식
    us: MarketTradingSettings = field(default_factory=lambda: MarketTradingSettings(
        max_buy_amount=0,   # $1,000
        initial_balance=0, # $10,000
        fee_rate=0.0025,       # 0.25%
        tax_rate=0.0000229,    # SEC Fee
        sell_up_rate=0.0,
        sell_down_rate=-0.0
    ))


# ==========================================
# [평가 설정 분리] MarketEvaluationSettings
# ==========================================
@dataclass
class MarketEvaluationSettings:
    """시장별 개별 평가 설정"""
    min_total_score: int = 0
    
    # 가중치
    weight_sheet: float = 1.0
    weight_trend: float = 1.0
    weight_price: float = 1.0
    weight_kpi: float = 1.0
    weight_buy: float = 1.0
    weight_avls: float = 1.0
    weight_per: float = 1.0
    weight_pbr: float = 1.0
    
    # 기준값
    threshold_grs: float = 0.0
    threshold_bsop_prfi_inrt: float = 0.0
    threshold_rsrv_rate: float = 0.0
    threshold_lblt_rate: float = 0.0
    
    trend_alignment: str = "REGULAR"
    
    # 밸류에이션 기준
    per_benchmark: float = 0.0
    per_step: float = 0.0
    pbr_benchmark: float = 0.0
    pbr_step: float = 0.0
    
    high_rate_benchmark: float = 0.0
    high_rate_step: float = 0.0
    low_rate_benchmark: float = 0.0
    low_rate_step: float = 0.0
    
    # 시가총액 기준
    avls_benchmark: float = 0.0 
    avls_step: float = 0.0

@dataclass
class EvaluationSettings:
    """전체 평가 설정"""
    # 한국
    kr: MarketEvaluationSettings = field(default_factory=lambda: MarketEvaluationSettings(
        avls_benchmark=0.0,
        avls_step=0.0
    ))
    
    # 미국
    us: MarketEvaluationSettings = field(default_factory=lambda: MarketEvaluationSettings(
        avls_benchmark=0.0, 
        avls_step=0.0,
        per_benchmark=0.0,   
        pbr_benchmark=0.0
    ))


@dataclass
class ScheduleItem:
    """스케줄 항목"""
    id: str = ""
    name: str = ""
    task_type: str = ""
    market_type: str = "KR" # KR / US
    cron_expression: str = ""
    enabled: bool = True
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScheduleItem':
        return cls(**data)


@dataclass
class ScheduleSettings:
    """스케줄 설정"""
    schedules: List[ScheduleItem] = field(default_factory=list)
    
    def add_schedule(self, schedule: ScheduleItem):
        self.schedules.append(schedule)
    
    def remove_schedule(self, schedule_id: str):
        self.schedules = [s for s in self.schedules if s.id != schedule_id]
        
    def get_schedule(self, schedule_id: str) -> Optional[ScheduleItem]:
        for s in self.schedules:
            if s.id == schedule_id:
                return s
        return None


@dataclass
class AppSettings:
    """애플리케이션 전체 설정"""
    environment: str = Environment.LOCAL.value
    
    execution_mode_kr: str = ExecutionMode.SIMULATION.value
    execution_mode_us: str = ExecutionMode.SIMULATION.value
    
    api: APISettings = field(default_factory=APISettings)
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    collection: CollectionSettings = field(default_factory=CollectionSettings)
    trading: TradingSettings = field(default_factory=TradingSettings)
    evaluation: EvaluationSettings = field(default_factory=EvaluationSettings)
    schedule: ScheduleSettings = field(default_factory=ScheduleSettings)
    
    last_updated: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AppSettings':
        settings = cls()
        settings.environment = data.get("environment", Environment.LOCAL.value)
        global_mode = data.get("execution_mode", ExecutionMode.SIMULATION.value)
        settings.execution_mode_kr = data.get("execution_mode_kr", global_mode)
        settings.execution_mode_us = data.get("execution_mode_us", global_mode)
        
        if "api" in data: settings.api = APISettings(**data["api"])
        if "database" in data: settings.database = DatabaseSettings(**data["database"])
        if "collection" in data: settings.collection = CollectionSettings(**data["collection"])
        
        if "trading" in data:
            t_data = data["trading"]
            t_settings = TradingSettings()
            if "kr" in t_data: t_settings.kr = MarketTradingSettings(**t_data["kr"])
            if "us" in t_data: t_settings.us = MarketTradingSettings(**t_data["us"])
            settings.trading = t_settings
            
        if "evaluation" in data:
            e_data = data["evaluation"]
            e_settings = EvaluationSettings()
            if "kr" in e_data: e_settings.kr = MarketEvaluationSettings(**e_data["kr"])
            if "us" in e_data: e_settings.us = MarketEvaluationSettings(**e_data["us"])
            settings.evaluation = e_settings
            
        if "schedule" in data:
            schedules = [ScheduleItem.from_dict(s) for s in data["schedule"].get("schedules", [])]
            settings.schedule = ScheduleSettings(schedules=schedules)
            
        settings.last_updated = data.get("last_updated", "")
        return settings


class SettingsManager:
    """설정 관리자"""
    _instance: Optional['SettingsManager'] = None
    _settings: Optional[AppSettings] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._settings is None:
            self.load()
    
    @property
    def settings(self) -> AppSettings:
        if self._settings is None:
            self.load()
        return self._settings
    
    def load(self) -> AppSettings:
        if SETTINGS_FILE.exists():
            try:
                with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self._settings = AppSettings.from_dict(data)
            except Exception as e:
                print(f"설정 로드 오류: {e}")
                self._settings = AppSettings()
        else:
            self._settings = AppSettings()
            self.save()
        return self._settings
    
    def save(self):
        self._settings.last_updated = datetime.now().isoformat()
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(self._settings.to_dict(), f, ensure_ascii=False, indent=2)

    def update_api(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self.settings.api, k):
                setattr(self.settings.api, k, v)
        self.save()

    def update_database(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self.settings.database, k):
                setattr(self.settings.database, k, v)
        self.save()

    def update_collection(self, **kwargs):
        """수집 설정 업데이트"""
        for k, v in kwargs.items():
            if hasattr(self._settings.collection, k):
                setattr(self._settings.collection, k, v)
        self.save()

    def update_trading(self, market: str = "KR", **kwargs):
        target = self.settings.trading.kr if market == "KR" else self.settings.trading.us
        for k, v in kwargs.items():
            if hasattr(target, k):
                setattr(target, k, v)
        self.save()

    def update_evaluation(self, market: str = "KR", **kwargs):
        target = self.settings.evaluation.kr if market == "KR" else self.settings.evaluation.us
        for k, v in kwargs.items():
            if hasattr(target, k):
                setattr(target, k, v)
        self.save()
        
    def get_db_connection_string(self) -> str:
        db_type = self._settings.database.db_type
        if db_type == "sqlite":
            sqlite_path_str = self._settings.database.sqlite_path
            
            # 1. 입력된 경로가 절대 경로인지 확인
            if os.path.isabs(sqlite_path_str):
                db_path = Path(sqlite_path_str)
            else:
                # 2. 상대 경로이면 CONFIG_DIR 기준 (기존 로직 유지하되 명시적 결합)
                db_path = CONFIG_DIR / sqlite_path_str
            
            # [디버깅용 로그] 실제 연결하려는 DB 경로 출력
            # print(f"DEBUG: Connecting to SQLite DB at: {db_path}")
            
            # 3. 파일 존재 여부 확인 (선택 사항: 경고 메시지 출력)
            if not db_path.exists():
                print(f"WARNING: DB file not found at {db_path}. A new DB will be created.")
            else:
                print(f"INFO: Found existing DB file at {db_path}.")

            return f"sqlite:///{db_path}"
        else:
            dialect = "oracle+oracledb"
            return (
                f"{dialect}://{self._settings.database.oracle_user}:"
                f"{self._settings.database.oracle_password}@"
                f"{self._settings.database.oracle_dsn}"
            )
    
    def update_execution_mode(self, market: str, mode: str):
        if market == "KR":
            self.settings.execution_mode_kr = mode
        else:
            self.settings.execution_mode_us = mode
        self.save()

def get_settings_manager() -> SettingsManager:
    return SettingsManager()