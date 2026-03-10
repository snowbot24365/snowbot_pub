"""
데이터베이스 연결 및 모델 정의
- SQLite (Local) / Oracle ATP (Production) 자동 전환
- Oracle Thin Mode 적용
- ORA-01400 해결: Sequence 객체 추가
- [수정] 한국/미국 주식 통합 관리를 위한 구조 변경 (market_type 추가, 가격 Float 변경)
"""

from sqlalchemy import (
    create_engine, Column, String, Integer, Float, DateTime, event,
    Boolean, BigInteger, Text, ForeignKey, Index, Date, Sequence, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from contextlib import contextmanager
from datetime import datetime, date
from typing import Optional, Generator
import logging
import os

from config.settings import get_settings_manager

logger = logging.getLogger(__name__)

Base = declarative_base()


# ============ 모델 정의 ============

class ItemMst(Base):
    """종목 마스터 테이블 (한/미 통합)"""
    __tablename__ = 'item_mst'
    
    # [수정] 미국 티커 대응을 위해 길이 확장 (6 -> 20)
    item_cd = Column(String(20), primary_key=True, comment='종목코드')
    base_date = Column(String(8), primary_key=True, comment='기준일자 (YYYYMMDD)')
    
    # [수정] 시장 구분 추가 (KR/US)
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    mrkt_ctg = Column(String(10), comment='시장구분 (KOSPI/KOSDAQ/NAS/NYS/AMS)')
    itms_nm = Column(String(200), comment='종목명')
    corp_nm = Column(String(200), comment='법인명')
    sector = Column(String(200), comment='섹터/업종')
    collect_source = Column(String(10), default=None, comment='수집구분 (manual/auto, NULL=미수집)')
    created_date = Column(DateTime, default=datetime.now)
    updated_date = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        # [최적화] 시장별 종목 검색 속도 향상
        Index('idx_item_mst_market', 'market_type', 'item_cd'),
    )

class ItemPrice(Base):
    """일별 시세 테이블"""
    __tablename__ = 'item_price'
    
    # [수정] 길이 확장
    item_cd = Column(String(20), primary_key=True) 
    trade_date = Column(String(8), primary_key=True, comment='거래일자 (YYYYMMDD)')
    
    # [신규] 시장 구분
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')

    # [수정] 미국 주식 소수점 가격 대응을 위해 Integer -> Float 변경
    stck_clpr = Column(Float, comment='종가')
    stck_oprc = Column(Float, comment='시가')
    stck_hgpr = Column(Float, comment='고가')
    stck_lwpr = Column(Float, comment='저가')
    
    acml_vol = Column(BigInteger, comment='누적거래량')
    acml_tr_pbmn = Column(BigInteger, comment='누적거래대금')
    
    prdy_vrss = Column(Float, comment='전일대비') # Float 변경
    prdy_vrss_sign = Column(Integer, comment='전일대비부호')
    
    ma5 = Column(Float, comment='5일 이동평균')
    ma10 = Column(Float, comment='10일 이동평균')
    ma20 = Column(Float, comment='20일 이동평균')
    ma60 = Column(Float, comment='60일 이동평균')
    ma120 = Column(Float, comment='120일 이동평균')
    ma240 = Column(Float, comment='240일 이동평균')
    
    __table_args__ = (
        # [최적화] 종목코드와 날짜를 묶은 복합 인덱스 추가 (조회 속도 핵심)
        Index('idx_item_price_cd_date', 'item_cd', 'trade_date'),
        # [최적화] 날짜 단독 검색을 위한 인덱스
        Index('idx_item_price_date', 'trade_date'),
    )

class ItemEquity(Base):
    """종목 기본 정보"""
    __tablename__ = 'item_equity'
    
    # [수정] 길이 확장
    item_cd = Column(String(20), primary_key=True)
    
    # [신규] 시장 구분
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')

    bstp_kor_isnm = Column(String(100), comment='업종명')
    lstn_stcn = Column(BigInteger, comment='상장주수')
    hts_avls = Column(BigInteger, comment='시가총액')
    frgn_ntby_qty = Column(BigInteger, comment='외국인순매수수량')
    frgn_hldn_qty = Column(BigInteger, comment='외국인보유수량')
    hts_frgn_ehrt = Column(Float, comment='외국인소진율')
    pgtr_ntby_qty = Column(BigInteger, comment='프로그램순매수수량')
    
    # [수정] 가격 컬럼 Float 변경
    w52_hgpr = Column(Float, comment='52주최고가')
    w52_hgpr_date = Column(String(8), comment='52주최고가일자')
    w52_lwpr = Column(Float, comment='52주최저가')
    w52_lwpr_date = Column(String(8), comment='52주최저가일자')
    stck_dryy_hgpr = Column(Float, comment='연중최고가')
    stck_dryy_lwpr = Column(Float, comment='연중최저가')
    
    dryy_hgpr_vrss_prpr_rate = Column(Float, comment='연중최고가대비현재가비율')
    dryy_lwpr_vrss_prpr_rate = Column(Float, comment='연중최저가대비현재가비율')
    per = Column(Float, comment='PER')
    pbr = Column(Float, comment='PBR')
    eps = Column(Float, comment='EPS')
    bps = Column(Float, comment='BPS')
    updated_date = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    dividend_yield = Column(Float, comment='배당수익률')

    loan_rate = Column(Float, comment='신용잔고율')
    stat_code = Column(String(10), comment='종목상태코드')
    is_short_over = Column(String(1), comment='단기과열여부')
    vol_turnover = Column(Float, comment='거래량회전율')
    
    # [수정] 피벗 가격 Float 변경
    pvt_res = Column(Float, comment='피벗저항')
    pvt_res1 = Column(Float, comment='피벗1차저항')
    pvt_res2 = Column(Float, comment='피벗2차저항')
    pvt_sup = Column(Float, comment='피벗지지')
    pvt_sup1 = Column(Float, comment='피벗1차지지')
    pvt_sup2 = Column(Float, comment='피벗2차지지')
    pvt = Column(Float, comment='피벗')

class FinancialSheet(Base):
    """재무제표 테이블"""
    __tablename__ = 'financial_sheet'
    
    # [수정] 길이 확장
    item_cd = Column(String(20), primary_key=True)
    base_date = Column(String(8), primary_key=True, comment='기준일자 (YYYYMMDD)')
    sheet_cl = Column(String(1), primary_key=True, comment='시트구분 (0:연간, 1:분기)')
    stac_yymm = Column(String(6), primary_key=True, comment='결산년월')
    
    grs = Column(Float, comment='매출액증가율')
    bsop_prfi_inrt = Column(Float, comment='영업이익증가율')
    ntin_inrt = Column(Float, comment='순이익증가율')
    roe_val = Column(Float, comment='ROE')
    thtr_ntin = Column(BigInteger, comment='당기순이익')
    rsrv_rate = Column(Float, comment='유보율')
    lblt_rate = Column(Float, comment='부채비율')
    eps = Column(Float, comment='EPS')
    bps = Column(Float, comment='BPS')
    sps = Column(Float, comment='SPS')
    revenue = Column(BigInteger, comment='매출액')          # 활동성 계산용
    total_assets = Column(BigInteger, comment='자산총계')     # 활동성 계산용
    total_equity = Column(BigInteger, comment='자본총계')     # SRIM 계산용 (지배주주지분 대용)
    cf_oa = Column(BigInteger, comment='영업활동현금흐름')    # 현금흐름 계산용
    cf_ia = Column(BigInteger, comment='투자활동현금흐름')    # 현금흐름 계산용

class TradeStatus(Base):
    """매매 상태 테이블"""
    __tablename__ = 'trade_status'
    
    trade_id = Column(Integer, Sequence('trade_status_id_seq'), primary_key=True, autoincrement=True)
    
    # [수정] 길이 확장 & 시장 구분
    item_cd = Column(String(20), nullable=False)
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    trade_date = Column(String(8), nullable=False)
    trade_type = Column(String(2), nullable=False, comment='BS:매수, SS:매도')
    odno = Column(String(20), comment='주문번호')
    qty = Column(Integer, comment='수량')
    
    # [수정] 가격 Float 변경
    trade_price = Column(Float, comment='거래가격')
    trade_time = Column(String(6), comment='거래시간')
    
    __table_args__ = (
        Index('idx_trade_status_item_date', 'item_cd', 'trade_date'),
    )

class TradeHistory(Base):
    """매매 이력 테이블"""
    __tablename__ = 'trade_history'
    
    id = Column(Integer, Sequence('trade_history_id_seq'), primary_key=True, autoincrement=True)
    
    # [수정] 길이 확장 & 시장 구분
    item_cd = Column(String(20), nullable=False)
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    trade_date = Column(String(8), nullable=False)
    trade_time = Column(String(6), nullable=False)
    trade_type = Column(String(10), nullable=False, comment='buy:매수, sell:매도')
    quantity = Column(Integer, comment='수량')
    
    # [수정] 가격 및 금액 Float 변경 (달러/원화 호환)
    price = Column(Float, comment='가격')
    amount = Column(Float, comment='거래금액')
    fee = Column(Float, default=0, comment='수수료')
    tax = Column(Float, default=0, comment='세금')
    profit = Column(Float, comment='실현손익')
    
    profit_rate = Column(Float, comment='수익률')
    trade_source = Column(String(20), default='manual', comment='manual:수동, auto:자동')
    trade_reason = Column(String(200), comment='매매 사유')
    rmk = Column(Text, comment='비고')
    created_at = Column(DateTime, default=datetime.now)
    __table_args__ = (
        Index('idx_trade_history_date', 'trade_date'),
        Index('idx_trade_history_source', 'trade_source'),
    )

class VirtualAccount(Base):
    """가상 계좌 테이블 (시장별 분리 운영)"""
    __tablename__ = 'virtual_account'
    
    id = Column(Integer, Sequence('virtual_account_id_seq'), primary_key=True, autoincrement=True)
    
    # [신규] 시장 구분 (KR용 계좌, US용 계좌 분리)
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    # [수정] 달러 소수점 대응 Float 변경
    balance = Column(Float, comment='예수금')
    total_eval = Column(Float, comment='총평가금액')
    total_profit = Column(Float, comment='총손익')
    
    total_profit_rate = Column(Float, comment='총수익률')
    updated_date = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class VirtualHolding(Base):
    """가상 보유 종목 테이블"""
    __tablename__ = 'virtual_holding'
    
    # [수정] 길이 확장
    item_cd = Column(String(20), primary_key=True)
    
    # [신규] 시장 구분 (어떤 시장의 종목인지)
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    item_nm = Column(String(200), comment='종목명')
    quantity = Column(Integer, comment='보유수량')
    
    # [수정] 가격 Float 변경
    avg_price = Column(Float, comment='평균매입가')
    current_price = Column(Float, comment='현재가')
    eval_amt = Column(Float, comment='평가금액')
    profit = Column(Float, comment='평가손익')
    
    profit_rate = Column(Float, comment='수익률')
    buy_date = Column(String(8), comment='최초매수일')
    updated_date = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class ScheduleLog(Base):
    """스케줄 실행 로그 테이블"""
    __tablename__ = 'schedule_log'
    
    id = Column(Integer, Sequence('schedule_log_id_seq'), primary_key=True, autoincrement=True)
    
    schedule_id = Column(String(50), nullable=False)
    schedule_name = Column(String(100))
    
    # [신규] 시장 구분 (어떤 시장 관련 작업인지)
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US/ALL)')
    
    task_type = Column(String(50))
    status = Column(String(20), comment='running, success, failed')
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    message = Column(Text)
    error_message = Column(Text)
    __table_args__ = (
        Index('idx_schedule_log_date', 'start_time'),
    )

class ScheduleItem(Base):
    """스케줄 설정 저장 테이블"""
    __tablename__ = 'schedule_item'
    
    id = Column(Integer, Sequence('schedule_item_id_seq'), primary_key=True, autoincrement=True)
    
    # [신규] 시장 구분 (시장별 스케줄 별도 운영)
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    name = Column(String(100), nullable=False, comment='스케줄 이름')
    task_type = Column(String(50), nullable=False, comment='작업 유형')
    cron_expression = Column(String(50), nullable=False, comment='Cron 표현식')
    enabled = Column(Boolean, default=True, comment='활성화 여부')
    created_at = Column(DateTime, default=datetime.now)

class EvaluationResult(Base):
    """종목 평가 결과 테이블"""
    __tablename__ = 'evaluation_result'
    
    # [수정] 길이 확장
    item_cd = Column(String(20), primary_key=True, comment='종목코드')
    base_date = Column(String(8), primary_key=True, comment='기준일자 (YYYYMMDD)')
    
    # [신규] 시장 구분
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    item_nm = Column(String(200), comment='종목명')
    sheet_score = Column(Integer, default=0, comment='1.재무제표 점수')
    trend_score = Column(Integer, default=0, comment='2.주가 모멘텀 점수')
    price_score = Column(Integer, default=0, comment='3.주가 점수')
    kpi_score = Column(Integer, default=0, comment='4.보조지표 점수')
    buy_score = Column(Integer, default=0, comment='5.수급 점수')
    avls_score = Column(Integer, default=0, comment='6.시가총액 점수')
    per_score = Column(Integer, default=0, comment='7.PER 점수')
    pbr_score = Column(Integer, default=0, comment='8.PBR 점수')
    total_score = Column(Integer, default=0, comment='총점')
    is_buy_candidate = Column(Boolean, default=False, comment='매수 후보 여부')
    
    # [수정] 가격 및 금액 Float 변경
    current_price = Column(Float, comment='현재가')
    market_cap = Column(BigInteger, comment='시가총액') # 시총은 숫자가 너무 커서 BigInteger 유지 (미국도 센트 단위 아님)
    
    per = Column(Float, comment='PER')
    pbr = Column(Float, comment='PBR')
    srim_price = Column(Float, default=0, comment='SRIM 적정주가') # Float 변경
    
    srim_pass = Column(Boolean, default=False, comment='SRIM 통과여부')
    cashflow_pass = Column(Boolean, default=False, comment='현금흐름 통과여부')
    activity_pass = Column(Boolean, default=False, comment='활동성 통과여부')
    dividend_pass = Column(Boolean, default=False, comment='배당 통과여부')
    roe_pass = Column(Boolean, default=False, comment='ROE 통과여부')
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    __table_args__ = (
        Index('idx_eval_result_date', 'base_date'),
        Index('idx_eval_result_score', 'total_score'),
    )

class Holdings(Base):
    """보유 종목 테이블 (실계좌용)"""
    __tablename__ = 'holdings'
    
    # [수정] 길이 확장
    item_cd = Column(String(20), primary_key=True, comment='종목코드')
    
    # [신규] 시장 구분
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    item_nm = Column(String(200), comment='종목명')
    quantity = Column(Integer, default=0, comment='보유수량')
    
    # [수정] 가격 Float 변경
    avg_price = Column(Float, default=0, comment='평균매입가')
    current_price = Column(Float, default=0, comment='현재가')
    highest_price = Column(Float, comment='최고가 (트레일링 스탑용)')
    
    buy_date = Column(String(8), comment='최초매수일')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class UserBuyTarget(Base):
    """사용자 정의 매수 대상 종목 (관심종목 등)"""
    __tablename__ = 'user_buy_target'
    
    # [수정] 길이 확장
    item_cd = Column(String(20), primary_key=True, comment='종목코드')
    
    # [신규] 시장 구분
    market_type = Column(String(10), default='KR', comment='시장유형 (KR/US)')
    
    item_nm = Column(String(200), comment='종목명')
    group_name = Column(String(100), comment='그룹명(출처)')
    exch_code = Column(String(5), comment='거래소코드')
    created_at = Column(DateTime, default=datetime.now)
    
    @property
    def total_score(self):
        return 999 
        
    @property
    def is_buy_candidate(self):
        return True

# ============ 데이터베이스 연결 관리 (기존 로직 유지) ============

class DatabaseManager:
    """데이터베이스 연결 관리자"""
    _instance: Optional['DatabaseManager'] = None
    _engine = None
    _session_factory = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._engine is None:
            self._initialize()
    
    def _initialize(self):
        """엔진 및 세션 팩토리 초기화"""
        settings_manager = get_settings_manager()
        settings = settings_manager.settings
        
        connection_string = settings_manager.get_db_connection_string()
        
        logger.info(f"데이터베이스 연결 초기화 중... (Type: {settings.database.db_type})")
        
        connect_args = {}
        if settings.database.db_type == "oracle":
            wallet_path = settings.database.oracle_wallet_path
            if wallet_path and os.path.exists(wallet_path):
                connect_args = {
                    "config_dir": wallet_path,
                    "wallet_location": wallet_path,
                    "wallet_password": settings.database.oracle_password 
                }
                logger.info(f"Oracle Thin Mode 활성화 (Wallet: {wallet_path})")
            else:
                logger.warning(f"Wallet 경로를 찾을 수 없음: {wallet_path}")

        try:
            if 'sqlite' in connection_string:
                self._engine = create_engine(
                    connection_string,
                    echo=False,
                    connect_args={
                        "check_same_thread": False,
                        "timeout": 60
                    }
                )
                @event.listens_for(self._engine, "connect")
                def set_sqlite_pragma(dbapi_connection, connection_record):
                    cursor = dbapi_connection.cursor()
                    cursor.execute("PRAGMA journal_mode=WAL")      # 쓰기 성능 및 읽기 동시성 향상
                    cursor.execute("PRAGMA synchronous=NORMAL")  # 쓰기 속도 향상
                    cursor.execute("PRAGMA cache_size=-10000")   # 캐시 메모리 약 10MB 할당
                    cursor.execute("PRAGMA temp_store=MEMORY")    # 임시 테이블 메모리 사용
                    cursor.execute("PRAGMA mmap_size=30000000000") # Memory Map 사용으로 대량 조회 가속
                    cursor.close()
            else:
                self._engine = create_engine(
                    connection_string,
                    echo=False,
                    pool_size=5,
                    max_overflow=10,
                    pool_recycle=1800,
                    pool_pre_ping=True,
                    connect_args=connect_args
                )

                @event.listens_for(self._engine, "connect")
                def do_connect(dbapi_connection, connection_record):
                    cursor = dbapi_connection.cursor()
                    try:
                        cursor.execute("ALTER SESSION DISABLE PARALLEL DML")
                    except Exception:
                        pass
                    finally:
                        cursor.close()
            
            with self._engine.connect() as conn:
                logger.info("데이터베이스 연결 성공!")
                
            Base.metadata.create_all(self._engine)

            # 2. [추가] 기존 DB에 인덱스가 없는 경우 자동 생성
            self.create_indexes_if_not_exists(self._engine)

            self._session_factory = sessionmaker(bind=self._engine)
            
        except Exception as e:
            logger.error(f"데이터베이스 엔진 생성 중 치명적 오류: {e}")
            raise e
    
    
    def create_indexes_if_not_exists(self, engine):
        """기존 테이블에 누락된 인덱스를 자동으로 생성하는 함수"""
        
        # 1. 실행할 인덱스 쿼리 정의 (SQLite/Oracle 공용 또는 분기)
        # SQLite는 'IF NOT EXISTS'를 지원하여 관리가 매우 쉽습니다.
        index_queries = [
            # ItemPrice 최적화
            "CREATE INDEX IF NOT EXISTS idx_item_price_cd_date ON item_price (item_cd, trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_item_price_date ON item_price (trade_date)",
            
            # ItemMst 최적화
            "CREATE INDEX IF NOT EXISTS idx_item_mst_market ON item_mst (market_type, item_cd)",
            
            # Trade & Evaluation 최적화
            "CREATE INDEX IF NOT EXISTS idx_eval_result_date ON evaluation_result (base_date)",
            "CREATE INDEX IF NOT EXISTS idx_eval_result_score ON evaluation_result (total_score)",
            "CREATE INDEX IF NOT EXISTS idx_trade_history_date ON trade_history (trade_date)"
        ]

        is_sqlite = 'sqlite' in str(engine.url)
        
        with engine.connect() as conn:
            logger.info("데이터베이스 인덱스 체크 및 최적화 시작...")
            
            for query in index_queries:
                try:
                    # Oracle인 경우 'IF NOT EXISTS' 문법이 없으므로 예외 처리로 대응
                    if not is_sqlite:
                        # Oracle 전용: 'IF NOT EXISTS' 제거 및 'ONLINE' 옵션 추가 고려
                        oracle_query = query.replace("IF NOT EXISTS ", "")
                        # 이미 인덱스가 있는지 먼저 확인하는 로직을 넣거나, try-except로 처리
                        try:
                            conn.execute(text(oracle_query))
                            logger.info(f"인덱스 생성 완료 (Oracle): {oracle_query.split(' ')[2]}")
                        except Exception as e:
                            if "ORA-00955" in str(e): # 이미 존재하는 객체 에러 코드
                                pass
                            else:
                                raise e
                    else:
                        # SQLite 실행
                        conn.execute(text(query))
                except Exception as e:
                    logger.error(f"인덱스 생성 중 오류 발생: {e}")
            
            conn.commit()
            logger.info("데이터베이스 인덱스 최적화 완료.")
    
    
    def reinitialize(self):
        if self._engine:
            self._engine.dispose()
        self._engine = None
        self._session_factory = None
        self._initialize()
    
    @property
    def engine(self):
        return self._engine
    
    @contextmanager
    def get_session(self) -> Generator:
        if self._session_factory is None:
            self._initialize()
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"데이터베이스 세션 오류: {e}")
            raise
        finally:
            session.close()

def get_db() -> DatabaseManager:
    return DatabaseManager()

def get_session():
    return get_db().get_session()