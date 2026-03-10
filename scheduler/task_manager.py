import logging
from datetime import datetime, time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.base import STATE_RUNNING, STATE_PAUSED, STATE_STOPPED
import pytz 
import streamlit as st
import warnings
import pandas as pd
from config.database import get_session, ScheduleLog, ScheduleItem
from core.definition import MarketType
from data.kr.dart_collector import DataCollectionService
from data.kr.evaluator import EvaluationService
from data.us.us_collector import UsDataCollector
from data.us.evaluator import UsEvaluationService

# [수정] KR/US Trader 임포트 (AutoTrader 제거)
from impl.kr.kr_trader import KrTrader
from impl.us.us_trader import UsTrader

try:
    import pandas_market_calendars as mcal
except ImportError:
    mcal = None

logger = logging.getLogger(__name__)

class TaskType:
    DATA_COLLECTION = "data_collection"
    EVALUATION = "evaluation"
    AUTO_TRADE = "auto_trade"
    SYSTEM = "system"

class SchedulerService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return
        
        self.kst = pytz.timezone('Asia/Seoul')
        
        executors = {
            'default': ThreadPoolExecutor(10)
        }
        
        self.scheduler = BackgroundScheduler(
            timezone=self.kst, 
            executors=executors
        )
        
        self.scheduler.start()
        self._initialized = True
        
        logger.info(f"스케줄러 서비스 시작됨 (Timezone: Asia/Seoul, 현재시간: {datetime.now(self.kst)})")
        
        self._load_schedules_from_db()

    def is_running(self):
        return self.scheduler.state == STATE_RUNNING

    def start(self):
        if self.scheduler.state == STATE_PAUSED:
            self.scheduler.resume()
            logger.info("스케줄러 재개됨")
        elif self.scheduler.state == STATE_STOPPED:
            self.scheduler.start()
            logger.info("스케줄러 시작됨")

    def stop(self):
        if self.scheduler.state == STATE_RUNNING:
            self.scheduler.pause()
            logger.info("스케줄러 일시정지됨")

    # Cron 표현식 유효성 검증 헬퍼 메서드
    def validate_cron_expression(self, expression: str) -> bool:
        try:
            CronTrigger.from_crontab(expression, timezone=self.kst)
            return True
        except Exception:
            return False

    def add_schedule(self, name: str, task_type: str, cron_expression: str, market_type: str = "KR", enabled: bool = True):
        # -------------------------------------------------------------
        # DB 저장 전 Cron 표현식 유효성 먼저 검증
        # -------------------------------------------------------------
        if not self.validate_cron_expression(cron_expression):
            error_msg = f"잘못된 Cron 표현식입니다: {cron_expression}"
            logger.error(error_msg)
            # 예외를 발생시켜 UI에서 잡을 수 있게 함
            raise ValueError(error_msg)
        try:
            with get_session() as session:
                new_schedule = ScheduleItem(
                    name=name,
                    task_type=task_type,
                    market_type=market_type, # [신규] DB에 시장 정보 저장
                    cron_expression=cron_expression,
                    enabled=enabled,
                    created_at=datetime.now(self.kst)
                )
                session.add(new_schedule)
                session.commit()
                
                self._add_job_to_scheduler(new_schedule)
                logger.info(f"스케줄 추가 완료: {name} ({market_type})")
                
        except Exception as e:
            logger.error(f"스케줄 추가 실패: {e}")
            raise

    def delete_schedule(self, schedule_id: int):
        try:
            with get_session() as session:
                schedule = session.query(ScheduleItem).filter(ScheduleItem.id == schedule_id).first()
                if schedule:
                    if self.scheduler.get_job(str(schedule_id)):
                        self.scheduler.remove_job(str(schedule_id))
                    
                    session.delete(schedule)
                    session.commit()
                    logger.info(f"스케줄 삭제 완료: ID {schedule_id}")
        except Exception as e:
            logger.error(f"스케줄 삭제 실패: {e}")

    def get_schedules(self, market_type: str = None):
        """
        스케줄 목록 조회
        :param market_type: 'KR', 'US' 등 시장 구분 (None일 경우 전체 조회)
        """
        try:
            with get_session() as session:
                session.expire_on_commit = False
                
                # 쿼리 객체 생성
                query = session.query(ScheduleItem)
                
                # 시장 타입 필터링 적용
                if market_type:
                    query = query.filter(ScheduleItem.market_type == market_type)
                
                return query.all()
        except Exception as e:
            logger.error(f"스케줄 조회 실패: {e}")
            return []

    def get_schedule_logs(self, limit=20, type_filter: str = None, market_type: str = None):
        """
        스케줄 로그 조회
        :param limit: 조회 개수 제한
        :param type_filter: 작업 유형 필터 (TRADING, COLLECT 등)
        :param market_type: 시장 구분 필터 (KR, US) - [신규 추가]
        """
        try:
            with get_session() as session:
                query = session.query(ScheduleLog)

                # 1. 작업 유형(Task Type) 필터링
                if type_filter and type_filter != "전체":
                    type_mapping = {
                        "TRADING": TaskType.AUTO_TRADE,
                        "COLLECT": TaskType.DATA_COLLECTION,
                        "ANALYSIS": TaskType.EVALUATION,
                        "SYSTEM": TaskType.SYSTEM
                    }
                    db_type = type_mapping.get(type_filter, type_filter)
                    query = query.filter(ScheduleLog.task_type == db_type)

                # 2. 시장(Market Type) 필터링 [신규]
                if market_type:
                    query = query.filter(ScheduleLog.market_type == market_type)

                # 정렬 및 제한
                logs = query.order_by(ScheduleLog.start_time.desc()).limit(limit).all()
                
                return [
                    {
                        'status': log.status,
                        'schedule_name': log.schedule_name,
                        'task_type': log.task_type,
                        'market_type': log.market_type,
                        'start_time': log.start_time.strftime('%Y-%m-%d %H:%M:%S') if log.start_time else None,
                        'end_time': log.end_time.strftime('%H:%M:%S') if log.end_time else None,
                        'message': log.message,
                        'error_message': log.error_message
                    }
                    for log in logs
                ]
        except Exception as e:
            logger.error(f"로그 조회 실패: {e}")
            return []

    def _load_schedules_from_db(self):
        try:
            with get_session() as session:
                schedules = session.query(ScheduleItem).filter(ScheduleItem.enabled == True).all()
                for item in schedules:
                    self._add_job_to_scheduler(item)
            logger.info(f"초기 스케줄 로드 완료: {len(schedules)}건")
        except Exception as e:
            logger.error(f"스케줄 로드 실패: {e}")

    def _add_job_to_scheduler(self, item: ScheduleItem):
        try:
            trigger = CronTrigger.from_crontab(item.cron_expression, timezone=self.kst)
            
            # [수정] execute_task에 market_type 전달 추가
            market_type_str = item.market_type if item.market_type else "KR"
            
            self.scheduler.add_job(
                func=self.execute_task,
                trigger=trigger,
                args=[item.task_type, item.name, market_type_str], # 인자 추가
                id=str(item.id),
                replace_existing=True,
                misfire_grace_time=60,
                coalesce=True
            )
            logger.info(f"스케줄 등록: {item.name} ({market_type_str})")
        except Exception as e:
            logger.error(f"스케줄 등록 오류 ({item.name}): {e}")

    def execute_task(self, task_type: str, schedule_name: str, market_type: str = "KR"):
        """스케줄 작업 실행 (시장 구분 적용)"""
        logger.info(f"===== [{schedule_name}] 작업 시작 ({market_type} / {task_type}) =====")
        
        log_id = self._log_start(task_type, schedule_name, market_type)
        
        try:
            result_msg = ""
            
            # 1. 데이터 수집
            if task_type == TaskType.DATA_COLLECTION:
                # 여기서는 일단 기존 로직 유지하되 로그만 남김
                if market_type == "US":
                    service = UsDataCollector()
                    def log_wrapper(msg):
                        logger.info(f"[DataCollection-US] {msg}")
                    
                    # US 수집 실행
                    result = service.run_collection(
                        base_date=datetime.now(self.kst).date(),
                        collect_source="auto",
                        log_callback=log_wrapper
                    )
                    
                    # 결과 메시지 포맷팅
                    result_msg = (
                        f"종목:{result.get('items_collected', 0)}, "
                        f"상세:{result.get('financial_collected', 0)}, "
                        f"실패:{len(result.get('errors', []))}"
                    )
                else:
                    service = DataCollectionService()
                    def log_wrapper(msg):
                        logger.info(f"[DataCollection-KR] {msg}")

                    result = service.run_incremental_collection(
                        collect_source="auto", 
                        log_callback=log_wrapper
                    )
                    result_msg = (
                        f"종목:{result.get('items_collected',0)}, "
                        f"재무:{result.get('financial_collected',0)}, "
                        f"시세:{result.get('kis_collected',0)}"
                    )

            # 2. 종목 평가
            elif task_type == TaskType.EVALUATION:
                logger.info(f"[{market_type}] 종목 평가 시작...")

                # [수정] 시장 타입에 맞는 서비스 인스턴스 생성
                if market_type == "US":
                    service = UsEvaluationService()
                    log_tag = "Eval-US"
                else:
                    service = EvaluationService()
                    log_tag = "Eval-KR"
                
                # EvaluationService 내부에서 market_type을 처리하도록 개선 필요
                # 현재는 기존 로직(KR) 호출
                eval_result = service.run_evaluation(
                    base_date=datetime.now(self.kst).date(),
                    auto_detect_data_date=True,
                    log_callback=lambda msg: logger.info(f"[{log_tag}] {msg}")
                )
                
                count = eval_result.get('total_evaluated', 0)
                candidates = eval_result.get('buy_candidates', 0)
                result_msg = f"평가 완료: {count}개 종목, 매수후보 {candidates}건"

            # 3. 자동 매매 [핵심 수정 부분]
            elif task_type == TaskType.AUTO_TRADE:
                # 개장일이 아니면 매매 로직 건너뜀
                is_open, status_msg = self._is_market_open_time(market_type)

                if not is_open:
                    # 구체적인 사유(휴장일 or 시간아님)를 결과 메시지로 저장
                    result_msg = status_msg 
                    self._log_end(log_id, "skipped", result_msg)
                    logger.info(f"===== [{schedule_name}] 작업 스킵: {result_msg} =====")
                    return
                
                logger.info(f"[{market_type}] 자동매매 로직 실행 시작...")
                
                trader = None
                if market_type == "KR":
                    trader = KrTrader()
                elif market_type == "US":
                    trader = UsTrader()
                
                if trader:
                    log_output = trader.run()
                    # summary = log_output.replace('\n', ', ')
                    # if len(summary) > 100: summary = summary[:100] + "..."
                    result_msg = f"{log_output}"
                    logger.info(f"[{market_type}] 자동매매 결과:\n{log_output}")
                else:
                    result_msg = f"지원하지 않는 시장: {market_type}"

            else:
                result_msg = f"알 수 없는 작업 유형: {task_type}"
                logger.warning(result_msg)

            self._log_end(log_id, "success", result_msg)
            logger.info(f"===== [{schedule_name}] 작업 완료: {result_msg} =====")

        except Exception as e:
            logger.error(f"===== [{schedule_name}] 작업 실패: {e} =====", exc_info=True)
            self._log_end(log_id, "failed", error_msg=str(e))

    def _is_market_open_time(self, market_type: str) -> tuple[bool, str]:
        """
        현재 시간이 정규 장 운영 시간 내에 있는지 확인
        Return: (가능여부: bool, 사유: str)
        """
        if mcal is None:
            return True, "라이브러리 미설치(체크 건너뜀)"

        # 1. 시장 설정
        if market_type == "US":
            target_tz = pytz.timezone('America/New_York')
            exchange_code = "NYSE"
        else:
            target_tz = self.kst
            exchange_code = "XKRX"

        # 2. 현재 시간 (UTC)
        now_utc = pd.Timestamp.now(tz='UTC')
        target_date = datetime.now(target_tz).date()
        date_str = target_date.strftime("%Y-%m-%d")

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                calendar = mcal.get_calendar(exchange_code)

            # 3. 오늘 스케줄 조회
            schedule = calendar.schedule(start_date=target_date, end_date=target_date)

            # [CASE 1] 휴장일 체크
            if schedule.empty:
                if market_type == "US":
                    msg = f"[{market_type}] 오늘은 증시 휴장일입니다. (현지기준: {date_str})"
                else:
                    msg = f"[{market_type}] 오늘은 증시 휴장일입니다. ({date_str})"
                logger.info(msg)
                return False, msg

            # 4. 개장/마감 시간 추출
            market_open = schedule.iloc[0]['market_open']
            market_close = schedule.iloc[0]['market_close']

            # [CASE 2] 운영 시간 체크
            if market_open <= now_utc <= market_close:
                return True, "장 운영 중"
            else:
                local_now = datetime.now(target_tz).strftime('%H:%M')
                local_open = market_open.astimezone(target_tz).strftime('%H:%M')
                local_close = market_close.astimezone(target_tz).strftime('%H:%M')
                
                msg = f"장 운영 시간 아님 (현재: {local_now} / 운영: {local_open}~{local_close})"
                logger.info(f"[{market_type}] {msg}")
                return False, msg

        except Exception as e:
            logger.error(f"[{market_type}] 장 운영 시간 체크 중 오류: {e}")
            # 오류 시 안전하게 False 처리
            return False, f"체크 중 오류 발생: {str(e)}"
    
    def _log_start(self, task_type, name, market_type="KR"):
        try:
            with get_session() as session:
                log = ScheduleLog(
                    schedule_id=f"auto_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    schedule_name=name,
                    task_type=task_type,
                    market_type=market_type, # [신규] 시장 정보 저장
                    status="running",
                    start_time=datetime.now(self.kst)
                )
                session.add(log)
                session.commit()
                return log.id
        except Exception as e:
            logger.error(f"로그 시작 기록 실패: {e}")
            return 0

    def _log_end(self, log_id, status, msg=None, error_msg=None):
        if not log_id: return
        try:
            with get_session() as session:
                log = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
                if log:
                    log.status = status
                    log.end_time = datetime.now(self.kst)
                    log.message = msg
                    log.error_message = error_msg
                    session.commit()
        except Exception as e:
            logger.error(f"로그 종료 기록 실패: {e}")

@st.cache_resource
def get_scheduler() -> SchedulerService:
    return SchedulerService()