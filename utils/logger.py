import logging
import sys
from pathlib import Path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

def setup_logger(
    name: str = None, 
    level: int = logging.INFO,
    log_to_file: bool = True,
    log_dir: str = "logs",
    retention_days: int = 30 # [설정] 로그 보관 기간 (일)
) -> logging.Logger:
    """
    [핵심 수정]
    특정 이름이 아닌 '최상위 Root Logger'를 가져와서 핸들러를 부착합니다.
    그래야 logging.getLogger(__name__)을 쓰는 모든 모듈의 로그가 잡힙니다.
    """
    # 1. 이름 없이 호출하여 Root Logger 획득
    root_logger = logging.getLogger()
    
    # 이미 핸들러가 설정되어 있다면 중복 설정 방지
    if root_logger.hasHandlers():
        return root_logger
    
    root_logger.setLevel(level)
    
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 2. 콘솔 핸들러 (Streamlit/터미널 출력용)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 3. 파일 핸들러 (파일 기록용)
    if log_to_file:
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)
        
        file_name = name if name else "stock_trading"
        # [변경 포인트]
        # 날짜가 파일명에 바로 붙는 방식이 아니라, 
        # 평소엔 'stock_trading.log'에 적히다가 자정이 지나면 
        # 'stock_trading.log.2024-01-30' 처럼 뒤로 밀려나고 새 파일이 생깁니다.
        filename = log_path / f"{file_name}.log"
        
        file_handler = TimedRotatingFileHandler(
            filename=filename,
            when="midnight",    # 자정마다 파일 교체
            interval=1,         # 1일 간격
            backupCount=retention_days, # [핵심] 보관할 파일 개수 (30개가 넘으면 옛날 것 자동 삭제)
            encoding='utf-8'
        )
        
        # 생성될 백업 파일명 뒤에 붙을 날짜 포맷 (예: .2024-01-30)
        file_handler.suffix = "%Y-%m-%d"
        
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # -----------------------------------------------------------
    # [노이즈 제어] 타사 라이브러리 로그 레벨 조정
    # Root Logger를 잡았기 때문에 모든 라이브러리 로그가 들어옵니다.
    # 너무 시끄러운 애들은 WARNING 이상일 때만 기록하도록 조절합니다.
    # -----------------------------------------------------------
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("peewee").setLevel(logging.WARNING)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("streamlit").setLevel(logging.WARNING) # Streamlit 내부 로그 줄이기

    return root_logger


def get_logger(name: str = None) -> logging.Logger:
    """
    각 모듈에서 사용할 로거 반환
    """
    if name:
        return logging.getLogger(name)
    return logging.getLogger()


# [중요] 모듈 임포트 시점에 즉시 실행되어 Root Logger 설정을 완료함
default_logger = setup_logger()