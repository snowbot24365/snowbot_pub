import logging
import json
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import sys

logger = logging.getLogger(__name__)

if getattr(sys, 'frozen', False):
    # 1. EXE 실행 모드: 실행 파일(.exe)이 있는 폴더를 기준(ROOT)으로 잡음
    ROOT_DIR = Path(sys.executable).parent
else:
    # 2. 개발(Script) 모드: 현재 파일의 위치를 기준으로 상위 폴더를 잡음
    # (기존 코드: Path(__file__).parent.parent)
    ROOT_DIR = Path(__file__).resolve().parent.parent

# 토큰 저장 경로
TOKEN_DIR = ROOT_DIR / "config_data"
TOKEN_FILE = TOKEN_DIR / "kis_tokens.json"

class KISTokenManager:
    """한국투자증권 API 토큰 관리자 (싱글톤, 시장/모드별 분리)"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized: return
        
        # 관리할 토큰 키 목록 (시장_모드)
        self.token_keys = ['KR_mock', 'KR_real', 'US_mock', 'US_real']
        
        # 토큰 저장소 초기화
        self._tokens = {}
        for key in self.token_keys:
            self._tokens[key] = {
                'access_token': None, 
                'token_expires': None, 
                'issue_count': 0, 
                'issue_date': None
            }
            
        self._load_tokens()
        self._initialized = True
    
    def _load_tokens(self):
        """파일에서 토큰 정보 로드"""
        try:
            TOKEN_DIR.mkdir(exist_ok=True)
            if TOKEN_FILE.exists():
                with open(TOKEN_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                for key in self.token_keys:
                    if key in data:
                        token_data = self._tokens[key]
                        saved_data = data[key]
                        
                        token_data['access_token'] = saved_data.get('access_token')
                        
                        expires_str = saved_data.get('token_expires')
                        if expires_str:
                            token_data['token_expires'] = datetime.fromisoformat(expires_str)
                            
                        token_data['issue_count'] = saved_data.get('issue_count', 0)
                        
                        issue_date_str = saved_data.get('issue_date')
                        if issue_date_str:
                            token_data['issue_date'] = datetime.fromisoformat(issue_date_str).date()
                            
        except Exception as e:
            logger.warning(f"토큰 파일 로드 실패: {e}")
    
    def _save_tokens(self):
        """토큰 정보를 파일에 저장"""
        try:
            TOKEN_DIR.mkdir(exist_ok=True)
            data = {}
            for key in self.token_keys:
                token_data = self._tokens[key]
                data[key] = {
                    'access_token': token_data['access_token'],
                    'token_expires': token_data['token_expires'].isoformat() if token_data['token_expires'] else None,
                    'issue_count': token_data['issue_count'],
                    'issue_date': token_data['issue_date'].isoformat() if token_data['issue_date'] else None
                }
            with open(TOKEN_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"토큰 파일 저장 실패: {e}")

    def clear_token(self, market: str, mode: str):
        """특정 시장/모드의 토큰 정보 삭제"""
        key = f"{market}_{mode}"
        if key in self._tokens:
            logger.info(f"[{key}] 만료된 토큰 정보를 삭제합니다.")
            self._tokens[key]['access_token'] = None
            self._tokens[key]['token_expires'] = None
            self._save_tokens()
    
    def get_token(self, market: str, mode: str, app_key: str, app_secret: str, base_url: str) -> Optional[str]:
        """토큰 가져오기 (만료 시 재발급)"""
        if mode not in ['mock', 'real']: return None
        
        # 키 생성 (예: KR_real, US_mock)
        key = f"{market}_{mode}"
        
        # 정의되지 않은 키라면(혹시 모를 오류 방지) 임시 초기화
        if key not in self._tokens:
            self._tokens[key] = {'access_token': None, 'token_expires': None, 'issue_count': 0, 'issue_date': None}
            
        token_data = self._tokens[key]
        
        # 1. 유효한 토큰이 있으면 반환
        if token_data['access_token'] and token_data['token_expires']:
            if datetime.now() < token_data['token_expires']:
                return token_data['access_token']
        
        # 2. 일일 발급 한도 체크 및 날짜 갱신
        today = date.today()
        if token_data['issue_date'] != today:
            token_data['issue_count'] = 0
            token_data['issue_date'] = today
        
        # 발급 횟수 경고 (실전/해외 등은 제한이 엄격할 수 있음)
        if token_data['issue_count'] >= 20: 
            logger.warning(f"[{key}] 일일 토큰 발급 횟수가 많습니다 ({token_data['issue_count']}). 주의하세요.")
        
        # 3. 새 토큰 발급 요청
        new_token = self._issue_new_token(key, app_key, app_secret, base_url)
        if new_token:
            token_data['access_token'] = new_token
            # 토큰 유효기간 (보통 24시간이나 안전하게 23시간으로 설정)
            token_data['token_expires'] = datetime.now() + timedelta(hours=23) 
            token_data['issue_count'] += 1
            token_data['issue_date'] = today
            self._save_tokens()
            return new_token
            
        return None
    
    def _issue_new_token(self, log_prefix: str, app_key: str, app_secret: str, base_url: str) -> Optional[str]:
        """실제 API 호출하여 토큰 발급"""
        try:
            url = f"{base_url}/oauth2/tokenP"
            headers = {"content-type": "application/json"}
            body = {"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret}
            
            logger.info(f"[{log_prefix}] 새 접근 토큰 발급 요청 중...")
            response = requests.post(url, headers=headers, json=body, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"[{log_prefix}] 토큰 발급 성공")
                return response.json().get('access_token')
            else:
                logger.error(f"[{log_prefix}] 토큰 발급 실패: {response.text}")
                return None
        except Exception as e:
            logger.error(f"[{log_prefix}] 토큰 발급 오류: {e}")
            return None

    def get_token_status(self, market: str, mode: str) -> Dict[str, Any]:
        """UI 표시용 토큰 상태 반환"""
        key = f"{market}_{mode}"
        token_data = self._tokens.get(key, {})
        
        is_valid = False
        remaining_time = timedelta(0)
        
        if token_data.get('access_token') and token_data.get('token_expires'):
            if datetime.now() < token_data['token_expires']:
                is_valid = True
                remaining_time = token_data['token_expires'] - datetime.now()
        
        return {
            'is_valid': is_valid,
            'remaining_time': remaining_time,
            'issue_count_today': token_data.get('issue_count', 0)
        }

def get_token_manager() -> KISTokenManager:
    return KISTokenManager()