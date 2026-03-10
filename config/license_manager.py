import json
import base64
import hmac
import hashlib
import logging
import requests
import sys
import os  # [추가] 환경 변수 읽기를 위해 추가
from pathlib import Path
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from core.definition import MarketType, LicenseLevel
from utils.hardware import get_hardware_id

# [윈도우 레지스트리 모듈]
try:
    import winreg
except ImportError:
    winreg = None  # 리눅스/맥 환경 대비

logger = logging.getLogger(__name__)

MASTER_SECRET = "SNOWBOT_SUPER_SECRET_SALT_VALUE_0001"
TRIAL_SECRET = "SNOWBOT_TRIAL_PROTECTION_KEY_v1"

REG_PATH = r"Software\Classes\CLSID\{E48D63A5-9821-4C21-9556-912839210382}"
REG_VALUE_NAME = "Data"

class LicenseManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LicenseManager, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance

    def __init__(self):
        if not self.initialized:
            self.current_license = None 
            self.expiration_date = None
            self.owner = ""
            self.is_trial = False
            self.update_required = False
            self.server_version = ""
            self.link_yn = "Y" 
            self.etc_link = ""

            # [경로 설정] OS별 호환성을 위해 Path.home() 사용
            try:
                home_dir = Path.home()
                self.trial_file_path = home_dir / ".snowbot" / "sys_config.dat"
            except Exception as e:
                # 매우 드문 경우지만 홈 디렉토리를 못 찾을 경우 실행 파일 경로 사용
                if getattr(sys, 'frozen', False):
                    base_path = Path(sys.executable).parent
                else:
                    base_path = Path(__file__).resolve().parent.parent
                self.trial_file_path = base_path / "config_data" / "sys_config.dat"
                logger.warning(f"홈 디렉토리 접근 불가로 경로 변경: {self.trial_file_path}")

            # 라이선스 검증 실행
            self._verify_license()
            
            if self.current_license:
                self.initialized = True
                status = "Trial" if self.is_trial else "Registered"
                logger.info(f"✅ 라이선스 등급: {self.current_license.value} (Owner: {self.owner}, Status: {status})")

    # =========================================================================
    # [버전 확인 로직 수정됨] auth.yaml 파일 대신 환경변수에서 로컬 버전을 가져옵니다.
    # =========================================================================
    def _get_local_version(self):
        # 환경 변수 SNOWBOT_VERSION을 읽어오고, 없으면 기본값 'v1.0.1' 반환
        return os.getenv("SNOWBOT_VERSION", "v1.0.1")

    # =========================================================================
    # [보안 핵심] 네트워크 시간 조회 함수
    # =========================================================================
    def get_network_time(self):
        # 1. Google 시도
        try:
            response = requests.head("http://www.google.com", timeout=2)
            if 'Date' in response.headers:
                server_time = parsedate_to_datetime(response.headers['Date'])
                return server_time.astimezone(timezone.utc).replace(tzinfo=None) + timedelta(hours=9)
        except:
            pass 

        # 2. Naver 시도
        try:
            response = requests.head("http://www.naver.com", timeout=2)
            if 'Date' in response.headers:
                server_time = parsedate_to_datetime(response.headers['Date'])
                return server_time.astimezone(timezone.utc).replace(tzinfo=None) + timedelta(hours=9)
        except:
            pass

        # 3. Fallback
        logger.warning("⚠️ 네트워크 시간 조회 실패. 시스템 시간(KST 변환)을 사용합니다.")
        return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=9)

    def _load_license_key_from_file(self):
        if getattr(sys, 'frozen', False):
            root = Path(sys.executable).parent
        else:
            root = Path(__file__).resolve().parent.parent 
            
        key_path = root / "license.key"
        if key_path.exists():
            with open(key_path, "r", encoding="utf-8") as f:
                return f.read().strip()
        return None
    
    def _fetch_server_policy(self):
        """
        서버에서 Lock-in 정책, 체험 기간, 시작일, 버전을 가져옵니다.
        통신 오류 시 예외(Unpacking Error)를 방지하기 위해 4개의 기본값을 반환합니다.
        """
        api_url = "https://basedvalue.co.kr/api/items/policy/snowbot"
        
        try:
            # 3초 안에 응답이 없으면 서버 장애로 간주
            response = requests.get(api_url, timeout=3.0)
            if response.status_code == 200:
                data = response.json()
                lock_in = str(data.get("lockInYn", "N")).upper() == "Y"
                trial_days = int(data.get("trialDays", 0))
                start_date = data.get("startDate", "")
                app_version = data.get("appVersion", "")
                link_yn = data.get("linkYn", "Y")
                etc_link = data.get("etcLink", "")
                return lock_in, trial_days, start_date, app_version, link_yn, etc_link
            else:
                logger.warning(f"⚠️ 정책 서버 응답 오류({response.status_code}). 무제한 무료 모드로 실행합니다.")
                return False, 0, "", "", "Y", ""
        except Exception as e:
            logger.warning(f"⚠️ 정책 서버 통신 실패. 무제한 무료 모드로 실행합니다. ({e})")
            return False, 0, "", "", "Y", ""

    def _verify_license(self):
        """서버 정책(버전/락인) 확인 -> 정식 라이선스 확인 -> 체험판 로직 수행"""
        
        # 1. 서버 정책 조회
        lock_in_enabled, trial_days, server_start_date, server_version, link_yn, etc_link = self._fetch_server_policy()
        
        # [신규] 2. 버전 체크 로직
        current_local_version = self._get_local_version()
        
        # v 접두사 통일 (비교를 명확히 하기 위함)
        if current_local_version and not current_local_version.startswith('v'):
            current_local_version = f"v{current_local_version}"
        if server_version and not server_version.startswith('v'):
            server_version = f"v{server_version}"

        self.server_version = server_version
        self.link_yn = link_yn
        self.etc_link = etc_link
        
        if server_version and current_local_version != server_version:
            logger.error(f"⛔ 버전 불일치: 업데이트가 필요합니다. (현재: {current_local_version}, 최신: {server_version})")
            self.update_required = True
            
        # 3. 락인 해제 상태 (또는 서버 통신 실패) -> 무조건 프리패스
        if not lock_in_enabled:
            self.current_license = LicenseLevel.PREMIUM
            self.is_trial = False
            self.expiration_date = None
            self.owner = "Guest (free)"
            return

        # 4. 락인 상태 -> 로컬 정식 라이선스 키 확인
        license_key = self._load_license_key_from_file()
        if license_key:
            try:
                self._verify_official_key()
                # 정식 키가 유효하다면 여기서 종료
                if self.current_license:
                    self.is_trial = False
                    return
            except Exception as e:
                logger.error(f"정식 라이선스 검증 실패: {e}")
                self.current_license = None

        # 5. 정식 키가 없거나 무효하다면 체험판 로직 수행
        self._check_trial_period(trial_days, server_start_date)

    def _verify_official_key(self):
        license_key = self._load_license_key_from_file()
        if not license_key: return

        try:
            decoded_str = base64.b64decode(license_key).decode()
            data = json.loads(decoded_str)
            
            level = data.get("level")
            expire_str = data.get("expire")
            owner = data.get("owner")
            target_hw_id = data.get("hw_id")
            sign_received = data.get("sign")

            raw_str = f"{level}|{expire_str}|{owner}|{target_hw_id}"
            sign_calculated = hmac.new(
                MASTER_SECRET.encode(), 
                raw_str.encode(), 
                hashlib.sha256
            ).hexdigest()

            if sign_received != sign_calculated:
                logger.error("⛔ 라이선스 키가 위변조되었습니다!")
                return
            
            if target_hw_id != get_hardware_id():
                logger.error("⛔ 하드웨어 ID 불일치")
                return

            now_time = self.get_network_time()
            expire_date = datetime.strptime(expire_str, "%Y%m%d")
            
            if now_time.date() > expire_date.date():
                logger.warning(f"⛔ 라이선스가 만료되었습니다. ({expire_str})")
                return

            if level == "PREMIUM":
                self.current_license = LicenseLevel.PREMIUM
            else: 
                self.current_license = LicenseLevel.BASIC
            
            self.expiration_date = expire_date
            self.owner = owner
            
        except Exception as e:
            logger.error(f"라이선스 검증 오류: {e}")

    # =========================================================================
    # [보안 강화] 레지스트리 읽기/쓰기 함수
    # =========================================================================
    def _read_registry(self):
        if not winreg: return None
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, REG_PATH, 0, winreg.KEY_READ)
            value, _ = winreg.QueryValueEx(key, REG_VALUE_NAME)
            winreg.CloseKey(key)
            return value
        except FileNotFoundError:
            return None
        except Exception as e:
            return None

    def _write_registry(self, content):
        if not winreg: return
        try:
            key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, REG_PATH)
            winreg.SetValueEx(key, REG_VALUE_NAME, 0, winreg.REG_SZ, content)
            winreg.CloseKey(key)
        except Exception as e:
            logger.error(f"레지스트리 쓰기 실패: {e}")

    def _check_trial_period(self, trial_days: int, server_start_date: str):
        """체험판 로직 (이중 저장소 검증 및 서버 시작일 비교)"""
        try:
            if not self.trial_file_path.parent.exists():
                try:
                    self.trial_file_path.parent.mkdir(parents=True, exist_ok=True)
                except OSError:
                    pass

            current_hw_id = get_hardware_id()
            now_time = self.get_network_time()
            now_date_str = now_time.strftime("%Y%m%d")

            file_content = None
            if self.trial_file_path.exists():
                try:
                    with open(self.trial_file_path, 'r', encoding='utf-8') as f:
                        file_content = f.read().strip()
                except: pass
            
            reg_content = self._read_registry()

            final_content = None
            
            if not file_content and not reg_content:
                final_content = None 
            elif not file_content and reg_content:
                logger.warning("🛡️ 파일 삭제가 감지되어 레지스트리에서 복구합니다.")
                final_content = reg_content
                self._save_to_file_only(reg_content)
            elif file_content and not reg_content:
                final_content = file_content
                self._write_registry(file_content)
            else:
                final_content = self._resolve_conflict(file_content, reg_content)

            start_date_str = None
            
            if final_content:
                try:
                    json_str = base64.b64decode(final_content).decode()
                    data = json.loads(json_str)
                    
                    saved_date = data.get("d")
                    saved_hw = data.get("h")
                    saved_sign = data.get("s")
                    saved_last_run = data.get("l", saved_date)
                    
                    if saved_hw != current_hw_id:
                        logger.warning("⛔ 타 PC 데이터 복사 감지")
                        self._invalidate_trial()
                        return

                    verify_payload = f"{saved_date}|{saved_hw}|{saved_last_run}"
                    expected_sign = hmac.new(
                        TRIAL_SECRET.encode(), 
                        verify_payload.encode(), 
                        hashlib.sha256
                    ).hexdigest()
                    
                    if saved_sign != expected_sign:
                        logger.error("⛔ 데이터 위변조 감지")
                        self._invalidate_trial()
                        return
                    
                    last_run_date = datetime.strptime(saved_last_run, "%Y%m%d")
                    if now_time.date() < last_run_date.date():
                        logger.error(f"⛔ 시스템 시간 조작 감지 (Last: {saved_last_run})")
                        self._invalidate_trial()
                        return

                    start_date_str = saved_date

                except Exception:
                    logger.error("⛔ 데이터 손상")
                    self._invalidate_trial()
                    return
            else:
                # 최초 실행
                start_date_str = now_date_str

            # [수정됨] 실행 기록 갱신 (서버 기준일이 아닌 순수 로컬의 '진짜 최초 실행일'을 유지)
            # 서버에서 일시적으로 이벤트 기준일을 줬다고 해서 로컬의 원본 설치일을 훼손하면 안 됨!
            self._save_trial_data(start_date_str, current_hw_id, now_date_str)

            # [핵심] 만료일 계산 시에만 서버 기준일(server_start_date)과 로컬 시작일 비교
            effective_start_date_str = start_date_str
            
            if server_start_date and len(server_start_date) == 8:
                if server_start_date > start_date_str:
                    logger.info(f"🎁 서버의 새로운 기준일({server_start_date})이 임시 적용되어 체험 기간이 리셋/연장됩니다.")
                    effective_start_date_str = server_start_date
            
            # 5. 만료 체크 (가장 유리한 날짜 기준으로 계산)
            if effective_start_date_str:
                start_date = datetime.strptime(effective_start_date_str, "%Y%m%d")
                end_date = start_date + timedelta(days=trial_days)
                
                if now_time < end_date:
                    self.current_license = LicenseLevel.PREMIUM
                    self.expiration_date = end_date
                    self.owner = "Guest (체험판)"
                    self.is_trial = True
                    logger.info(f"🧪 체험판 모드 (남은 기간: {(end_date - now_time).days}일, 만료일: {end_date.strftime('%Y-%m-%d')})")
                else:
                    logger.warning("⛔ 체험판 기간이 만료되었습니다.")
                    self._invalidate_trial()

        except Exception as e:
            logger.error(f"체험판 로직 오류: {e}")
            self._invalidate_trial()

    def _resolve_conflict(self, file_content, reg_content):
        try:
            f_data = json.loads(base64.b64decode(file_content).decode())
            f_date = f_data.get("d")
            
            r_data = json.loads(base64.b64decode(reg_content).decode())
            r_date = r_data.get("d")
            
            if f_date > r_date:
                logger.warning("🛡️ 파일 데이터가 초기화된 정황 포착. 레지스트리 기록으로 덮어씁니다.")
                self._save_to_file_only(reg_content)
                return reg_content
            elif f_date < r_date:
                self._write_registry(file_content)
                return file_content
            else:
                f_last = f_data.get("l", f_date)
                r_last = r_data.get("l", r_date)
                if f_last >= r_last:
                    return file_content
                else:
                    return reg_content
        except:
            return file_content

    def _save_trial_data(self, date_str, hw_id, last_run_date):
        try:
            payload = f"{date_str}|{hw_id}|{last_run_date}"
            signature = hmac.new(
                TRIAL_SECRET.encode(), 
                payload.encode(), 
                hashlib.sha256
            ).hexdigest()
            
            data = {
                "d": date_str,
                "h": hw_id,
                "l": last_run_date,
                "s": signature
            }
            
            json_str = json.dumps(data)
            encoded_content = base64.b64encode(json_str.encode()).decode()
            
            self._save_to_file_only(encoded_content)
            self._write_registry(encoded_content)
            
        except Exception as e:
            logger.error(f"체험판 데이터 저장 실패: {e}")

    def _save_to_file_only(self, content):
        try:
            with open(self.trial_file_path, 'w', encoding='utf-8') as f:
                f.write(content)
        except: pass

    def _invalidate_trial(self):
        self.current_license = None
        self.is_trial = False

    def _check_expiration_realtime(self):
        if not self.is_trial or not self.expiration_date:
            return

        now = self.get_network_time()
        expire_limit = self.expiration_date + timedelta(days=1)
        
        if now >= expire_limit:
            logger.warning(f"⛔ 실시간 감지: 체험판 기간이 만료되었습니다. (만료일: {self.expiration_date.strftime('%Y-%m-%d')}, 현재: {now.strftime('%Y-%m-%d %H:%M')})")
            self._invalidate_trial()

    def get_allowed_markets(self):
        self._check_expiration_realtime()
        allowed = []
        if self.current_license:
            allowed.append(MarketType.KR)
            if self.current_license == LicenseLevel.PREMIUM:
                allowed.append(MarketType.US)
        return allowed

    def is_market_allowed(self, market: MarketType) -> bool:
        return market in self.get_allowed_markets()