import utils.logger
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from scheduler.task_manager import SchedulerService
import platform
from config.license_manager import LicenseManager
from core.definition import MarketType
from utils.hardware import get_hardware_id
from PIL import Image
import os
import sys
from pathlib import Path
import base64
from datetime import datetime
import requests
import zipfile
from datetime import datetime, date
from pathlib import Path
import webbrowser

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1472376415226695924/flPl2Gls8mepsTRK9_gzXNZOEd3Nk02o63Ymodpve6ZfOg-2sLkjmv0TA3Yb8S1SM-DF"

class LogManager:
    def __init__(self, root_dir):
        self.log_dir = root_dir / 'logs'
        self.zip_filename = None
        self.zip_path = None

    def collect_and_zip_logs(self, user_id=None):
        """최근 5일치 로그와 현재 로그를 찾아 압축합니다."""
        if not self.log_dir.exists():
            return False, "logs 폴더가 존재하지 않습니다."
        
        # 1. 파일명 생성 (user_id가 있으면 앞에 붙임)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if user_id:
            # 파일명에 쓸 수 없는 문자 제거 (안전성 확보)
            safe_id = "".join(c for c in str(user_id) if c.isalnum() or c in ('-', '_'))
            self.zip_filename = f"[{safe_id}]_snowbot_logs_{timestamp}.zip"
        else:
            self.zip_filename = f"snowbot_logs_{timestamp}.zip"
            
        self.zip_path = self.log_dir / self.zip_filename

        files_to_zip = []
        today = date.today()
        
        # 로그 폴더 내 파일 검색
        for file_path in self.log_dir.glob("stock_trading.log*"):
            filename = file_path.name
            
            # 1. 현재 기록 중인 로그
            if filename == "stock_trading.log":
                files_to_zip.append(file_path)
                continue
            
            # 2. 날짜별 백업 로그
            try:
                date_part = filename.split('.')[-1]
                log_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                
                # 최근 5일 이내인지 확인
                delta = (today - log_date).days
                if 0 <= delta <= 5:
                    files_to_zip.append(file_path)
            except ValueError:
                continue

        if not files_to_zip:
            return False, "전송할 로그 파일이 없습니다."

        try:
            with zipfile.ZipFile(self.zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file in files_to_zip:
                    zipf.write(file, arcname=file.name)
            return True, self.zip_path
        except Exception as e:
            return False, f"압축 중 오류 발생: {e}"

    def send_to_discord(self, user_id):
        """압축된 로그 파일을 디스코드 웹훅으로 전송합니다."""
        if "discord.com" not in DISCORD_WEBHOOK_URL:
            return False, "디스코드 웹훅 URL이 설정되지 않았습니다."

        success, result = self.collect_and_zip_logs(user_id)
        if not success:
            return False, result

        zip_file_path = result
        
        try:
            # 1. 사용자 정보 수집 (하드웨어 ID 등)
            hw_id = user_id or get_hardware_id()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 2. 메시지 내용 구성
            payload = {
                "content": f"🚨 **[로그 제보]**\n- 시간: `{timestamp}`\n- 사용자 ID: `{hw_id}`"
            }
            
            # 3. 파일 및 메시지 전송
            with open(zip_file_path, 'rb') as f:
                # files 딕셔너리의 키는 'file'이어야 디스코드에서 파일로 인식함
                response = requests.post(
                    DISCORD_WEBHOOK_URL,
                    data=payload,
                    files={'file': (self.zip_filename, f)}
                )

            # 4. 전송 후 압축 파일 삭제
            if os.path.exists(zip_file_path):
                os.remove(zip_file_path)

            if response.status_code in [200, 204]:
                return True, "개발자에게 로그를 성공적으로 전송했습니다! 📨"
            else:
                return False, f"전송 실패 (HTTP {response.status_code}): {response.text}"
                
        except Exception as e:
            return False, f"전송 중 오류 발생: {e}"


# ==========================================
# [중요] 경로 계산 함수 개선
# ==========================================
def get_root_dir():
    """
    실행 환경(Dev/Nuitka/PyInstaller)에 맞춰 프로젝트 루트 경로를 반환합니다.
    """
    if getattr(sys, 'frozen', False):
        # 1. PyInstaller EXE 실행 시: 실행 파일(exe)이 있는 폴더 기준
        return Path(sys.executable).parent
    else:
        # 2. 개발(Dev) 또는 Nuitka Pyd 실행 시
        # 이 파일(main_impl.py)은 'ui' 폴더 안에 있으므로, 
        # 루트로 가려면 부모(ui)의 부모(root)로 두 번 올라가야 합니다.
        # .resolve()를 써야 심볼릭 링크 등 경로 꼬임을 방지합니다.
        return Path(__file__).resolve().parent.parent

# 전역 변수로 ROOT_DIR 설정
ROOT_DIR = get_root_dir()

def get_img_as_base64(file_path):
    with open(file_path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

def load_auth_config():
    try:
        config_path = ROOT_DIR / 'config_data' / 'auth.yaml'
        
        with open(config_path, encoding='utf-8') as file:
            config = yaml.load(file, Loader=SafeLoader)
            # [수정됨] yaml 파일에 버전을 강제 주입하던 로직 삭제
            return config
    except FileNotFoundError:
        return {'enabled': False} 

@st.cache_resource
def init_scheduler():
    manager = SchedulerService()
    manager.start()
    return manager

def render_license_guide():
    st.markdown("#### 1️⃣ 라이선스 구매")
    # 카페 URL 설정 (필요시 특정 메뉴 게시판 URL로 변경 가능)
    seller_url = "https://basedvalue.co.kr/notice/6"
    # 링크 버튼 생성 (새 탭에서 열기)
    st.link_button("👉 라이선스 구매 안내", seller_url, type="primary")
    
    st.markdown("#### 2️⃣  하드웨어 ID 복사")
    st.caption("아래 ID를 복사 하세요.")
    
    my_hw_id = get_hardware_id()
    st.code(my_hw_id, language="text")   

    st.markdown("#### 3️⃣ 라이선스 등록")
    st.caption("발급받은 라이선스 키를 아래에 입력하세요.")
    
    # 키가 길 수 있으므로 text_area 사용
    license_key_input = st.text_area("라이선스 키", label_visibility="collapsed", height=100)
    
    if st.button("💾 라이선스 키 등록", use_container_width=True):
        raw_key = license_key_input.strip()
        
        if raw_key:
            try:
                license_path = ROOT_DIR / "license.key"
                
                # 1. 기존 라이선스가 있다면 복구를 위해 백업
                backup_key = None
                if license_path.exists():
                    with open(license_path, "r", encoding="utf-8") as f:
                        backup_key = f.read()

                # 2. 새로 입력받은 키를 파일에 덮어쓰기
                with open(license_path, "w", encoding="utf-8") as f:
                    f.write(raw_key)

                # 3. 새로운 LicenseManager 인스턴스를 생성하여 유효성 테스트 시도
                temp_lm = LicenseManager()
                
                # 4. 검증 결과 확인 (정식 라이선스인지 확인)
                is_valid = temp_lm.current_license is not None and not getattr(temp_lm, 'is_trial', False)

                if is_valid:
                    # 검증 성공!
                    st.success("✅ 라이선스 키가 정상적으로 검증 및 등록되었습니다!\n\n**적용을 위해 프로그램을 완전히 종료한 후 다시 실행해 주세요.**")
                else:
                    # 검증 실패 -> 파일 롤백(원상복구)
                    if backup_key:
                        with open(license_path, "w", encoding="utf-8") as f:
                            f.write(backup_key) # 원래 키로 되돌림
                    else:
                        license_path.unlink()   # 원래 없었으면 파일 삭제
                        
                    st.error("❌ 유효하지 않은 라이선스 키이거나 다른 PC(HW ID 불일치)의 키입니다.\n\n키를 다시 확인해 주세요.")

            except Exception as e:
                st.error(f"검증 및 등록 중 오류가 발생했습니다: {e}")
        else:
            st.warning("⚠️ 라이선스 키를 입력해 주세요.")
    

def run_snowbot():
    """
    메인 실행 로직
    """
    # 1. 아이콘 파일 경로 (ROOT_DIR 기준) 및 아이콘 로드
    ICON_PATH = ROOT_DIR / "icon.ico"
    page_icon_img = "📈"

    if ICON_PATH.exists():
        try:
            page_icon_img = Image.open(ICON_PATH)
        except Exception as e:
            print(f"⚠️ 아이콘 로드 실패: {e}")
    else:
        print(f"⚠️ 아이콘 파일을 찾을 수 없음: {ICON_PATH}")

    # 2. [설정 적용] Streamlit의 페이지 설정은 반드시 최상단(다른 UI 렌더링 전)에 위치해야 합니다.
    st.set_page_config(
        page_title="SnowBot",
        page_icon=page_icon_img,
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # 3. 라이선스 매니저 초기화
    if 'license_manager' not in st.session_state:
        st.session_state['license_manager'] = LicenseManager()

    lm = st.session_state['license_manager']

    # 4. 앱 실행 시 공지사항 페이지 1회 팝업 호출 (광고 및 트래픽용)
    if "notice_opened" not in st.session_state:
        st.session_state["notice_opened"] = True
        
        # 서버에서 받아온 링크 설정값 (기본값 'Y')
        link_yn = str(getattr(lm, 'link_yn', 'Y')).strip().upper()
        
        raw_etc_link = getattr(lm, 'etc_link', '')
        etc_link = str(raw_etc_link).strip() if raw_etc_link else ""

        # link_yn이 'Y'일 때만 브라우저 열기 실행
        if link_yn == 'Y':
            my_hw_id = get_hardware_id()
            
            # etcLink 값이 존재하면 그 링크로, 없으면 기본 URL로 설정
            if etc_link:
                target_url = etc_link
            else:
                target_url = f"https://basedvalue.co.kr/notice?src=snowbot&uid={my_hw_id}"
                
            webbrowser.open(target_url)
    
    # 5. 버전 체크 알림 배너 노출
    if getattr(lm, 'update_required', False):
        st.warning(
            f"🚨 **새로운 버전({lm.server_version})이 출시되었습니다.** "
            f"안전한 자동매매와 최신 기능 사용을 위해 [공지사항](https://basedvalue.co.kr/notice)에서 버전 업데이트를 확인 해 주세요.",
            icon="⚠️"
        )
    
    # 커스텀 CSS 적용
    st.markdown("""
    <style>
        /* 1. 메인 콘텐츠 영역: 항상 꽉 차게 설정 */
        .block-container {
            padding-top: 3rem !important;
            padding-bottom: 1rem !important;
            max-width: 100% !important;
        }

        /* 2. 사이드바: '열려 있을 때만' 너비 고정 */
        /* aria-expanded="true" 조건을 반드시 넣어야 닫았을 때 빈 공간이 안 생깁니다. */
        section[data-testid="stSidebar"][aria-expanded="true"] {
            width: 330px !important;
            min-width: 330px !important;
            max-width: 330px !important;
        }

        /* 3. 사이드바가 닫혔을 때 (선택 사항: 확실하게 공간 제거) */
        section[data-testid="stSidebar"][aria-expanded="false"] {
            width: auto !important;
            min-width: auto !important;
        }

        /* 4. 기타 스타일 유지 */
        .main-header { font-size: 2rem; font-weight: bold; color: #1f77b4; margin-bottom: 1rem; }
        .sub-header { font-size: 1.5rem; font-weight: bold; color: #2c3e50; margin: 1rem 0; }
        .metric-card { background-color: #f8f9fa; border-radius: 10px; padding: 1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .status-running { color: #28a745; font-weight: bold; }
        .status-stopped { color: #dc3545; font-weight: bold; }
        .account-simulation { background-color: #d4edda; border: 2px solid #28a745; border-radius: 10px; padding: 1rem; margin-bottom: 1rem; }
        .account-mock { background-color: #fff3cd; border: 2px solid #ffc107; border-radius: 10px; padding: 1rem; margin-bottom: 1rem; }
        .account-real { background-color: #f8d7da; border: 2px solid #dc3545; border-radius: 10px; padding: 1rem; margin-bottom: 1rem; }
    </style>
    """, unsafe_allow_html=True)

    config = load_auth_config()
    if config is None:
        config = {'enabled': False}

    auth_enabled = config.get('enabled', True)

    if platform.system() == 'Linux':
        auth_enabled = True

    name = "snowbot"
    authenticator = None

    if auth_enabled:
        try:
            authenticator = stauth.Authenticate(
                config['credentials'],
                config['cookie']['name'],
                config['cookie']['key'],
                config['cookie']['expiry_days']
            )
            authenticator.login(location='main')

            if st.session_state["authentication_status"] is False:
                st.error('아이디 또는 비밀번호가 일치하지 않습니다.')
                return
            elif st.session_state["authentication_status"] is None:
                st.warning('아이디와 비밀번호를 입력해주세요.')
                return
            
            name = st.session_state["name"]
            
        except Exception as e:
            st.error(f"인증 모듈 오류: {e}")
            auth_enabled = False

    if not auth_enabled:
        st.session_state["authentication_status"] = True
        st.session_state["name"] = name
        st.session_state["username"] = "admin"

    # --- 메인 앱 로직 ---

    # 사이드바 CSS 제어 (라이선스 미보유/체험판)
    is_trial_mode = getattr(lm, 'is_trial', False)
    is_no_license = lm.current_license is None

    if is_trial_mode or is_no_license:
        st.markdown(
            """
            <style>
                [data-testid="stSidebar"] {
                    min-width: 330px !important;
                    max-width: 330px !important;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )

    allowed_markets = lm.get_allowed_markets()

    # ==========================================
    # [수정됨] yaml 대신 환경변수에서 버전을 가져옵니다.
    # ==========================================
    env_version = os.getenv('SNOWBOT_VERSION', '1.0.1')
    version_info = env_version if env_version.startswith('v') else f"v{env_version}"

    with st.sidebar:
        if ICON_PATH.exists():
            try:
                icon_b64 = get_img_as_base64(ICON_PATH)
                st.markdown(f"""
                    <h1 style='display: inline-block;'>
                        <img src="data:image/x-icon;base64,{icon_b64}" 
                            style="width: 35px; height: 35px; vertical-align: -10px; margin-right: 1px;">
                        SnowBot
                    </h1>
                    <span style="
                        background-color: #f0f2f6; 
                        color: #586069; 
                        font-size: 11px; 
                        font-weight: bold;
                        padding: 2px 8px; 
                        border-radius: 12px;
                        border: 1px solid #d1d5da;
                        margin-top: 5px;
                    ">{version_info}</span>
                """, unsafe_allow_html=True)
            except Exception:
                st.title(f"📈 SnowBot {version_info}")
        else:
            st.title(f"📈 SnowBot {version_info}")

        # 라이선스 상태 표시
        if lm.current_license:
            if getattr(lm, 'is_trial', False):
                st.warning(f"🧪 {lm.current_license.value} 체험판")
                st.caption(f"사용자: {lm.owner}")
                if lm.expiration_date:
                    days_left = (lm.expiration_date - datetime.now()).days
                    st.caption(f"만료일: {lm.expiration_date.strftime('%Y-%m-%d')}")
                    if days_left >= 0:
                        st.write(f"⏳ 체험 종료까지 **{days_left}일** 남았습니다.")
                st.markdown("---")
                st.markdown("#### 🔑 정식 라이선스 등록")
                with st.expander("라이선스 등록 안내", expanded=False):
                    render_license_guide()
            else:
                st.success(f"✅ {lm.current_license.value} License")
                st.caption(f"{lm.owner} 님 환영합니다.👋")
                if lm.expiration_date:
                    days_left = (lm.expiration_date - datetime.now()).days
                    st.caption(f"만료일: {lm.expiration_date.strftime('%Y-%m-%d')}")
                    if days_left >= 0:
                        st.write(f"⏳ 만료까지 **{days_left}일** 남았습니다.")
        else:
            st.error("⛔ 라이선스 미등록")
            st.warning("체험 기간이 종료되었습니다.")
            render_license_guide()
            st.stop() 
        
        if auth_enabled and authenticator:
            try:
                authenticator.logout(location='sidebar')
            except: pass
            
        st.markdown("---")

        st.markdown("### 🌐 Market")
        if len(allowed_markets) > 1:
            market_str = st.selectbox(
                "거래 시장 선택", 
                options=[m.value for m in allowed_markets],
                index=0,
                key="sb_market_selector",
                label_visibility="collapsed"
            )
            current_market = MarketType(market_str)
        elif len(allowed_markets) == 1:
            current_market = allowed_markets[0]
            st.info(f"✅ 시장: {current_market.value}")
        else:
            st.error("이용 가능한 시장 권한이 없습니다.")
            st.stop()
        
        st.session_state['current_market'] = current_market
        st.markdown("---")
    
    scheduler = init_scheduler()

    menu = st.sidebar.radio(
        "메뉴",
        ["📈 대시보드", "📥 데이터수집", "📊 종목평가", "🖐️ 수동매매", "⚡ 자동매매", "⭐ 관심종목", "⚙️ 설정"],
        label_visibility="collapsed"
    )
    
    st.sidebar.markdown("---")
    
    from config.settings import get_settings_manager
    settings = get_settings_manager().settings
    
    if current_market == MarketType.KR:
        current_mode = settings.execution_mode_kr
        api_account_mode = getattr(settings.api, 'kis_trading_account_mode_kr', 'mock')
    else:
        current_mode = settings.execution_mode_us
        api_account_mode = getattr(settings.api, 'kis_trading_account_mode_us', 'mock')
    
    if current_mode == "simulation":
        st.sidebar.success(f"🎮 {current_market.value} 시뮬레이션")
    elif api_account_mode == "mock":
        st.sidebar.warning(f"🧪 {current_market.value} 모의투자")
    else:
        st.sidebar.error(f"💰 {current_market.value} 실계좌")
    
    st.sidebar.caption(f"DB: {settings.database.db_type}")

    st.sidebar.markdown("---")
    st.sidebar.markdown("#### 🛠️ 지원")

    noti_url = "https://basedvalue.co.kr/notice"
    faq_url = "https://blog.naver.com/PostList.naver?blogId=snowbot&from=postList&categoryNo=15"
    qa_url = "https://basedvalue.co.kr/"
    st.sidebar.link_button("👉 공지사항", noti_url, type="secondary")
    st.sidebar.link_button("👉 FAQ 바로가기", faq_url, type="secondary")
    st.sidebar.link_button("👉 문의하러 가기", qa_url, type="secondary")

    if st.sidebar.button("📧 로그 파일 보내기", help="최근 5일간의 로그를 개발자에게 전송합니다.", type="secondary"):
        with st.sidebar:
            with st.spinner("로그 압축 및 전송 중..."):
                log_mgr = LogManager(ROOT_DIR)
                success, msg = log_mgr.send_to_discord(lm.owner)
                
                if success:
                    st.toast(msg)
                else:
                    st.toast(msg)
        
    
    if menu == "⚙️ 설정":
        from ui.settings_page import render_settings
        render_settings()
    elif menu == "📥 데이터수집":
        from ui.data_collection_page import render_data_collection
        render_data_collection()
    elif menu == "📊 종목평가":
        from ui.evaluation_page import render_evaluation
        render_evaluation()
    elif menu == "🖐️ 수동매매":
        from ui.manual_trading_page import render_manual_trading
        render_manual_trading()
    elif menu == "⚡ 자동매매":
        from ui.auto_trading_page import render_auto_trading
        render_auto_trading()
    elif menu == "📈 대시보드":
        from ui.dashboard import render_dashboard
        render_dashboard()
    elif menu == "⭐ 관심종목":
        from ui.favorite_page import render_favorite_page
        render_favorite_page()