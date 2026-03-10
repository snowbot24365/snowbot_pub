import utils.logger
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from scheduler.task_manager import SchedulerService
import platform
from core.definition import MarketType
from PIL import Image
import sys
from pathlib import Path
import base64
from datetime import datetime


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

    allowed_markets = [MarketType.KR, MarketType.US]

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
                """, unsafe_allow_html=True)
            except Exception:
                st.title("📈 SnowBot")
        else:
            st.title("📈 SnowBot")

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