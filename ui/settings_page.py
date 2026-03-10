"""
설정 페이지 (KR/US 분리 적용 - Main 연동 버전)
- API 키 설정 (현재 선택된 시장 기준)
- 계좌 설정 (시뮬레이션/실거래)
- 데이터베이스 설정
- 매매 설정 (현재 선택된 시장 기준)
- 스케줄 관리
"""

import streamlit as st
from datetime import datetime
import uuid
import time

from config.settings import get_settings_manager, ScheduleItem
from config.database import get_session, VirtualAccount, VirtualHolding
from core.definition import MarketType
from impl.kr.kr_fetcher import KrFetcher
from impl.us.us_fetcher import UsFetcher
from utils.token_manager import get_token_manager
from ui.schedule_page import render_schedule
from utils.common import custom_metric

def render_settings():
    """설정 페이지 렌더링"""
    st.markdown('<div class="main-header">⚙️ 설정</div>', unsafe_allow_html=True)
    
    settings_manager = get_settings_manager()
    settings = settings_manager.settings
    
    # 현재 선택된 시장 확인 (Main에서 전달됨)
    current_market = st.session_state.get('current_market', MarketType.KR)
    market_str = current_market.value # "KR" or "US"
    
    st.caption(f"현재 설정 대상 시장: **{market_str}** (시장 변경은 사이드바에서 가능합니다)")

    # 메인 탭 구성
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔑 API 키",
        "💳 계좌 설정",
        "🗄️ 데이터베이스",
        "💹 매매/평가 설정",
        "📅 스케줄 관리"
    ])
    
    # ========== 1. API 키 설정 ==========
    with tab1:
        render_api_settings(settings_manager, settings, current_market)
    
    # ========== 2. 계좌 설정 ==========
    with tab2:
        render_account_settings(settings_manager, settings, current_market)
    
    # ========== 3. 데이터베이스 설정 (공통) ==========
    with tab3:
        render_database_settings(settings_manager, settings)
    
    # ========== 4. 매매 설정 ==========
    with tab4:
        render_trading_settings(settings_manager, settings, current_market)
    
    # ========== 5. 스케줄 관리 (공통/시장별) ==========
    with tab5:
        render_schedule(current_market)


def render_api_settings(settings_manager, settings, current_market):
    """API 키 설정 탭"""
    st.subheader(f"API 키 설정 ({current_market.value})")
    
    # 1. 공통 설정 (OpenDart - 한국일 때만 의미가 있을 수 있으나 일단 공통 노출)
    if current_market == MarketType.KR:
        with st.expander("📊 OpenDart API (재무제표용)", expanded=False):
            opendart_key = st.text_input(
                "OpenDart API 키",
                value=settings.api.opendart_api_key,
                type="password",
                help="OpenDart에서 발급받은 API 키"
            )
            if st.button("저장 (OpenDart)", key="save_opendart"):
                settings_manager.update_api(opendart_api_key=opendart_key)
                st.success("저장되었습니다.")
        st.divider()

    # 2. 시장별 KIS API 설정
    market_code = current_market.value
    st.markdown(f"#### 🏦 {market_code} 투자증권(KIS) API 설정")
    
    api = settings.api
    
    # 변수 매핑
    if current_market == MarketType.KR:
        mock_key = api.kis_mock_app_key_kr
        mock_secret = api.kis_mock_app_secret_kr
        real_key = api.kis_real_app_key_kr
        real_secret = api.kis_real_app_secret_kr
        api_mode = api.kis_api_mode_kr
    else:
        mock_key = api.kis_mock_app_key_us
        mock_secret = api.kis_mock_app_secret_us
        real_key = api.kis_real_app_key_us
        real_secret = api.kis_real_app_secret_us
        api_mode = api.kis_api_mode_us

    # --- 모의투자 입력 ---
    col1, col2 = st.columns(2)
    with col1:
        new_mock_key = st.text_input(f"App Key (모의 - {market_code})", value=mock_key, type="password", key=f"mock_k_{market_code}")
    with col2:
        new_mock_secret = st.text_input(f"App Secret (모의 - {market_code})", value=mock_secret, type="password", key=f"mock_s_{market_code}")

    # --- 실전투자 입력 ---
    col1, col2 = st.columns(2)
    with col1:
        new_real_key = st.text_input(f"App Key (실전 - {market_code})", value=real_key, type="password", key=f"real_k_{market_code}")
    with col2:
        new_real_secret = st.text_input(f"App Secret (실전 - {market_code})", value=real_secret, type="password", key=f"real_s_{market_code}")

    st.divider()
    
    # --- 모드 설정 ---
    st.markdown(f"##### 📡 {market_code} 데이터 수집 모드")
    new_api_mode = st.radio(
        f"[{market_code}] 데이터 수집에 사용할 API",
        options=["mock", "real"],
        format_func=lambda x: "🧪 모의투자 API" if x == "mock" else "💰 실전투자 API",
        index=0 if api_mode == "mock" else 1,
        horizontal=True,
        key=f"mode_{market_code}"
    )
    
    # --- 토큰 상태 표시 (Utils TokenManager 사용) ---
    try:
        st.markdown("##### 🔐 토큰 상태")
        token_mgr = get_token_manager()
        
        # market_code는 함수 상단에서 정의됨 (current_market.value -> "KR" or "US")
        
        c1, c2 = st.columns(2)
        
        # 1. 모의투자 토큰 상태
        with c1:
            # 시장(KR/US)과 모드('mock')를 명시하여 상태 조회
            status = token_mgr.get_token_status(market_code, 'mock')
            
            is_valid = status.get('is_valid', False)
            issue_cnt = status.get('issue_count_today', 0)
            
            if is_valid:
                # 남은 시간을 보기 좋게 포맷팅 (예: 23:59:59)
                rem_time = status.get('remaining_time')
                rem_str = str(rem_time).split('.')[0] if rem_time else "-"
                st.success(f"🧪 모의: **유효** (남은 시간: {rem_str})")
            else:
                st.warning("🧪 모의: 토큰 없음 / 만료")
            
            st.caption(f"금일 발급 횟수: {issue_cnt}회")

        # 2. 실전투자 토큰 상태
        with c2:
            # 시장(KR/US)과 모드('real')를 명시하여 상태 조회
            status = token_mgr.get_token_status(market_code, 'real')
            
            is_valid = status.get('is_valid', False)
            issue_cnt = status.get('issue_count_today', 0)
            
            if is_valid:
                rem_time = status.get('remaining_time')
                rem_str = str(rem_time).split('.')[0] if rem_time else "-"
                st.success(f"💰 실전: **유효** (남은 시간: {rem_str})")
            else:
                st.warning("💰 실전: 토큰 없음 / 만료")
                
            st.caption(f"금일 발급 횟수: {issue_cnt}회")
                
    except Exception as e:
        # get_token_status 메서드가 없거나 오류 발생 시
        st.error(f"토큰 상태 조회 오류: {e}")

    st.divider()

    # --- 저장 버튼 ---
    if st.button(f"💾 {market_code} API 설정 저장", type="primary", key=f"save_api_{market_code}"):
        if current_market == MarketType.KR:
            settings_manager.update_api(
                kis_mock_app_key_kr=new_mock_key, kis_mock_app_secret_kr=new_mock_secret,
                kis_real_app_key_kr=new_real_key, kis_real_app_secret_kr=new_real_secret,
                kis_api_mode_kr=new_api_mode
            )
        else:
            settings_manager.update_api(
                kis_mock_app_key_us=new_mock_key, kis_mock_app_secret_us=new_mock_secret,
                kis_real_app_key_us=new_real_key, kis_real_app_secret_us=new_real_secret,
                kis_api_mode_us=new_api_mode
            )
        st.toast(f"✅ {market_code} API 설정이 저장되었습니다.")

@st.dialog("⚠️ 실행 모드 변경 확인")
def confirm_mode_change(settings_manager, market_code, new_mode):
    st.warning(f"**{market_code}** 시장의 실행 모드를 변경하시겠습니까?")
    
    msg = ""
    if new_mode == "real_trading":
        msg = """
        💰 **증권사 API 연동 (모의/실전)** 모드로 변경합니다.
        
        설정된 API 정보를 사용하여 시세 조회 및 주문을 수행합니다.
        (실계좌가 연결된 경우 실제 자금이 사용될 수 있습니다)
        """
    else:
        msg = """
        🎮 **시뮬레이션** 모드로 변경합니다.
        
        내부 가상 계좌를 사용하여 매매를 시뮬레이션합니다.
        실제 주문은 전송되지 않습니다.
        """
    
    st.info(msg)
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("취소", width="stretch"):
            st.rerun()
    with col2:
        if st.button("확인 (변경)", type="primary", width="stretch"):
            settings_manager.update_execution_mode(market_code, new_mode)
            st.success("실행 모드가 변경되었습니다.")
            time.sleep(0.5)
            st.rerun()

def render_account_settings(settings_manager, settings, current_market):
    """계좌 설정 탭"""
    market_code = current_market.value
    st.subheader(f"계좌 설정 ({market_code})")
    
    st.markdown(f"#### 🎮 {market_code} 실행 모드")

    st.info(f"💡 **{market_code} 시장**의 실행 모드와 계좌를 설정합니다.")
    st.info("""
    **실행 모드 안내:**
    - **시뮬레이션**: 시스템 내부 가상 계좌로 매매를 테스트합니다. 실제 거래 없이 전략을 검증할 수 있습니다.
    - **실거래**: 증권사 API를 통해 실제 매매를 수행합니다.
    """)
    
    api = settings.api
    
    # 현재 시장에 맞는 실행 모드 및 API 설정 로드
    if current_market == MarketType.KR:
        current_mode = settings.execution_mode_kr
        trade_acct_mode = api.kis_trading_account_mode_kr
        
        mock_acct = api.kis_mock_account_no_kr
        mock_code = api.kis_mock_account_cd_kr
        real_acct = api.kis_real_account_no_kr
        real_code = api.kis_real_account_cd_kr
    else:
        current_mode = settings.execution_mode_us
        trade_acct_mode = api.kis_trading_account_mode_us
        
        mock_acct = api.kis_mock_account_no_us
        mock_code = api.kis_mock_account_cd_us
        real_acct = api.kis_real_account_no_us
        real_code = api.kis_real_account_cd_us

    # =========================================================
    # 1. 실행 모드 선택 (Simulation vs Real Trading)
    # =========================================================
        
    mode_opts = {
        "simulation": "🎮 시뮬레이션 (가상매매)",
        "real_trading": "💰 증권사 API 연동 (모의/실전)"
    }
    
    new_mode = st.radio(
        f"[{market_code}] 실행 모드 선택",
        options=list(mode_opts.keys()),
        format_func=lambda x: mode_opts[x],
        index=0 if current_mode == "simulation" else 1,
        key=f"mode_sel_{market_code}"
    )
    
    # 실행 모드 저장 (변경 시 즉시 반영)
    if new_mode != current_mode:
        confirm_mode_change(settings_manager, market_code, new_mode)

    st.divider()

    # =========================================================
    # 2. 증권사 API 계좌 설정 (실거래 모드일 때만 활성화해도 됨)
    # =========================================================
    if new_mode == "real_trading":
        render_real_trading_settings(settings_manager, settings, current_market)
            
    else:
        # 시뮬레이션 모드일 때
        st.info("시뮬레이션 모드에서는 증권사 API 계좌 정보가 필요하지 않습니다. 아래에서 가상 자금을 설정하세요.")

        # =========================================================
        # 3. 시뮬레이션 자금 설정 (항상 표시하거나 시뮬 모드일 때만 표시)
        # =========================================================
        st.divider()
        with st.expander("🎮 시뮬레이션 초기 자금 설정 (가상 계좌)", expanded=(new_mode == "simulation")):
            render_simulation_settings(settings_manager, settings, current_market)

def render_real_trading_settings(settings_manager, settings, current_market):
    """실거래 모드 설정 (시장별 분리 적용)"""
    market_str = current_market.value
    st.markdown(f"#### 💰 {market_str} 증권사 API 연동 설정")
    
    api = settings.api
    
    # 1. 시장별 API 키 유무 확인 및 변수 로드
    if current_market.value == "KR":
        has_mock_api = bool(api.kis_mock_app_key_kr and api.kis_mock_app_secret_kr)
        has_real_api = bool(api.kis_real_app_key_kr and api.kis_real_app_secret_kr)
        
        current_data_api = api.kis_api_mode_kr
        current_trading_mode = api.kis_trading_account_mode_kr
        
        mock_acct_val = api.kis_mock_account_no_kr
        mock_cd_val = api.kis_mock_account_cd_kr
        real_acct_val = api.kis_real_account_no_kr
        real_cd_val = api.kis_real_account_cd_kr
        real_confirmed = api.kis_real_confirmed_kr
        
    else: # US
        has_mock_api = bool(api.kis_mock_app_key_us and api.kis_mock_app_secret_us)
        has_real_api = bool(api.kis_real_app_key_us and api.kis_real_app_secret_us)
        
        current_data_api = api.kis_api_mode_us
        current_trading_mode = api.kis_trading_account_mode_us
        
        mock_acct_val = api.kis_mock_account_no_us
        mock_cd_val = api.kis_mock_account_cd_us
        real_acct_val = api.kis_real_account_no_us
        real_cd_val = api.kis_real_account_cd_us
        # 미국장 전용 확인 플래그가 settings에 없다면 KR과 공유하거나 새로 추가 필요
        # 여기서는 기존 kis_real_confirmed 사용 (또는 US용 추가)
        real_confirmed = api.kis_real_confirmed_us

    # 2. API 설정 경고
    if not has_mock_api and not has_real_api:
        st.error(f"❌ {market_str} 증권사 API가 설정되지 않았습니다. 'API 키' 탭에서 먼저 API 정보를 설정해주세요.")
        return
    
    st.info(f"""
    💡 **{market_str} 거래 계좌 안내**
    - 거래에 사용할 계좌를 선택합니다.
    - 데이터 수집용 API는 'API 키' 탭에서 별도로 설정됩니다.
    - 예: 실전투자 API로 데이터를 수집하고, 모의계좌로 거래 테스트 가능
    """)
    
    # 3. API 상태 표시
    st.markdown("###### 📡 API 설정 상태")
    col1, col2 = st.columns(2)
    with col1:
        if has_mock_api:
            st.success("✅ 모의투자 API 설정됨")
        else:
            st.warning("⚠️ 모의투자 API 미설정")
    with col2:
        if has_real_api:
            st.success("✅ 실전투자 API 설정됨")
        else:
            st.warning("⚠️ 실전투자 API 미설정")
    
    st.caption(f"📊 현재 데이터 수집 API: {'실전투자' if current_data_api == 'real' else '모의투자'} (API 키 탭에서 변경)")
    
    st.divider()
    
    # 4. 거래 계좌 선택
    st.markdown("#### 📋 거래 계좌 선택")
    
    account_mode = st.radio(
        f"[{market_str}] 거래에 사용할 계좌",
        options=["mock", "real"],
        format_func=lambda x: "🧪 모의계좌 (모의투자)" if x == "mock" else "💳 실계좌 (실전투자)",
        index=0 if current_trading_mode == "mock" else 1,
        horizontal=True,
        key=f"account_mode_radio_{market_str}"
    )
    
    st.divider()
    
    # 5. 계좌 번호 설정 (모의/실전 분기)
    if account_mode == "mock":
        st.markdown("##### 🧪 모의계좌 설정")
        
        if not has_mock_api:
            st.error("❌ 모의투자 API가 설정되지 않았습니다. 'API 키' 탭에서 먼저 설정해주세요.")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            mock_account_no = st.text_input(
                "모의계좌 번호 (8자리)",
                value=mock_acct_val,
                max_chars=8,
                key=f"mock_acct_no_{market_str}"
            )
        
        with col2:
            mock_account_cd = st.text_input(
                "계좌상품코드 (2자리)",
                value=mock_cd_val,
                max_chars=2,
                key=f"mock_acct_cd_{market_str}"
            )
        
        st.info("💡 모의투자 계좌로 실제 자금 없이 거래를 테스트할 수 있습니다.")
        
        if st.button("💾 모의계좌 설정 저장", type="primary", key=f"save_mock_{market_str}"):
            # 시장별 설정 업데이트 메서드 호출 (settings_manager에 구현 필요)
            # 여기서는 kwargs로 직접 전달
            updates = {}
            if current_market.value == "KR":
                updates = {
                    "execution_mode_kr": "real_trading", # 실행 모드 변경
                    "kis_trading_account_mode_kr": "mock",
                    "kis_mock_account_no_kr": mock_account_no,
                    "kis_mock_account_cd_kr": mock_account_cd
                }
            else:
                updates = {
                    "execution_mode_us": "real_trading",
                    "kis_trading_account_mode_us": "mock",
                    "kis_mock_account_no_us": mock_account_no,
                    "kis_mock_account_cd_us": mock_account_cd
                }
            
            # API 설정 업데이트 (실행모드는 settings 레벨, 계좌는 api 레벨)
            # settings_manager 구조에 따라 분리 호출
            if current_market.value == "KR":
                settings_manager.update_execution_mode("KR", "real_trading")
                settings_manager.update_api(**updates) # update_api는 내부적으로 매칭되는 키만 업데이트함
            else:
                settings_manager.update_execution_mode("US", "real_trading")
                settings_manager.update_api(**updates)

            st.toast(f"✅ {market_str} 모의계좌 설정이 저장되었습니다.")
    
    else:  # real
        st.markdown("##### 💳 실계좌 설정")
        
        if not has_real_api:
            st.error(f"""
            ❌ **{market_str} 실전투자 API가 설정되지 않았습니다.**
            
            실계좌로 거래하려면 실전투자 API가 필요합니다.
            'API 키' 탭에서 실전투자 App Key와 App Secret을 먼저 설정해주세요.
            """)
            return
        
        st.error("""
        🚨 **실계좌 사용 주의사항**
        
        실계좌를 선택하면 **실제 자금**으로 거래가 실행됩니다.
        자동매매 프로그램 사용으로 인한 투자 손실에 대해 본인이 전적으로 책임집니다.
        """)

        key_real_no = f"r_no_{market_str}"
        key_real_cd = f"r_cd_{market_str}"
        key_agree = f"agree_risk_{market_str}"
        
        if key_real_no not in st.session_state:
            st.session_state[key_real_no] = real_acct_val
        if key_real_cd not in st.session_state:
            st.session_state[key_real_cd] = real_cd_val

        col1, col2 = st.columns(2)
        
        with col1:
            real_account_no = st.text_input(
                "실계좌 번호 (8자리)",
                max_chars=8,
                key=key_real_no
            )
        
        with col2:
            real_account_cd = st.text_input(
                "계좌상품코드 (2자리)",
                max_chars=2,
                key=key_real_cd
            )
        
        st.divider()
        
        # 체크박스 활성화 조건: 계좌번호와 코드가 모두 입력되어야 함
        is_input_valid = bool(real_account_no and real_account_cd)

        def on_agree_change():
            # 체크가 해제되었을 때 (False)
            if not st.session_state.get(key_agree):
                # 입력 필드 세션 값을 강제로 초기화 (화면에서 지워짐)
                st.session_state[key_real_no] = ""

        agree_risk = st.checkbox(
            "⚠️ 위 주의사항을 모두 읽었으며, 실계좌 사용으로 인한 투자 손실에 대해 본인이 책임집니다.",
            value=real_confirmed, # 저장된 값으로 초기화
            disabled=not is_input_valid, # 입력 없으면 체크 불가
            key=key_agree,
            on_change=on_agree_change
        )
        
        if st.button("💾 실계좌 설정 저장", type="primary", key=f"save_real_{market_str}"):
            updates = {}
            market_key = market_str.lower()
            
            if agree_risk:
                # [Case A] 체크된 상태로 저장 -> 정보 업데이트
                updates = {
                    f"kis_trading_account_mode_{market_key}": "real",
                    f"kis_real_account_no_{market_key}": real_account_no,
                    f"kis_real_account_cd_{market_key}": real_account_cd,
                    f"kis_real_confirmed_{market_key}": True
                }
                settings_manager.update_api(**updates)
                st.success(f"✅ {market_str} 실계좌 설정이 저장되었습니다.")
                st.toast("실계좌 모드가 적용되었습니다.", icon="💳")
            else:
                # [Case B] 체크 해제 후 저장 -> 정보 삭제 (요청사항 반영)
                updates = {
                    f"kis_trading_account_mode_{market_key}": "real", # 모드는 유지하되 정보만 날림 (또는 mock으로 돌릴 수도 있음)
                    f"kis_real_account_no_{market_key}": "", # 번호 삭제
                    f"kis_real_confirmed_{market_key}": False # 확인 해제
                }
                settings_manager.update_api(**updates)
                st.toast("동의가 해제되어 실계좌 정보가 삭제되었습니다.", icon="🗑️")

def render_simulation_settings(settings_manager, settings, current_market):
    """시뮬레이션 자금 설정"""
    market_code = current_market.value
    
    # 현재 설정 로드
    target_settings = settings.trading.kr if current_market == MarketType.KR else settings.trading.us
    current_balance = target_settings.initial_balance
    currency = "원" if current_market == MarketType.KR else "달러($)"

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"##### {market_code} 가상 계좌")
    
        # 현재 상태 조회
        va = get_virtual_account(current_market)
        if va:
            st.info(f"현재 잔고: {va.get('balance', 0):,.0f} {currency}")
        
        new_balance = st.number_input(
            f"초기 자본금 ({currency})", 
            value=current_balance, 
            step=1000000 if current_market == MarketType.KR else 1000
        )
        st.caption(f"💵 설정 금액: {new_balance:,.0f} {currency}")
    with col2:
        st.markdown("##### 📊 현재 가상 계좌 현황")
        if va:
            custom_metric("예수금", f"{va.get('deposit', 0):,.0f}{currency}")
            custom_metric("총 평가금액", f"{va.get('total_eval', 0):,.0f}{currency}")
            custom_metric("총 손익", f"{va.get('total_profit', 0):,.0f}{currency}")
        else:
            st.info("가상 계좌가 초기화되지 않았습니다.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button(f"설정 저장 ({market_code})", key=f"sim_save_{market_code}"):
            settings_manager.update_trading(market=market_code, initial_balance=new_balance)
            st.toast("저장되었습니다.")
                        
    with col2:
        if st.button(f"계좌 초기화 ({market_code})", type="secondary", key=f"sim_reset_{market_code}"):
            reset_virtual_account(current_market, new_balance)
            st.toast(f"{market_code} 가상 계좌가 {new_balance:,} {currency}로 초기화되었습니다.")


def render_database_settings(settings_manager, settings):
    """데이터베이스 설정 탭"""
    st.subheader("데이터베이스 설정")
    
    st.markdown("#### 데이터베이스 연결")
    
    db_type = st.selectbox(
        "DB 유형",
        options=["sqlite", "oracle"],
        format_func=lambda x: "SQLite (로컬)" if x == "sqlite" else "Oracle (ATP)",
        index=0 if settings.database.db_type == "sqlite" else 1
    )
    
    if db_type == "sqlite":
        st.info("📁 로컬 SQLite 데이터베이스를 사용합니다.")
        db_path = st.text_input(
            "DB 파일 경로",
            value=settings.database.sqlite_path,
            help="SQLite 데이터베이스 파일 경로"
        )
        if st.button("💾 데이터베이스 설정 저장", key="save_db"):
            settings_manager.update_database(
                db_type="sqlite",
                sqlite_path=db_path
            )
            st.success("✅ 데이터베이스 설정이 저장되었습니다.")

    else:
        st.markdown("#### Oracle ATP 설정 (Cloud)")
        col1, col2 = st.columns(2)
        with col1:
            oracle_user = st.text_input("사용자명 (User)", value=settings.database.oracle_user)
            oracle_dsn = st.text_input("DSN (별칭)", value=settings.database.oracle_dsn)
        with col2:
            oracle_password = st.text_input("비밀번호", value=settings.database.oracle_password, type="password")
            oracle_wallet_path = st.text_input("지갑 경로", value=settings.database.oracle_wallet_path)

        if st.button("저장 (Oracle)", key="save_oracle"):
            settings_manager.update_database(
                db_type="oracle",
                oracle_user=oracle_user,
                oracle_password=oracle_password,
                oracle_dsn=oracle_dsn,
                oracle_wallet_path=oracle_wallet_path
            )
            st.success("Oracle 설정이 저장되었습니다.")
    st.warning("⚠️ 설정 변경 후 반드시 프로그램 재시작 해야 합니다.")    
    st.divider()
    
    # DB 초기화 버튼들
    with st.expander("⚠️ 데이터 관리 (초기화)", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ 전체 데이터 삭제"):
                delete_data("all")
                st.success("삭제 완료")
        with col2:
            st.warning("주의: 모든 데이터가 영구 삭제됩니다.")


def render_trading_settings(settings_manager, settings, current_market):
    """매매/평가 설정 탭"""
    market_code = current_market.value
    if current_market == MarketType.KR:
        current_mode = settings.execution_mode_kr
    else:
        current_mode = settings.execution_mode_us
        
    st.subheader(f"매매 및 종목 평가 설정 ({market_code})")
    
    # 해당 시장의 설정 객체 가져오기
    t_settings = settings.trading.kr if current_market == MarketType.KR else settings.trading.us
    e_settings = settings.evaluation.kr if current_market == MarketType.KR else settings.evaluation.us
    
    currency = "원" if current_market == MarketType.KR else "달러($)"
    
    # 1. 평가 기준 설정
    st.markdown(f"#### 📊 {market_code} 평가 기준 (Evaluation)")
    
    col1, col2 = st.columns(2)
    with col1:
        min_score = st.slider(
            "최소 매수 점수 (40점 만점)",
            0,
            40,
            e_settings.min_total_score,
            key=f"min_sc_{market_code}",
            help="종합 점수(가중치 포함)가 이 점수 이상인 종목만 매수 후보로 선정됩니다. (30점이상 권장)"
            )
    with col2:
        st.info(f"💡 현재 설정: 종합 점수 **{min_score}점** 이상인 종목을 매수합니다.")

    # 가중치 설정 (Expander)
    with st.expander("⚖️ 지표별 가중치 설정", expanded=False):
        st.caption("각 항목의 중요도를 설정합니다. 1.0보다 크면 총점에 더 큰 영향을 줍니다. (최대 3.0)")
        c1, c2, c3, c4 = st.columns(4)
        w_sheet = c1.number_input("재무", 0.0, 3.0, e_settings.weight_sheet, 0.5, key=f"w_s_{market_code}")
        w_trend = c2.number_input("모멘텀", 0.0, 3.0, e_settings.weight_trend, 0.5, key=f"w_t_{market_code}")
        w_price = c3.number_input("주가", 0.0, 3.0, e_settings.weight_price, 0.5, key=f"w_p_{market_code}")
        w_kpi = c4.number_input("KPI", 0.0, 3.0, e_settings.weight_kpi, 0.5, key=f"w_k_{market_code}")
        
        c1, c2, c3, c4 = st.columns(4)
        w_buy = c1.number_input("수급", 0.0, 3.0, e_settings.weight_buy, 0.5, key=f"w_b_{market_code}")
        w_avls = c2.number_input("시총", 0.0, 3.0, e_settings.weight_avls, 0.5, key=f"w_a_{market_code}")
        w_per = c3.number_input("PER", 0.0, 3.0, e_settings.weight_per, 0.5, key=f"w_pe_{market_code}")
        w_pbr = c4.number_input("PBR", 0.0, 3.0, e_settings.weight_pbr, 0.5, key=f"w_pb_{market_code}")

    # 상세 평가 기준 설정 (Expander)
    with st.expander("🛠️ 상세 평가 기준 설정", expanded=False):
        
        # 시장별 설정 객체 로드
        if current_market == MarketType.KR:
            eval_config = settings.evaluation.kr
            currency_unit = "억원"
        else:
            eval_config = settings.evaluation.us
            currency_unit = "백만달러"

        st.markdown(f"##### 1️⃣ {current_market.value} 재무 평가 기준")
        f1, f2, f3, f4 = st.columns(4)
        
        with f1: 
            th_grs = st.number_input(
                "매출액증가율(%) >", 
                value=eval_config.threshold_grs, 
                step=5.0, 
                help="전년 동기 대비 매출 성장률. 이 값보다 커야 점수 획득.",
                key=f"th_grs_{current_market.value}"
            )
        with f2: 
            th_prof = st.number_input(
                "영업이익증가율(%) >", 
                value=eval_config.threshold_bsop_prfi_inrt, 
                step=5.0, 
                help="전년 동기 대비 영업이익 성장률. 이 값보다 커야 점수 획득.",
                key=f"th_prof_{current_market.value}"
            )
        with f3: 
            th_rsrv = st.number_input(
                "유보율(%) >", 
                value=eval_config.threshold_rsrv_rate, 
                step=50.0, 
                help="기업의 현금 보유 능력. 높을수록 안전. 이 값보다 커야 점수 획득.",
                key=f"th_rsrv_{current_market.value}"
            )
        with f4: 
            th_lblt = st.number_input(
                "부채비율(%) <", 
                value=eval_config.threshold_lblt_rate, 
                step=50.0, 
                help="자본 대비 부채 비율. 이 값보다 작아야(빚이 적어야) 점수 획득.",
                key=f"th_lblt_{current_market.value}"
            )
        
        st.divider()
        st.markdown("##### 2️⃣ 추세 전략 (Trend Strategy)")
        trend_opts = ["REGULAR", "REVERSE"]
        trend_idx = 0 if eval_config.trend_alignment == "REGULAR" else 1
        trend_align = st.radio(
            "추세선 정렬 기준", options=trend_opts, index=trend_idx, horizontal=True,
            format_func=lambda x: "📈 정배열 (추세 추종)" if x == "REGULAR" else "📉 역배열 (바닥 반등 기대)",
            help="정배열: 5>20>60일선 (상승세 탑승) / 역배열: 60>20>5일선 (저점 매수)",
            key=f"trend_align_{current_market.value}"
        )
        
        st.divider()
        st.markdown("##### 3️⃣ 점수 산정 상세 기준 (Benchmark & Step)")
        st.info("""
        **점수 계산 방식**: (설정값에 따른 점수 예시를 확인하며 조절하세요.)
        - **만점 기준(Benchmark)**: 이 조건보다 좋으면 **5점 만점**을 받습니다.
        - **차감 간격(Step)**: 기준에서 이 간격만큼 멀어질 때마다 **1점씩 차감**됩니다.
        """)
        
        sc1, sc2 = st.columns(2)
        
        # --- [좌측 컬럼] PER & 고가 괴리율 ---
        with sc1:
            st.markdown("**PER 평가 (저평가)**")
            per_bench = st.number_input(
                "PER 만점 기준 (<)", 
                value=eval_config.per_benchmark, 
                step=1.0, 
                help="PER가 이 값보다 낮으면 5점 (예: 5.0)",
                key=f"per_bench_{current_market.value}"
            )
            per_step = st.number_input(
                "PER 차감 간격", 
                value=eval_config.per_step, 
                step=5.0, 
                help="점수가 1점씩 깎이는 PER 구간 (예: 5.0)",
                key=f"per_step_{current_market.value}"
            )
            
            # [예시 출력]
            per_ex_5 = per_bench
            per_ex_4 = per_bench + per_step
            per_ex_3 = per_bench + (per_step * 2)
            st.caption(f"📝 **예시**: {per_ex_5:.1f} 미만(5점), {per_ex_4:.1f} 미만(4점), {per_ex_3:.1f} 미만(3점)...")
            
            st.markdown("---") # 구분선
            
            st.markdown("**고가 괴리율 (낙폭과대)**")
            high_bench = st.number_input(
                "낙폭 만점 기준 (<)", 
                value=eval_config.high_rate_benchmark, 
                step=5.0, 
                help="고점 대비 하락률. 예: -30(%)보다 더 떨어지면 5점",
                key=f"high_bench_{current_market.value}"
            )
            high_step = st.number_input(
                "낙폭 차감 간격", 
                value=eval_config.high_rate_step, 
                step=5.0, 
                help="점수가 1점씩 깎이는 하락률 구간",
                key=f"high_step_{current_market.value}"
            )
            
            # [예시 출력]
            high_ex_5 = high_bench
            high_ex_4 = high_bench + high_step
            high_ex_3 = high_bench + (high_step * 2)
            st.caption(f"📝 **예시**: {high_ex_5:.1f}% 미만(5점), {high_ex_4:.1f}% 미만(4점), {high_ex_3:.1f}% 미만(3점)...")
            
        # --- [우측 컬럼] PBR & 저가 괴리율 ---
        with sc2:
            st.markdown("**PBR 평가 (자산가치)**")
            pbr_bench = st.number_input(
                "PBR 만점 기준 (<)", 
                value=eval_config.pbr_benchmark, 
                step=1.0, 
                help="PBR이 이 값보다 낮으면 5점 (예: 1.0)",
                key=f"pbr_bench_{current_market.value}"
            )
            pbr_step = st.number_input(
                "PBR 차감 간격", 
                value=eval_config.pbr_step, 
                step=1.0, 
                help="점수가 1점씩 깎이는 PBR 구간",
                key=f"pbr_step_{current_market.value}"
            )
            
            # [예시 출력]
            pbr_ex_5 = pbr_bench
            pbr_ex_4 = pbr_bench + pbr_step
            pbr_ex_3 = pbr_bench + (pbr_step * 2)
            st.caption(f"📝 **예시**: {pbr_ex_5:.1f} 미만(5점), {pbr_ex_4:.1f} 미만(4점), {pbr_ex_3:.1f} 미만(3점)...")
            
            st.markdown("---") # 구분선
            
            st.markdown("**저가 괴리율 (급등부담)**")
            low_bench = st.number_input(
                "급등 감점 기준 (>)", 
                value=eval_config.low_rate_benchmark, 
                step=5.0, 
                help="저점 대비 상승률. 예: 30(%)보다 더 오르면 최대 감점",
                key=f"low_bench_{current_market.value}"
            )
            low_step = st.number_input(
                "급등 차감 간격", 
                value=eval_config.low_rate_step, 
                step=5.0, 
                help="감점 폭이 줄어드는 상승률 구간",
                key=f"low_step_{current_market.value}"
            )
            
            # [예시 출력]
            low_ex_5 = low_bench
            low_ex_4 = low_bench - low_step
            low_ex_3 = low_bench - (low_step * 2)
            st.caption(f"📝 **예시**: {low_ex_5:.1f}% 초과(5점감점), {low_ex_4:.1f}% 초과(4점감점)...")
            
        st.markdown("---") # 구분선
        
        # 시가총액 단위 동적 적용
        st.markdown(f"**시가총액 (AVLS) - 단위: {currency_unit}**")
        av1, av2 = st.columns(2)
        with av1: 
            avls_bench = st.number_input(
                f"시총 최소 기준 ({currency_unit})", 
                value=eval_config.avls_benchmark, 
                step=100.0, 
                help=f"이 금액 미만이면 최하점(1점). 예: 100{currency_unit}",
                key=f"avls_bench_{current_market.value}"
            )
        with av2: 
            avls_step = st.number_input(
                f"시총 증가 간격 ({currency_unit})", 
                value=eval_config.avls_step, 
                step=500.0, 
                help=f"점수가 1점씩 올라가는 시총 단위. 예: 1000{currency_unit}",
                key=f"avls_step_{current_market.value}"
            )
        
        # [예시 출력]
        av_ex_1 = avls_bench
        av_ex_2 = avls_bench + avls_step
        av_ex_3 = avls_bench + (avls_step * 2)
        st.caption(f"📝 **예시**: {av_ex_1:,.0f}{currency_unit} 미만(1점), {av_ex_2:,.0f}{currency_unit} 미만(2점), {av_ex_3:,.0f}{currency_unit} 미만(3점)...")

    st.divider()

    # 2. 안전망 설정
    st.markdown("#### 🛡️ 추가 안전망 설정")
    st.caption("선택한 항목에 대해서만 필터링을 수행합니다. (체크 해제 시 해당 조건 무시)")
    
    c1, c2, c3 = st.columns(3)
    with c1:
        use_srim = st.checkbox("SRIM 저평가 필터 적용", value=t_settings.use_srim_filter, key=f"f_srim_{market_code}", help="현재가가 SRIM 적정주가보다 낮은 종목만 선정")
        use_roe = st.checkbox("ROE 3년 평균 8% 이상", value=t_settings.use_roe_filter, key=f"f_roe_{market_code}")
    with c2:
        use_dividend = st.checkbox("배당 지급 기업 필터", value=t_settings.use_dividend_filter, key=f"f_dividend_{market_code}", help="배당수익률이 0보다 큰 기업만 선정")
        use_cashflow = st.checkbox("현금흐름 양호 필터", value=t_settings.use_cashflow_filter, key=f"f_cashflow_{market_code}", help="FCF(잉여현금흐름)가 0보다 큰 기업만 선정")
    with c3:
        use_activity = st.checkbox("활동성(자산회전율) 필터", value=t_settings.use_activity_filter, key=f"f_ctivity_{market_code}", help="총자산회전율이 0.3 이상인 기업만 선정")
    
    st.divider()

    # 3. 매매 설정
    st.markdown(f"#### 💹 {market_code} 매매 설정 (Trading)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**매수 설정**")
        buy_rate = st.slider("1회 매수 비중 (총 투자금 대비 %)", 1.0, 100.0, t_settings.buy_rate, 0.5, key=f"b_rate_{market_code}", help="총 투자금에서 1회 매수 시 사용할 비율")
        max_buy_amt = st.number_input(f"종목당 최대 매수 금액 ({currency})", 0, 100000000, t_settings.max_buy_amount, step=10_000, key=f"b_max_{market_code}", help="한 종목에 투자할 수 있는 최대 금액")
        limit_cnt = st.number_input("최대 보유 종목 수", 1, 50, t_settings.limit_count, key=f"l_cnt_{market_code}")
        buy_criteria_opts = {
            "current": "현재가 (시장가)",
            "pvt": "Pivot Point (피벗 포인트)",
            "pvt_sup1": "Support 1 (1차 지지선)",
            "pvt_sup2": "Support 2 (2차 지지선)",
            "pvt_avg": "Pivot avg (피벗 평균)",
        }
        # 저장된 값이 옵션에 없으면 기본값 'pvt' 사용
        current_criteria = t_settings.buy_price_criteria if hasattr(t_settings, 'buy_price_criteria') else 'pvt'
        
        buy_price_criteria = st.selectbox(
            "매수 가격 기준",
            options=list(buy_criteria_opts.keys()),
            format_func=lambda x: f"✅ {buy_criteria_opts[x]}" if x == current_criteria else f"　 {buy_criteria_opts[x]}",
            index=list(buy_criteria_opts.keys()).index(current_criteria),
            key=f"b_criteria_{market_code}",
            help="매수 주문 시 기준이 되는 가격 (현재가 또는 지지선 예약매수)"
        )
        
    with col2:
        st.markdown("**매도 설정**")
        sell_up = st.slider("목표 수익률 (%)", 1.0, 100.0, t_settings.sell_up_rate, 0.5, key=f"s_up_{market_code}")
        sell_down = st.slider("손절 기준 (%)", -50.0, -1.0, t_settings.sell_down_rate, 0.5, key=f"s_down_{market_code}")
        sell_hold_rate = st.slider(
            "매도 보류 비율 (%)",
            min_value=0.0,
            max_value=100.0,
            value=settings.trading.us.sell_hold_rate if current_market == MarketType.US else settings.trading.kr.sell_hold_rate, # 시장별 설정값 로드
            step=5.0,
            key=f"hold_rate_{market_code}",
            help="종목당 최대 매수금액의 N% 도달 전까지 매도 제외"
        )

        st.divider()

        # 3. 분할 매도 설정
        st.markdown("**📉 분할 매도 & 트레일링 스탑**")
        
        sell_split_rate = st.slider(
            "1회 매도 비율 (%, 분할매도)",
            10.0, 100.0,
            t_settings.sell_split_rate if hasattr(t_settings, 'sell_split_rate') else 100.0,
            step=10.0,
            key=f"split_rate_{market_code}",
            help="매도 신호 발생 시 보유 수량 중 처분할 비율 (100% = 전량 매도)"
        )
        
        # 4. 트레일링 스탑 설정
        ts_col1, ts_col2 = st.columns([1, 2])
        with ts_col1:
            use_ts = st.checkbox(
                "Profit Trailing Stop", 
                value=t_settings.trailing_stop_enabled, 
                key=f"ts_use_{market_code}"
            )
        with ts_col2:
            ts_rate = st.number_input(
                "고점 대비 하락 (%)",
                0.5, 20.0,
                t_settings.trailing_stop_rate,
                step=0.5,
                disabled=not use_ts,
                key=f"ts_rate_{market_code}",
                help="수익 구간에서 당일 고점 대비 N% 하락 시 이익 실현"
            )
        
        # fee_rate와 tax_rate 초기값 설정 (시뮬레이션 모드가 아닐 때를 대비)
        fee_rate = t_settings.fee_rate
        tax_rate = t_settings.tax_rate

        if current_mode == "simulation":
            # 수수료/세금 (시장별 다름)
            st.divider()
            st.markdown("#### 💸 시뮬레이션 수수료/세금 설정")
            st.caption("※ 이 설정은 시뮬레이션(가상매매) 모드에서 수익률 계산 시에만 적용됩니다.")
            
            c1, c2 = st.columns(2)
            with c1:
                fee_rate = st.number_input(
                    "수수료율 (0.00015 = 0.015%)", 
                    0.0, 1.0, 
                    t_settings.fee_rate, 
                    format="%.5f", 
                    step=0.00001,
                    key=f"fee_{market_code}"
                )
            with c2:
                tax_rate = st.number_input(
                    "세금율 (0.0023 = 0.23%)", 
                    0.0, 1.0, 
                    t_settings.tax_rate, 
                    format="%.5f", 
                    step=0.00001,
                    key=f"tax_{market_code}"
                )

    st.divider()
        
    # 저장 버튼
    if st.button(f"💾 {market_code} 매매/평가 설정 저장", type="primary", key=f"save_trade_{market_code}"):
                
        # 상세 평가 기준 업데이트
        new_values = {
            "threshold_grs": th_grs,
            "threshold_bsop_prfi_inrt": th_prof,
            "threshold_rsrv_rate": th_rsrv,
            "threshold_lblt_rate": th_lblt,
            "trend_alignment": trend_align,
            "per_benchmark": per_bench, "per_step": per_step,
            "high_rate_benchmark": high_bench, "high_rate_step": high_step,
            "pbr_benchmark": pbr_bench, "pbr_step": pbr_step,
            "low_rate_benchmark": low_bench, "low_rate_step": low_step,
            "avls_benchmark": avls_bench, "avls_step": avls_step
        }
        # 평가 설정 업데이트
        settings_manager.update_evaluation(
            market=market_code,
            min_total_score=min_score,
            weight_sheet=w_sheet, weight_trend=w_trend, weight_price=w_price, weight_kpi=w_kpi,
            weight_buy=w_buy, weight_avls=w_avls, weight_per=w_per, weight_pbr=w_pbr,
            **new_values
        )

        # 매매 설정 업데이트
        settings_manager.update_trading(
            market=market_code,
            buy_rate=buy_rate,
            max_buy_amount=max_buy_amt,
            limit_count=limit_cnt,
            buy_price_criteria= buy_price_criteria,
            sell_up_rate=sell_up,
            sell_down_rate=sell_down,
            sell_split_rate= sell_split_rate,
            fee_rate=fee_rate,
            tax_rate=tax_rate,
            use_srim_filter=use_srim,
            use_dividend_filter=use_dividend,
            use_roe_filter=use_roe,
            use_cashflow_filter=use_cashflow,
            use_activity_filter=use_activity,
            sell_hold_rate=sell_hold_rate,
            trailing_stop_enabled= use_ts,
            trailing_stop_rate= ts_rate,
        )
        st.toast(f"✅ {market_code} 설정이 저장되었습니다.")

# ==========================================
# 내부 유틸리티 함수 (PriceFetcher 제거됨)
# ==========================================

def get_virtual_account(market: MarketType):
    """가상 계좌 정보 조회 (실시간 시세 반영)"""
    try:
        with get_session() as session:
            # 시장별 계좌 조회
            account = session.query(VirtualAccount).filter_by(market_type=market.value).first()
            if not account:
                return None

            initial_balance = account.total_eval - account.total_profit
            # 안전장치
            if initial_balance <= 0:
                settings_mgr = get_settings_manager()
                if market == MarketType.KR:
                    initial_balance = settings_mgr.settings.trading.kr.initial_balance
                else:
                    initial_balance = settings_mgr.settings.trading.us.initial_balance

            deposit = account.balance or 0
            
            # 보유 종목 조회
            holdings = session.query(VirtualHolding).filter_by(
                market_type=market.value
            ).filter(VirtualHolding.quantity > 0).all()
            
            # Fetcher 선택 (KR / US)
            fetcher = None
            if market == MarketType.KR:
                fetcher = KrFetcher()
            else:
                fetcher = UsFetcher()
            
            total_stock_eval = 0
            
            for h in holdings:
                # 실시간 현재가 조회
                price_data = fetcher.get_current_price(h.item_cd)
                
                if price_data and price_data.get('price') and price_data['price'] > 0:
                    current_price = price_data['price']
                else:
                    current_price = h.avg_price 
                
                total_stock_eval += (current_price * h.quantity)
            
            current_total_asset = deposit + total_stock_eval
            current_profit = current_total_asset - initial_balance
            
            current_profit_rate = 0.0
            if initial_balance > 0:
                current_profit_rate = (current_profit / initial_balance) * 100
            
            return {
                'balance': account.total_eval,
                'deposit' : deposit,
                'total_eval': current_total_asset,
                'total_profit': current_profit,
                'total_profit_rate': current_profit_rate
            }

    except Exception as e:
        print(f"계좌 조회 오류: {e}")
        return None


def reset_virtual_account(market: MarketType, balance: int):
    """가상 계좌 초기화"""
    try:
        with get_session() as session:
            # 해당 시장의 보유 종목 삭제
            session.query(VirtualHolding).filter_by(market_type=market.value).delete()
            
            # 해당 시장의 계좌 삭제 후 재생성
            session.query(VirtualAccount).filter_by(market_type=market.value).delete()
            
            new_account = VirtualAccount(
                market_type=market.value,
                balance=balance,
                total_eval=balance,
                total_profit=0,
                total_profit_rate=0.0
            )
            session.add(new_account)
            session.commit()
    except Exception as e:
        st.error(f"계좌 초기화 오류: {e}")


def delete_data(del_type: str):
    """데이터 삭제"""
    from config.database import (
        ItemMst, ItemPrice, ItemEquity, FinancialSheet, 
        EvaluationResult, TradeHistory, Holdings
    )
    try:
        with get_session() as session:
            if del_type == "all":
                session.query(TradeHistory).delete()
                session.query(Holdings).delete()
                session.query(EvaluationResult).delete()
                session.query(FinancialSheet).delete()
                session.query(ItemEquity).delete()
                session.query(ItemPrice).delete()
                session.query(ItemMst).delete()
                session.commit()
    except Exception as e:
        st.error(f"삭제 오류: {e}")