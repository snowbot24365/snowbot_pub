"""
데이터수집 페이지 (KR/US 분리 - Main 연동 버전)
- Main의 'Market' 선택에 따라 해당 시장의 데이터 수집 화면만 표시
- 기준 날짜 선택, 수집 실행, 데이터 조회, 스케줄 설정, 로그 조회
"""

import streamlit as st
import time
from datetime import datetime, date, timedelta

from config.settings import get_settings_manager
from config.database import get_session, ItemMst, FinancialSheet, ScheduleLog, ItemEquity, EvaluationResult
from core.definition import MarketType

from data.kr.dart_collector import DataCollectionService as KrDataCollector
from data.us.us_collector import UsDataCollector

from scheduler.task_manager import get_scheduler, TaskType
from ui.components import render_log_grid, render_data_grid_with_paging, render_schedule_config, render_log_section
from utils.common import custom_metric

def render_data_collection():
    """데이터수집 페이지 렌더링"""
    
    # 1. 현재 선택된 시장 확인
    current_market = st.session_state.get('current_market', MarketType.KR)
    market_str = current_market.value
    
    st.markdown(f'<div class="main-header">📥 데이터수집 ({market_str})</div>', unsafe_allow_html=True)
    
    settings_manager = get_settings_manager()
    settings = settings_manager.settings
    c_set = settings.collection

    # ========================================================
    # [CASE 1] 한국 주식 (KR) 데이터 수집 화면
    # ========================================================
    if current_market == MarketType.KR:
        # ========== 안내 문구 (OpenDart 한도 & 보관 기간) ==========
        st.warning("⚠️ **Open DART API 주의**: 하루 사용량이 **10,000건**으로 제한됩니다. 초과 시 서비스가 차단될 수 있습니다.")
        st.info("💡 **KIS Open API**: 당일 데이터는 장 종료 후 제공됩니다.")
        st.info("💡 **데이터 보관 정책**: 효율적인 관리를 위해 수집 데이터는 **최근 1개월치만 보관**되며, 수집 실행 시 1개월 이전 데이터는 자동 삭제됩니다.")
        
        st.divider()

        # ========== 기준 날짜 선택 ==========
        st.markdown("#### 📅 기준 날짜")
        
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            base_date = st.date_input(
                "수집 기준일",
                value=date.today(),
                max_value=date.today(),
                key="collection_base_date_kr"
            )
        
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            st.info(f"📅 선택된 날짜: **{base_date.strftime('%Y-%m-%d')}**")
        
        st.divider()
        
        # ========== 수집 설정 및 실행 ==========
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("#### ⚙️ 수집 설정")
            
            # 시장 선택
            collect_kospi = st.checkbox("KOSPI", value=c_set.collect_kospi, key="dc_kospi")
            collect_kosdaq = st.checkbox("KOSDAQ", value=c_set.collect_kosdaq, key="dc_kosdaq")
            
            # 수집 모드
            collection_mode = st.radio(
                "수집 모드",
                options=["random_n", "all"],
                format_func=lambda x: f"무작위 N개 (테스트)" if x == "random_n" else "전체 (스케줄 권장)",
                index=0 if c_set.kr_collection_mode == "random_n" else 1,
                key="dc_mode_kr",
                horizontal=True
            )
            
            if collection_mode == "random_n":
                random_n = st.number_input(
                    "무작위 샘플링 개수 (최대 100개)",
                    min_value=1, max_value=100,
                    value=min(c_set.kr_random_n_stocks, 100),
                    key="dc_random_n_kr"
                )
            else:
                random_n = c_set.kr_random_n_stocks
            
            # 설정 저장
            if st.button("💾 KR 설정 저장", key="kr_dc_save_settings"):
                settings_manager.update_collection(
                    collect_kospi=collect_kospi,
                    collect_kosdaq=collect_kosdaq,
                    kr_collection_mode=collection_mode,
                    kr_random_n_stocks=random_n
                )
                st.success("✅ 설정이 저장되었습니다.")
        
        with col2:
            st.markdown("#### 🚀 실행")
            
            # API 상태 확인
            api_settings = settings.api
            api_ok = True
            
            if not api_settings.opendart_api_key:
                st.error("❌ OpenDart API 키 필요")
                api_ok = False
                
            # KIS API 상태
            kis_mode = api_settings.kis_api_mode_kr
            if kis_mode == "real":
                if not (api_settings.kis_real_app_key_kr and api_settings.kis_real_app_secret_kr):
                    st.warning("⚠️ KIS API (실전) 미설정 - 시세/수급 수집 불가")
            else:
                if not (api_settings.kis_mock_app_key_kr and api_settings.kis_mock_app_secret_kr):
                    st.warning("⚠️ KIS API (모의) 미설정 - 시세/수급 수집 불가")
            
            st.caption(f"📡 데이터 수집 API: {'실전투자' if kis_mode == 'real' else '모의투자'}")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # 실행 버튼
            if st.button("🚀 데이터 수집 실행", type="primary", width="stretch", key="btn_collect_kr", disabled=not api_ok):

                # [추가] 1. 현재 UI 설정값과 저장된 설정값(c_set) 비교
                is_changed = False
                
                # 체크박스 변경 여부 확인
                if collect_kospi != c_set.collect_kospi: is_changed = True
                if collect_kosdaq != c_set.collect_kosdaq: is_changed = True
                
                # 모드 변경 여부 확인
                if collection_mode != c_set.kr_collection_mode: is_changed = True
                
                # 무작위 개수 변경 여부 확인 (모드가 'random_n'일 때만)
                if collection_mode == "random_n" and random_n != c_set.kr_random_n_stocks:
                     is_changed = True

                # [추가] 2. 변경사항이 있다면 실행 차단 및 경고
                if is_changed:
                    st.warning("⚠️ 설정이 변경되었습니다. 좌측의 **[💾 KR 설정 저장]** 버튼을 먼저 눌러주세요.")
                
                # 3. 변경사항이 없을 때만 기존 로직 수행
                else:
                    if not collection_mode:
                        st.warning("⚠️ 수집 모드를 먼저 설정후 저장 해주세요.")
                    if collection_mode == "all":
                        st.error("⛔ 전체 수집은 **자동스케줄 설정**으로만 가능합니다.")
                    elif collection_mode == "random_n" and random_n > 100:
                        st.error("⛔ 무작위 수집은 **최대 100건**까지만 가능합니다.")
                    else:
                        if KrDataCollector:
                            _run_collection("KR", base_date)
                        else:
                            st.error("KR 수집기 모듈을 찾을 수 없습니다.")
            
            if collection_mode == "all":
                st.caption("ℹ️ '전체' 모드는 데이터 양이 많아 스케줄 실행을 권장합니다.")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # 초기화 버튼
            with st.expander("🗑️ 데이터 초기화"):
                st.warning(f"⚠️ {base_date} 날짜의 모든 수집 데이터가 삭제됩니다!")
                if st.button("🗑️ 선택한 날짜 데이터 삭제", type="secondary", key="dc_delete_kr"):
                    delete_collection_data(base_date, "KR")
        
        st.divider()

        # 스케줄 설정
        render_schedule_config("data_collection", "sched_col_kr", "0 18 * * *", market_str)
        
        st.divider()
        
        # 수집 결과 데이터 조회
        render_collection_result_grid("KR") # 파라미터는 market_type
                
        st.divider()
        
        # 실행 로그
        render_log_section("data_collection", f"📜 {market_str} 최근 실행 로그", key_suffix=market_str)

    # ========================================================
    # [CASE 2] 미국 주식 (US) 데이터 수집 화면
    # ========================================================
    else:
        # ========== 안내 문구 ==========
        st.info("💡 **수집 대상**: 나스닥(NASDAQ), 뉴욕(NYSE) 거래소 종목")
        st.info("💡 **속도 안내**: yfinance 라이브러리를 사용하므로 대량 수집 시 시간이 다소 소요될 수 있습니다.")
        st.info("💡 **데이터 보관 정책**: 최근 1개월치만 보관되며, 실행 시 이전 데이터는 정리됩니다.")
        
        st.divider()

        # ========== 기준 날짜 선택 ==========
        st.markdown("#### 📅 기준 날짜")
        
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            base_date = st.date_input(
                "수집 기준일",
                value=date.today(),
                max_value=date.today(),
                key="collection_base_date_us"
            )
        
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            st.info(f"📅 선택된 날짜: **{base_date.strftime('%Y-%m-%d')}**")
        
        st.divider()

        # ========== 수집 설정 및 실행 ==========
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("#### ⚙️ 수집 설정")
            
            # NASDAQ
            c1, c2 = st.columns([1, 2])
            with c1: collect_nasdaq = st.checkbox("NASDAQ", value=c_set.collect_nasdaq, key="dc_nas")
            # with c2: nasdaq_top_n = st.number_input("Top N", 0, 5000, c_set.nasdaq_top_n, disabled=not collect_nasdaq, key="lim_nas", label_visibility="collapsed")
            
            # NYSE
            c1, c2 = st.columns([1, 2])
            with c1: collect_nyse = st.checkbox("NYSE", value=c_set.collect_nyse, key="dc_nys")
            # with c2: nyse_top_n = st.number_input("Top N", 0, 5000, c_set.nyse_top_n, disabled=not collect_nyse, key="lim_nys", label_visibility="collapsed")
            
            # AMEX
            collect_amex = False
            # c1, c2 = st.columns([1, 2])
            # with c1: collect_amex = st.checkbox("AMEX", value=c_set.collect_amex, key="dc_ams")
            # with c2: amex_top_n = st.number_input("Top N", 0, 5000, c_set.amex_top_n, disabled=not collect_amex, key="lim_ams", label_visibility="collapsed")

            st.markdown("---")

            # 수집 모드
            collection_mode = st.radio(
                "수집 모드",
                options=["random_n", "all"],
                format_func=lambda x: f"무작위 N개 (테스트)" if x == "random_n" else "전체 (스케줄 권장)",
                index=0 if c_set.us_collection_mode == "random_n" else 1,
                key="dc_mode_us",
                horizontal=True
            )
            
            if collection_mode == "random_n":
                random_n = st.number_input(
                    "무작위 샘플링 개수 (최대 100개)",
                    min_value=1, max_value=500,
                    value=min(c_set.us_random_n_stocks, 500),
                    key="dc_random_n_us"
                )
            else:
                random_n = c_set.us_random_n_stocks
            
            # 설정 저장
            if st.button("💾 US 설정 저장", key="us_dc_save_settings"):
                settings_manager.update_collection(
                    collect_nasdaq=collect_nasdaq,
                    collect_nyse=collect_nyse,
                    collect_amex=collect_amex,
                    nasdaq_top_n=0,
                    nyse_top_n=0,
                    amex_top_n=0,
                    us_collection_mode=collection_mode,
                    us_random_n_stocks=random_n
                )
                st.success("✅ 설정이 저장되었습니다.")

        with col2:
            st.markdown("#### 🚀 실행")
            
            # API 상태 (KIS for Price Check)
            api_settings = settings.api
            kis_mode = api_settings.kis_api_mode_us
            
            if not (api_settings.kis_real_app_key_us and api_settings.kis_real_app_secret_us):
                st.warning("⚠️ KIS API (실전) 미설정 - 일부 시세 조회 제한될 수 있음")
            
            st.caption(f"📡 API 모드: {'실전투자' if kis_mode == 'real' else '모의투자'}")
            st.markdown("<br>", unsafe_allow_html=True)
            
            # 실행 버튼
            run_disabled = not (collect_nasdaq or collect_nyse or collect_amex)
            if run_disabled:
                st.warning("⚠️ 수집할 거래소를 하나 이상 선택해주세요.")
            
            if st.button("🚀 데이터 수집 실행", type="primary", width="stretch", key="btn_collect_us", disabled=run_disabled):
                # [추가] 1. 현재 UI 설정값과 저장된 설정값(c_set) 비교
                is_changed = False
                
                # 체크박스 변경 여부 확인
                if collect_nasdaq != c_set.collect_nasdaq: is_changed = True
                if collect_nyse != c_set.collect_nyse: is_changed = True
                
                # 모드 변경 여부 확인
                if collection_mode != c_set.us_collection_mode: is_changed = True
                
                # 무작위 개수 변경 여부 확인 (모드가 'random_n'일 때만)
                if collection_mode == "random_n" and random_n != c_set.us_random_n_stocks:
                     is_changed = True

                # [추가] 2. 변경사항이 있다면 실행 차단 및 경고
                if is_changed:
                    st.warning("⚠️ 설정이 변경되었습니다. 좌측의 **[💾 US 설정 저장]** 버튼을 먼저 눌러주세요.")
                
                # 3. 변경사항이 없을 때만 기존 로직 수행
                else:
                    if not collection_mode:
                        st.warning("⚠️ 수집 모드를 먼저 설정후 저장 해주세요.")
                    # 실행 조건 체크 (US는 API 제한이 덜하므로 All 모드도 허용하되 경고)
                    if collection_mode == "all":
                        st.error("⛔ 전체 수집은 **자동스케줄 설정**으로만 가능합니다.")
                    elif collection_mode == "random_n" and random_n > 100:
                        st.error("⛔ 무작위 수집은 **최대 100건**까지만 가능합니다.")
                    else:
                        if UsDataCollector:
                            _run_collection("US", base_date)
                        else:
                            st.error("US 수집기 모듈을 찾을 수 없습니다.")
            
            if collection_mode == "all":
                st.caption("ℹ️ '전체' 모드는 데이터 양이 많아 스케줄 실행을 권장합니다.")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # 초기화 버튼
            with st.expander("🗑️ 데이터 초기화"):
                st.warning(f"⚠️ {base_date} 날짜의 모든 US 수집 데이터가 삭제됩니다!")
                if st.button("🗑️ 선택한 날짜 데이터 삭제", type="secondary", key="dc_delete_us"):
                    delete_collection_data(base_date, "US")

        st.divider()
        
        # 스케줄 설정 (US 전용)
        render_schedule_config("data_collection", "sched_col_us", "0 7 * * *", market_str)
        
        st.divider()
        
        # 실행 로그
        render_log_section("data_collection", f"📜 {market_str} 최근 실행 로그", key_suffix=market_str)
        
        st.divider()
        
        # 수집 결과 데이터 조회
        render_collection_result_grid("US") # 파라미터는 market_type    


def _run_collection(market_type: str, base_date: date = None):
    """수집 로직 실행 (Blocking) - 통합 버전"""
    
    # base_date가 없으면 오늘 날짜 사용
    if not base_date:
        base_date = date.today()

    log_id = save_schedule_log_start("data_collection", f"{market_type} 수동 수집", market_type)
    
    # UI 요소 초기화
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    log_container = st.container()
    log_area = log_container.empty()
    log_messages = []
    
    def update_progress(current, total, message):
        progress = int((current / total) * 100) if total > 0 else 0
        progress_bar.progress(progress)
        status_text.text(f"[{progress}%] {message}")
    
    def update_log(message):
        log_messages.append(message)
        display_logs = log_messages[-30:]
        log_area.code("\n".join(display_logs), language=None)
    
    try:
        # 1. 실행 전 정리 (오래된 데이터)
        _delete_old_data_before_run(market_type, update_log)
        
        status_text.text(f"{market_type} 데이터 수집 시작... (기준일: {base_date})")
        update_log(f"[시작] {market_type} 데이터 수집 시작 (기준일: {base_date})")
        
        # 2. 수집기 실행
        result = {}
        if market_type == "KR":
            if KrDataCollector:
                collector = KrDataCollector()
                # KR 수집기는 progress_callback을 지원한다고 가정 (run_full_collection 사용)
                # 만약 run_incremental_collection만 있다면 해당 메서드의 인자에 맞게 조정 필요
                # 여기서는 기존 run_data_collection의 로직을 따라 run_full_collection 호출 시도
                if hasattr(collector, 'run_full_collection'):
                    result = collector.run_full_collection(
                        base_date=base_date,
                        collect_source='manual',
                        progress_callback=update_progress,
                        log_callback=update_log
                    )
                else:
                    # 메서드명이 다르다면 호환 처리
                    result = collector.run_incremental_collection(
                        collect_source='manual', 
                        log_callback=update_log
                    )
            else:
                raise ImportError("KR Data Collector not found")
                
        else: # US
            collector = UsDataCollector()
            # US 수집기도 log_callback 지원 (progress_callback은 구현 여부에 따라 추가)
            result = collector.run_collection(
                base_date=base_date, 
                collect_source='manual', 
                progress_callback=update_progress, 
                log_callback=update_log
                )
        
        progress_bar.progress(100)
        
        # 3. 결과 처리
        items = result.get('items_collected', 0)
        financial = result.get('financial_collected', 0)
        errors = result.get('errors', [])
        error_cnt = len(errors) if isinstance(errors, list) else errors
        
        result_msg = f"종목 {items}개, 재무 {financial}개 수집"
        
        if error_cnt > 0:
            status_text.text(f"⚠️ 수집 완료 (오류 {error_cnt}건)")
            st.warning(f"수집 완료 (오류 {error_cnt}건)")
            save_schedule_log_end(log_id, "success", result_msg + f", 오류 {error_cnt}건")
        else:
            status_text.text("✅ 수집 완료!")
            st.success(f"{market_type} 데이터 수집이 완료되었습니다.")
            save_schedule_log_end(log_id, "success", result_msg)
            
        # 결과 메트릭 표시 (함수 내부 호출 또는 직접 구현)
        _render_result_metrics(result)
                    
    except Exception as e:
        progress_bar.progress(100)
        status_text.text(f"❌ 오류 발생")
        st.error(f"수집 중 오류 발생: {e}")
        update_log(f"[오류] {e}")
        save_schedule_log_end(log_id, "failed", error=str(e))

def _render_result_metrics(result):
    """수집 결과 요약 메트릭 표시"""
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        custom_metric("종목 저장", f"{result.get('items_collected', 0)}개")
    with col2:
        custom_metric("재무 수집", f"{result.get('financial_collected', 0)}개")
    with col3:
        custom_metric("재무 없음", f"{result.get('financial_skipped', 0)}개")
    with col4:
        errors = result.get('errors', [])
        cnt = len(errors) if isinstance(errors, list) else errors
        custom_metric("오류", f"{cnt}개")


def _delete_old_data_before_run(market_type: str, log_callback=None):
    """실행 전 1개월 이전 데이터 삭제 (시장별 필터링 적용)"""
    try:
        # 1개월 전 날짜 계산
        one_month_ago = date.today() - timedelta(days=30)
        date_str = one_month_ago.strftime('%Y%m%d')
        
        if log_callback:
            log_callback(f"[정리] {market_type} 1개월 이전 데이터 삭제 중... (기준: {date_str} 이전)")
            
        with get_session() as session:
            # 1. EvaluationResult 삭제
            session.query(EvaluationResult).filter(
                EvaluationResult.market_type == market_type,
                EvaluationResult.base_date < date_str
            ).delete(synchronize_session=False)
            
            # 2. FinancialSheet 삭제 (ItemMst와 조인하여 시장 필터)
            subq = session.query(ItemMst.item_cd).filter(ItemMst.market_type == market_type)
            session.query(FinancialSheet).filter(
                FinancialSheet.base_date < date_str,
                FinancialSheet.item_cd.in_(subq)
            ).delete(synchronize_session=False)
            
            # 3. ItemMst 삭제 (오래된 기준일 데이터만)
            session.query(ItemMst).filter(
                ItemMst.market_type == market_type,
                ItemMst.base_date < date_str
            ).delete(synchronize_session=False)
            
            session.commit()
            
        if log_callback:
            log_callback(f"[정리] 데이터 정리 완료")
            
    except Exception as e:
        if log_callback:
            log_callback(f"[정리] 데이터 삭제 중 오류: {e}")


def render_collection_status(market_type: str):
    """수집된 데이터 현황 조회 (통계 요약)"""
    st.markdown(f"#### **📊 {market_type} 수집 데이터 DB 현황**")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button(f"🔄 새로고침", key=f"refresh_{market_type}"):
            st.rerun()
            
    # DB 조회
    try:
        with get_session() as session:
            # 최신 수집 종목 50개 조회
            query = session.query(ItemMst, FinancialSheet)\
                .outerjoin(FinancialSheet, ItemMst.item_cd == FinancialSheet.item_cd)\
                .filter(ItemMst.market_type == market_type)\
                .order_by(ItemMst.updated_date.desc())\
                .limit(50).all()
            
            # 전체 개수 (Count Query)
            total_cnt = session.query(ItemMst).filter(ItemMst.market_type == market_type).count()
            
            c1, c2 = st.columns(2)
            with c1:
                custom_metric("총 수집 종목 수", f"{total_cnt:,}개")
            with c2:
                custom_metric("최근 업데이트", f"{len(query)}개 (최신순)")
            
            # 그리드 렌더링 호출
            render_collection_result_grid(market_type)
                
    except Exception as e:
        st.error(f"데이터 조회 오류: {e}")


def render_collection_result_grid(market_type: str):
    """수집 결과 상세 그리드 (페이징/필터링 포함)"""
    
    # 필터링 옵션
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        # 날짜 선택 (기본값: 오늘)
        target_date = st.date_input(
            "조회 기준일", 
            value=date.today(),
            key=f"grid_date_{market_type}"
        )
        date_str = target_date.strftime('%Y%m%d')
        
    with col2:
        # 재무데이터 유무 필터
        fin_option = st.selectbox(
            "재무데이터", 
            ["전체", "있음", "없음"], 
            key=f"grid_fin_{market_type}"
        )
        
    with col3:
        # 종목명 검색
        search_txt = st.text_input(
            "종목명 검색", 
            placeholder="종목명 입력...", 
            key=f"grid_search_{market_type}"
        )

    try:
        with get_session() as session:
            # 기본 쿼리: ItemMst + FinancialSheet
            query = session.query(
                ItemMst.item_cd, 
                ItemMst.itms_nm, 
                ItemMst.mrkt_ctg, 
                ItemMst.collect_source,
                FinancialSheet.roe_val,
                FinancialSheet.lblt_rate,
                FinancialSheet.grs
            ).outerjoin(
                FinancialSheet, 
                (ItemMst.item_cd == FinancialSheet.item_cd) & (ItemMst.base_date == FinancialSheet.base_date)
            ).filter(
                ItemMst.market_type == market_type,
                ItemMst.base_date == date_str
            )
            
            # 필터링 적용
            if fin_option == "있음":
                query = query.filter(FinancialSheet.item_cd.isnot(None))
            elif fin_option == "없음":
                query = query.filter(FinancialSheet.item_cd.is_(None))
                
            if search_txt:
                query = query.filter(ItemMst.itms_nm.like(f"%{search_txt}%"))
                
            # 결과 조회
            results = query.order_by(ItemMst.item_cd).limit(200).all() # 성능상 200개 제한
            
            data = []
            for row in results:
                has_fin = "✅" if row.roe_val is not None else ""
                source = "🖐️" if row.collect_source == "manual" else "⚡"
                
                data.append({
                    "구분": source,
                    "종목코드": row.item_cd,
                    "종목명": row.itms_nm,
                    "시장": row.mrkt_ctg,
                    "재무": has_fin,
                    "ROE": f"{row.roe_val:.2f}%" if row.roe_val else "-",
                    "부채비율": f"{row.lblt_rate:.2f}%" if row.lblt_rate else "-",
                    "매출증가": f"{row.grs:.2f}%" if row.grs else "-"
                })
                
            if data:
                render_data_grid_with_paging(
                    data,
                    ["구분", "종목코드", "종목명", "시장", "재무", "ROE", "부채비율", "매출증가"],
                    page_size=10,
                    key_prefix=f"res_grid_{market_type}"
                )
            else:
                st.info(f"해당 날짜({target_date})의 조회 결과가 없습니다.")
                
    except Exception as e:
        st.warning(f"데이터 그리드 조회 실패: {e}")


def delete_collection_data(base_date: date, market_type: str):
    """선택한 시장의 수집 데이터 삭제"""
    try:
        with get_session() as session:
            # 날짜 문자열 변환
            date_str = base_date.strftime('%Y%m%d')
            
            # 1. EvaluationResult 삭제 (날짜 + 시장 필터)
            deleted_eval = session.query(EvaluationResult).filter(
                EvaluationResult.base_date == date_str,
                EvaluationResult.market_type == market_type
            ).delete(synchronize_session=False)
            
            # 2. 해당 날짜 및 시장에 해당하는 종목 코드 조회 (FinancialSheet, ItemEquity 삭제용)
            items = session.query(ItemMst.item_cd).filter(
                ItemMst.base_date == date_str,
                ItemMst.market_type == market_type
            ).all()
            item_codes = [i[0] for i in items]
            
            deleted_financial = 0
            deleted_equity = 0
            
            if item_codes:
                # 3. FinancialSheet 삭제 (날짜 + 종목코드 리스트)
                # FinancialSheet에는 market_type이 없을 수 있으므로 종목코드로 필터링
                deleted_financial = session.query(FinancialSheet).filter(
                    FinancialSheet.base_date == date_str,
                    FinancialSheet.item_cd.in_(item_codes)
                ).delete(synchronize_session=False)
                
                # 4. ItemEquity 삭제 (종목코드 리스트)
                # Equity는 날짜 컬럼이 없는 경우가 많으므로 종목코드로 삭제
                deleted_equity = session.query(ItemEquity).filter(
                    ItemEquity.item_cd.in_(item_codes)
                ).delete(synchronize_session=False)
            
            # 5. ItemMst 삭제 (날짜 + 시장 필터)
            deleted_items = session.query(ItemMst).filter(
                ItemMst.base_date == date_str,
                ItemMst.market_type == market_type
            ).delete(synchronize_session=False)
            
            session.commit()
            
            st.success(f"""
            ✅ [{market_type}] 삭제 완료:
            - 종목(ItemMst): {deleted_items}건
            - 재무(FinancialSheet): {deleted_financial}건  
            - 주식정보(ItemEquity): {deleted_equity}건
            - 평가결과(EvaluationResult): {deleted_eval}건
            
            💡 시세 데이터(ItemPrice)는 1년치 이력을 유지하므로 삭제되지 않습니다.
            """)
            time.sleep(2)
            st.rerun()
            
    except Exception as e:
        st.error(f"삭제 오류: {e}")


def save_schedule_log_start(task_type: str, schedule_name: str, market_type: str) -> int:
    """로그 시작 기록"""
    try:
        with get_session() as session:
            log = ScheduleLog(
                schedule_id=f"manual_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                schedule_name=schedule_name,
                task_type=task_type,
                market_type=market_type,
                status="running",
                start_time=datetime.now()
            )
            session.add(log)
            session.commit()
            return log.id
    except Exception as e:
        return 0

def save_schedule_log_end(log_id: int, status: str, message: str = None, error: str = None):
    """로그 종료 기록"""
    if not log_id: return
    try:
        with get_session() as session:
            log = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
            if log:
                log.status = status
                log.end_time = datetime.now()
                log.message = message
                log.error_message = error
                session.commit()
    except:
        pass