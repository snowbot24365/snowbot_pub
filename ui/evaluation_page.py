"""
종목평가 페이지 (KR/US 분리 적용 - Main 연동 버전)
- Main의 'Market' 선택에 따라 해당 시장의 평가 로직 및 화면만 표시
"""

import streamlit as st
from datetime import datetime, date
import time
from sqlalchemy import func

from config.settings import get_settings_manager
from config.database import get_session, EvaluationResult, ItemMst, ScheduleLog
from core.definition import MarketType
from scheduler.task_manager import get_scheduler, TaskType
from ui.components import render_log_grid, render_data_grid_with_paging, render_schedule_config, render_log_section
from data.kr.evaluator import EvaluationService as KrEvaluationService
from data.us.evaluator import UsEvaluationService
from utils.common import custom_metric
from io import BytesIO
import pandas as pd


def render_evaluation():
    """종목평가 페이지 렌더링"""
    
    # 1. 현재 선택된 시장 확인
    current_market = st.session_state.get('current_market', MarketType.KR)
    market_str = current_market.value
    
    st.markdown(f'<div class="main-header">📊 종목평가 ({market_str})</div>', unsafe_allow_html=True)
    
    settings_manager = get_settings_manager()
    settings = settings_manager.settings
    
    # ========== [핵심 로직] 최신 데이터 정보 자동 조회 ==========
    latest_date_str, latest_count = get_latest_data_info(current_market)
    
    # ========== 1. 기준 날짜 선택 ==========
    st.markdown("#### 📅 기준 날짜")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        target_base_date = st.date_input(
            "평가 기준일 (결과 저장일)",
            value=date.today(),
            max_value=date.today(),
            key=f"eval_base_date_{market_str}"
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if latest_date_str and latest_count > 0:
            formatted_data_date = f"{latest_date_str[:4]}-{latest_date_str[4:6]}-{latest_date_str[6:]}"
            if target_base_date.strftime('%Y%m%d') != latest_date_str:
                st.info(f"ℹ️ **데이터 출처: {formatted_data_date}** ({latest_count:,}개)")
            else:
                st.success(f"✅ 최신 데이터 기준 ({latest_count:,}개)")
        else:
            st.warning(f"⚠️ {market_str} 수집된 데이터가 없습니다.")

    st.divider()
    
    # ========== 평가 설정 및 실행 ==========
    col1, col2 = st.columns([1, 1])
    
    # 시장별 설정 로드
    if current_market == MarketType.KR:
        eval_settings = settings.evaluation.kr
    else:
        eval_settings = settings.evaluation.us
    
    with col1:
        st.markdown(f"#### ⚙️ {market_str} 평가 설정")
        
        # 1. 최소 점수 필터 (기존 유지)
        min_score = st.slider(
            "최소 총점 (매수 후보 기준)",
            min_value=10,
            max_value=50,
            value=eval_settings.min_total_score,
            key=f"eval_min_score_{market_str}",
            disabled=True
        )
        
        # 2. 평가 설정 상세 (Expander & Tabs)
        with st.expander("📊 설정 상세 보기 (가중치 / 기준 / 안전망)", expanded=False):
            
            # 탭 3개로 구성
            t_weight, t_detail, t_safe = st.tabs(["⚖️ 가중치", "🛠️ 상세 기준", "🛡️ 안전망"])
            
            # [Tab 1] 가중치 설정
            with t_weight:
                st.caption("※ 각 항목 중요도 (0.0 ~ 3.0)")
                
                w_data = [
                    ("재무", eval_settings.weight_sheet), ("모멘텀", eval_settings.weight_trend),
                    ("주가", eval_settings.weight_price), ("KPI", eval_settings.weight_kpi),
                    ("수급", eval_settings.weight_buy), ("시총", eval_settings.weight_avls),
                    ("PER", eval_settings.weight_per), ("PBR", eval_settings.weight_pbr),
                ]
                
                # 작은 글씨로 4열 배치
                wc1, wc2, wc3, wc4 = st.columns(4)
                cols = [wc1, wc2, wc3, wc4]
                for i, (name, val) in enumerate(w_data):
                    with cols[i % 4]:
                        # markdown과 caption을 사용하여 작게 표시
                        st.markdown(f"**{name}**")
                        st.caption(f"{val:.1f}")

            # [Tab 2] 상세 평가 기준
            with t_detail:
                # 시장별 단위 및 설정 로드
                if current_market == MarketType.KR:
                    currency_unit = "억원"
                else:
                    currency_unit = "백만달러"
                    
                st.markdown("**1. 재무 건전성**")
                dc1, dc2 = st.columns(2)
                with dc1:
                    st.caption(f"- 매출증가율: **> {eval_settings.threshold_grs}%**")
                    st.caption(f"- 이익증가율: **> {eval_settings.threshold_bsop_prfi_inrt}%**")
                with dc2:
                    st.caption(f"- 유보율: **> {eval_settings.threshold_rsrv_rate}%**")
                    st.caption(f"- 부채비율: **< {eval_settings.threshold_lblt_rate}%**")
                
                st.divider()
                
                st.markdown("**2. 밸류에이션 (만점 기준)**")
                vc1, vc2 = st.columns(2)
                with vc1:
                    st.caption(f"- PER: **{eval_settings.per_benchmark} 미만**")
                    st.caption(f"- 고가괴리(낙폭): **{eval_settings.high_rate_benchmark}% 미만**")
                with vc2:
                    st.caption(f"- PBR: **{eval_settings.pbr_benchmark} 미만**")
                    st.caption(f"- 저가괴리(급등): **{eval_settings.low_rate_benchmark}% 초과** (감점)")
                    
                st.divider()
                
                st.markdown(f"**3. 시가총액 ({currency_unit})**")
                ac1, ac2 = st.columns(2)
                ac1.caption(f"- 최소기준: **{eval_settings.avls_benchmark:,.0f}**")
                ac2.caption(f"- 증가단위: **{eval_settings.avls_step:,.0f}**")
                
                st.caption(f"※ 추세 전략: **{'정배열(추세추종)' if eval_settings.trend_alignment == 'REGULAR' else '역배열(반등)'}**")

            # [Tab 3] 안전망 설정 (신규 추가)
            with t_safe:
                # 안전망 설정은 TradingSettings에 있으므로 가져와야 함
                if current_market == MarketType.KR:
                    safe_cfg = settings.trading.kr
                else:
                    safe_cfg = settings.trading.us
                
                st.caption("※ 체크된 항목만 필터링에 적용됩니다.")
                
                # 보기 좋게 체크박스 상태를 이모지로 표현
                def get_status(val):
                    return "✅ 적용" if val else "➖ 미적용"

                sc1, sc2 = st.columns(2)
                
                with sc1:
                    st.markdown(f"**SRIM 저평가**: {get_status(safe_cfg.use_srim_filter)}")
                    st.caption(f"(적정주가 이하 매수)")
                    
                    st.markdown(f"**ROE 우량주**: {get_status(safe_cfg.use_roe_filter)}")
                    st.caption(f"(3년 평균 8% 이상)")
                    
                    st.markdown(f"**활동성 우수**: {get_status(safe_cfg.use_activity_filter)}")
                    st.caption(f"(자산회전율 0.3 이상)")

                with sc2:
                    st.markdown(f"**배당주**: {get_status(safe_cfg.use_dividend_filter)}")
                    st.caption(f"(배당수익률 > 0)")
                    
                    st.markdown(f"**현금흐름**: {get_status(safe_cfg.use_cashflow_filter)}")
                    st.caption(f"(잉여현금흐름 > 0)")

        st.caption("💡 설정 변경: 사이드바 > 설정 > 매매/평가 설정")
    
    with col2:
        st.markdown("#### 🚀 실행")
        st.markdown("<br>", unsafe_allow_html=True)
        
        # 실행 버튼
        if st.button("🚀 종목 평가 실행", type="primary", width="stretch", key=f"eval_run_{market_str}"):
            if latest_date_str and latest_count > 0:
                run_evaluation(
                    market_type=current_market,
                    base_date=target_base_date,
                    data_date_str=latest_date_str,
                )
            else:
                st.error("평가할 기초 데이터가 DB에 없습니다. [데이터 수집]을 먼저 수행해주세요.")
                
        # 초기화 버튼
        with st.expander("🗑️ 평가 결과 초기화"):
            st.warning(f"⚠️ {target_base_date} 날짜의 {market_str} 평가 결과가 삭제됩니다!")
            
            if st.button("🗑️ 선택한 날짜 평가 결과 삭제", type="secondary", key=f"eval_delete_{market_str}"):
                delete_evaluation_data(current_market, target_base_date)
    
    st.divider()
    
    # ========== 스케줄 설정 ==========
    # 시장별 키 구분
    render_schedule_config(
        task_type="evaluation",
        schedule_key=f"eval_schedule_{market_str}",
        default_cron="0 3 * * *" if current_market == MarketType.KR else "0 12 * * *",
        market_str=market_str
    )
    
    st.divider()
    
    # ========== 평가 결과 데이터 조회 ==========
    st.markdown(f"#### 📊 {market_str} 평가 결과 조회")
    render_evaluation_result_grid(current_market, target_base_date)
    
    st.divider()
    
    # ========== 실행 로그 ==========
    render_log_section("evaluation", f"📜 {market_str} 평가 로그", key_suffix=market_str)


def get_latest_data_info(market_type: MarketType) -> tuple[str, int]:
    """DB에 저장된 가장 최근 데이터 날짜와 개수 조회 (시장별)"""
    try:
        with get_session() as session:
            latest_date = session.query(func.max(ItemMst.base_date))\
                .filter(ItemMst.market_type == market_type.value).scalar()
            
            if not latest_date:
                return None, 0
            
            count = session.query(ItemMst).filter(
                ItemMst.base_date == latest_date,
                ItemMst.market_type == market_type.value
            ).count()
            
            return latest_date, count
    except Exception as e:
        return None, 0


def run_evaluation(market_type: MarketType, base_date: date, data_date_str: str):
    """종목 평가 실행"""
    
    log_id = 0
    market_str = market_type.value
    
    try:
        with get_session() as session:
            schedule_log = ScheduleLog(
                schedule_id=f"manual_eval_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                schedule_name=f"수동 평가 ({market_str})",
                task_type="evaluation",
                market_type=market_str, # 시장 정보 기록
                status="running",
                start_time=datetime.now(),
                message=f"기준일: {base_date}, 데이터일: {data_date_str}"
            )
            session.add(schedule_log)
            session.flush()
            log_id = schedule_log.id
    except Exception as e:
        st.warning(f"로그 기록 오류: {e}")
    
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
        log_area.code("\n".join(log_messages[-30:]), language=None)
    
    try:
        # 서비스 선택
        if market_type == MarketType.KR:
            if KrEvaluationService:
                eval_service = KrEvaluationService()
            else:
                raise ImportError("KR Evaluation Service not found")
        else:
            eval_service = UsEvaluationService()
        
        status_text.text("종목 평가 시작...")
        update_log(f"[{market_str}] 평가 시작 (기준일: {base_date})")
        
        # 실행
        result = eval_service.run_evaluation(
            base_date=base_date,
            target_data_date=data_date_str,
            progress_callback=update_progress,
            log_callback=update_log
        )
        
        progress_bar.progress(100)
        
        result_msg = f"평가 {result.get('total_evaluated', 0)}건, 매수후보 {result.get('buy_candidates', 0)}건"
        
        status = "success"
        if result.get('errors'):
            status_text.text(f"⚠️ 평가 완료 (오류 {len(result['errors'])}건)")
            result_msg += f", 오류 {len(result['errors'])}건"
        else:
            status_text.text("✅ 평가 완료!")
            st.success(f"{market_str} 평가 완료!")
            
        # 로그 종료
        if log_id:
            try:
                with get_session() as session:
                    log = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
                    if log:
                        log.status = status
                        log.end_time = datetime.now()
                        log.message = result_msg
                        session.commit()
            except: pass
            
        # 결과 요약
        c1, c2, c3 = st.columns(3)
        with c1:
            custom_metric("평가 종목", f"{result.get('total_evaluated', 0)}개")
        with c2:
            custom_metric("매수 후보", f"{result.get('buy_candidates', 0)}개")
        with c3:
            custom_metric("오류", f"{len(result.get('errors', []))}개")
        
    except Exception as e:
        progress_bar.progress(100)
        status_text.text("❌ 오류 발생")
        st.error(f"평가 중 오류: {e}")
        update_log(f"[Error] {e}")
        
        if log_id:
            try:
                with get_session() as session:
                    log = session.query(ScheduleLog).filter(ScheduleLog.id == log_id).first()
                    if log:
                        log.status = "failed"
                        log.end_time = datetime.now()
                        log.error_message = str(e)
                        session.commit()
            except: pass


def delete_evaluation_data(market_type: MarketType, base_date: date):
    """평가 결과 삭제"""
    try:
        with get_session() as session:
            date_str = base_date.strftime('%Y%m%d')
            deleted = session.query(EvaluationResult).filter(
                EvaluationResult.base_date == date_str,
                EvaluationResult.market_type == market_type.value
            ).delete()
            session.commit()
            st.success(f"✅ {market_type.value} 삭제 완료: {deleted}건")
    except Exception as e:
        st.error(f"삭제 오류: {e}")


def render_evaluation_result_grid(market_type: MarketType, query_date: date):
    """평가 결과 데이터 그리드"""
    
    col1, col2 = st.columns([1, 2])
    with col1:
        selected_date = st.date_input(
            "조회 날짜",
            value=query_date,
            max_value=date.today(),
            key=f"grid_date_{market_type.value}"
        )
    with col2:
        show_candidates = st.checkbox("매수 후보만 보기", key=f"grid_chk_{market_type.value}")
        
    try:
        with get_session() as session:
            date_str = selected_date.strftime('%Y%m%d')

            # [단계 1] ItemMst에서 종목별 가장 최신 base_date를 찾는 서브쿼리 생성
            latest_mst_subq = session.query(
                ItemMst.item_cd,
                func.max(ItemMst.base_date).label('max_date')
            ).group_by(ItemMst.item_cd).subquery()
            
            # [단계 2] 쿼리 구성
            query = session.query(EvaluationResult, ItemMst.mrkt_ctg).join(
                latest_mst_subq,
                EvaluationResult.item_cd == latest_mst_subq.c.item_cd
            ).join(
                ItemMst, 
                (ItemMst.item_cd == latest_mst_subq.c.item_cd) & 
                (ItemMst.base_date == latest_mst_subq.c.max_date)
            ).filter(
                EvaluationResult.base_date == date_str,
                EvaluationResult.market_type == market_type.value
            ).order_by(EvaluationResult.total_score.desc())
            
            data = []
            cand_cnt = 0
            
            for row, mrkt_ctg in query.all():
                is_cand = row.is_buy_candidate
                if is_cand: cand_cnt += 1
                if show_candidates and not is_cand: continue
                
                data.append({
                    "시장": mrkt_ctg or "",
                    "종목코드": row.item_cd,
                    "종목명": row.item_nm or "",
                    "총점": row.total_score,
                    "재무": row.sheet_score,
                    "모멘텀": row.trend_score,
                    "주가": row.price_score,
                    "수급": row.buy_score,
                    "시총": row.avls_score,
                    "PER": row.per_score,
                    "PBR": row.pbr_score,
                    "SRIM": "Pass" if row.srim_pass == 1 else "Fail",
                    "현금흐름": "Pass" if row.cashflow_pass == 1 else "Fail",
                    "활동성": "Pass" if row.activity_pass == 1 else "Fail",
                    "배당": "Pass" if row.dividend_pass == 1 else "Fail",
                    "ROE(3Y)": "Pass" if row.roe_pass == 1 else "Fail",
                    "매수": "✅" if is_cand else ""
                })
                
            if data:
                # 컬럼을 4개로 늘려 다운로드 버튼 위치 확보
                c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
                
                with c1:
                    custom_metric("조회 결과", f"{len(data)}건")
                with c2:
                    custom_metric("매수 후보", f"{cand_cnt}건")
                with c3:
                    avg = sum(d['총점'] for d in data) / len(data)
                    custom_metric("평균 점수", f"{avg:.1f}점")
                
                # 엑셀 다운로드 버튼
                with c4:
                    # 1. DataFrame 생성
                    df = pd.DataFrame(data)
                    
                    # 2. 엑셀 파일 메모리에 쓰기
                    output = BytesIO()
                    # engine='xlsxwriter'가 설치되어 있다면 사용하는 것이 좋으나, 없으면 기본값 사용
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df.to_excel(writer, index=False, sheet_name='평가결과')
                    excel_data = output.getvalue()
                    
                    # 3. 버튼 표시 (약간의 상단 여백을 주어 metric과 높이 맞춤)
                    st.write("") # Spacer
                    st.download_button(
                        label="📥 엑셀 다운로드",
                        data=excel_data,
                        file_name=f"evaluation_{market_type.value}_{date_str}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dn_btn_{market_type.value}"
                    )
                
                render_data_grid_with_paging(
                    data, 
                    ["시장", "종목코드", "종목명", "총점", "재무", "모멘텀", "주가", "수급", "시총", "PER", "PBR",
                     "SRIM", "현금흐름", "활동성", "배당", "ROE(3Y)", "매수"], 
                    page_size=20, 
                    key_prefix=f"eval_grid_{market_type.value}"
                )
            else:
                st.info(f"{market_type.value} 평가 결과가 없습니다.")
                
    except Exception as e:
        st.error(f"데이터 조회 오류: {e}")