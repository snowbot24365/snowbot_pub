"""
스케줄 관리 및 모니터링 페이지 UI (KR/US 분리 적용 - 인자 전달 방식)
- 상위 페이지(설정)에서 전달받은 current_market 기준으로 동작
- 스케줄러 상태 및 다음 실행 시간 확인 (모니터링)
- 스케줄 추가/수정/삭제 (현재 시장 자동 적용)
- 강제 실행 테스트 (현재 시장 자동 적용)
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import time

from config.settings import get_settings_manager
from scheduler.task_manager import get_scheduler, TaskType
from core.definition import MarketType


# 1. 일반 작업용 프리셋 (수집/평가용)
DEFAULT_PRESETS_KR = {
    "매일 16시 (장마감후)": "0 16 * * *",
    "매일 18시": "0 18 * * *",
    "매일 20시": "0 20 * * *",
    "매일 22시": "0 22 * * *",
    "매일 00시": "0 0 * * *",
    "매일 02시": "0 2 * * *",
    "매일 04시": "0 4 * * *",
    "매일 06시": "0 6 * * *",
    "직접 입력": ""
}

DEFAULT_PRESETS_US = {
    "매일 07시 (장마감후)": "0 7 * * *",
    "매일 09시": "0 9 * * *",
    "매일 11시": "0 11 * * *",
    "매일 13시": "0 13 * * *",
    "매일 15시": "0 15 * * *",
    "매일 17시": "0 17 * * *",
    "매일 19시": "0 19 * * *",
    "직접 입력": ""
}

# 2. 자동 매매 & 시세 확인용 프리셋
AUTO_TRADE_PRESETS_KR = {
    "1분마다 (장중)": "*/1 9-15 * * mon-fri",
    "5분마다 (장중)": "*/5 9-15 * * mon-fri",
    "10분마다 (장중)": "*/10 9-15 * * mon-fri",
    "15분마다 (장중)": "*/15 9-15 * * mon-fri",
    "20분마다 (장중)": "*/20 9-15 * * mon-fri",
    "30분마다 (장중)": "*/30 9-15 * * mon-fri",
    "1시간마다 (장중)": "0 9-15 * * mon-fri",
    "직접 입력": ""
}

AUTO_TRADE_PRESETS_US = {
    "1분마다 (미국장)": "*/1 22-23,0-6 * * *",
    "5분마다 (미국장)": "*/5 22-23,0-6 * * *",
    "10분마다 (미국장)": "*/10 22-23,0-6 * * *",
    "15분마다 (미국장)": "*/15 22-23,0-6 * * *",
    "20분마다 (미국장)": "*/20 22-23,0-6 * * *",
    "30분마다 (미국장)": "*/30 22-23,0-6 * * *",
    "1시간마다 (미국장)": "0 22-23,0-6 * * *",
    "직접 입력": ""
}


def render_schedule(current_market: MarketType):
    """스케줄 관리 페이지 렌더링 (인자로 시장 정보 받음)"""
    
    market_str = current_market.value
    st.subheader(f"⏰ 스케줄 관리 ({market_str})")
    
    scheduler_service = get_scheduler()
    apscheduler = scheduler_service.scheduler
    
    # 상단 상태 표시줄
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        is_running = scheduler_service.is_running()
        status_text = "🟢 실행 중" if is_running else "🔴 중지됨 (일시정지)"
        st.markdown(f"**상태:** {status_text}")
        
    with col2:
        if apscheduler.timezone:
            now = datetime.now(apscheduler.timezone)
            st.markdown(f"**현재 시간:** {now.strftime('%H:%M:%S')} (KST)")
        
    with col3:
        if is_running:
            if st.button("중지", type="secondary", key=f"stop_sch_{market_str}"):
                scheduler_service.stop()
                st.rerun()
        else:
            if st.button("시작", type="primary", key=f"start_sch_{market_str}"):
                scheduler_service.start()
                st.rerun()
    
    st.divider()
    
    # 탭 구성
    tab_monitor, tab_list, tab_add, tab_log = st.tabs(["🔍 모니터링", "📋 목록", "➕ 추가", "📜 로그"])
    
    # ========== [탭 1] 모니터링 (현재 시장 작업만 표시) ==========
    with tab_monitor:
        st.markdown(f"**{market_str} 실행 예정 작업**")
        
        jobs = apscheduler.get_jobs()
        
        if jobs:
            job_data = []
            for job in jobs:
                # job.args = (task_type, name, market_type) 구조라고 가정
                args = job.args
                job_market = args[2] if len(args) > 2 else "KR"
                
                # [필터링] 현재 선택된 시장과 일치하는 작업만 표시
                if job_market != market_str:
                    continue

                next_run = job.next_run_time
                if not is_running:
                    next_run_str = "⏸️ 대기중"
                else:
                    next_run_str = next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else "⏸️ 일시정지"
                
                task_type_str = args[0] if len(args) > 0 else "-"
                job_name = args[1] if len(args) > 1 else job.name
                
                job_data.append({
                    "작업명": job_name,
                    "유형": task_type_str,
                    "다음 실행 시간": next_run_str,
                    "트리거": str(job.trigger),
                    "ID": job.id
                })
            
            if job_data:
                df = pd.DataFrame(job_data)
                st.dataframe(
                    df, 
                    width="stretch", 
                    hide_index=True,
                    column_config={
                        "다음 실행 시간": st.column_config.TextColumn("다음 실행 시간", help="이 시간에 작업이 실행됩니다.")
                    }
                )
            else:
                st.info(f"{market_str} 시장에 예약된 작업이 없습니다.")
            
            if st.button("🔄 상태 새로고침", key=f"ref_mon_{market_str}"):
                st.rerun()
        else:
            st.info("예약된 작업이 없습니다.")
            
        st.markdown("---")
        
        # 강제 실행 테스트 (현재 시장 고정)
        with st.expander(f"🛠️ {market_str} 강제 실행 테스트 (디버깅용)"):
            st.info(f"설정된 시간과 무관하게 **{market_str} 시장 로직**을 즉시 실행합니다.")
            
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("🚀 데이터 수집", key=f"force_col_{market_str}"):
                    scheduler_service.execute_task(TaskType.DATA_COLLECTION, "[수동] 즉시 실행", market_type=market_str)
                    st.success("데이터 수집 시작됨")
            
            with c2:
                if st.button("🚀 종목 평가", key=f"force_eval_{market_str}"):
                    scheduler_service.execute_task(TaskType.EVALUATION, "[수동] 즉시 실행", market_type=market_str)
                    st.success("종목 평가 시작됨")
            
            with c3:
                if st.button("🚀 자동 매매", key=f"force_trade_{market_str}"):
                    scheduler_service.execute_task(TaskType.AUTO_TRADE, "[수동] 즉시 실행", market_type=market_str)
                    st.success("자동 매매 시작됨")

    # ========== [탭 2] 스케줄 목록 (DB 조회 및 필터링) ==========
    with tab_list:
        st.markdown(f"**{market_str} 등록 스케줄**")
        
        # 전체 조회 후 필터링
        all_schedules = scheduler_service.get_schedules()
        schedules = [s for s in all_schedules if s.market_type == market_str]
        
        if schedules:
            for schedule in schedules:
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                    
                    with col1:
                        status_emoji = "✅" if schedule.enabled else "⏸️"
                        st.markdown(f"**{status_emoji} {schedule.name}**")
                        st.caption(f"Cron: `{schedule.cron_expression}`")
                    
                    with col2:
                        task_names = {
                            TaskType.DATA_COLLECTION: "📥 데이터 수집",
                            TaskType.EVALUATION: "📊 종목 평가",
                            TaskType.AUTO_TRADE: "💰 자동 매매"
                        }
                        st.write(task_names.get(schedule.task_type, schedule.task_type))
                    
                    with col3:
                        job = apscheduler.get_job(str(schedule.id))
                        if job and job.next_run_time:
                            st.caption(f"예정: {job.next_run_time.strftime('%H:%M:%S')}")
                        else:
                            st.caption("-")
                    
                    with col4:
                        if st.button("🗑️", key=f"del_{schedule.id}", help="삭제"):
                            scheduler_service.delete_schedule(schedule.id)
                            st.success("삭제됨")
                            st.rerun()
                    
                    st.divider()
        else:
            st.info(f"{market_str} 시장에 등록된 스케줄이 없습니다.")
    
    # ========== [탭 3] 스케줄 추가 (시장 고정) ==========
    with tab_add:
        st.subheader(f"새 스케줄 추가 ({market_str})")
        
        name = st.text_input("스케줄 이름", placeholder="예: 매일 데이터 수집", key=f"add_name_{market_str}")
        
        task_type = st.selectbox(
            "작업 유형",
            options=[
                TaskType.DATA_COLLECTION,
                TaskType.EVALUATION,
                TaskType.AUTO_TRADE
            ],
            format_func=lambda x: {
                TaskType.DATA_COLLECTION: "📥 데이터 수집",
                TaskType.EVALUATION: "📊 종목 평가",
                TaskType.AUTO_TRADE: "💰 자동 매매"
            }.get(x, x),
            key=f"add_task_type_{market_str}"
        )
        
        st.markdown("#### 실행 시간 설정")
        
        # 시장/작업별 프리셋 자동 분기
        if task_type == TaskType.AUTO_TRADE:
            current_presets = AUTO_TRADE_PRESETS_KR if current_market == MarketType.KR else AUTO_TRADE_PRESETS_US
            info_msg = "평일 09:00 ~ 15:59 (KR)" if current_market == MarketType.KR else "평일 23:00 ~ 06:00 (US)"
        else:
            current_presets = DEFAULT_PRESETS_KR if current_market == MarketType.KR else DEFAULT_PRESETS_US
            info_msg = "장 마감 후(16:00 이후)" if current_market == MarketType.KR else "장 마감 후(07:00 이후)"
            
        st.caption(f"ℹ️ {market_str} 권장 시간대: **{info_msg}**")
            
        preset = st.selectbox("프리셋 선택", options=list(current_presets.keys()), key=f"add_preset_{market_str}")
        
        if preset == "직접 입력":
            default_cron = "0 16 * * *" if current_market == MarketType.KR else "0 7 * * *"
            cron_expression = st.text_input("Cron 표현식", value=default_cron, key=f"add_cron_{market_str}")
        else:
            cron_expression = current_presets[preset]
            st.info(f"Cron 표현식: `{cron_expression}`")
        
        enabled = st.checkbox("활성화", value=True, key=f"add_enabled_{market_str}")
        
        if st.button("스케줄 추가", type="primary", key=f"btn_add_{market_str}"):
            if not name:
                st.error("스케줄 이름을 입력해주세요.")
            elif not cron_expression:
                st.error("Cron 표현식을 입력해주세요.")
            else:
                try:
                    scheduler_service.add_schedule(
                        name=name, 
                        task_type=task_type, 
                        cron_expression=cron_expression, 
                        market_type=market_str, # 현재 시장 정보 전달
                        enabled=enabled
                    )
                    st.success(f"[{market_str}] '{name}' 스케줄이 추가되었습니다.")
                    
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"추가 오류: {e}")
    
    # ========== [탭 4] 실행 로그 (필터링) ==========
    with tab_log:
        st.markdown(f"**{market_str} 실행 로그**")
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            type_options = ["전체", "매매(auto_trade)", "수집(data_collection)", "평가(evaluation)"]
            selected_type_label = st.selectbox("타입 필터", options=type_options, index=0, key=f"log_filter_{market_str}")
            
            search_type = None
            if "전체" not in selected_type_label:
                search_type = selected_type_label.split('(')[-1].replace(')', '') 

        with col2:
            log_limit = st.selectbox("표시 개수", options=[20, 50, 100], index=0, key=f"log_limit_{market_str}")
            
        with col3:
            st.write("") 
            if st.button("🔄 새로고침", width="stretch", key=f"log_ref_{market_str}"):
                st.rerun()
        
        # 로그 조회 시 type_filter 사용 + 결과에서 market_type 필터링
        # (서비스에 market_filter 기능이 없다면 가져와서 거름)
        all_logs = scheduler_service.get_schedule_logs(limit=log_limit * 2, type_filter=search_type, market_type=market_str)
        
        # 시장 필터링
        logs = [l for l in all_logs if l.get('market_type') == market_str]
        logs = logs[:log_limit] # 개수 제한
        
        if logs:
            log_data = []
            for log in logs:
                status_emoji = {'success': '✅', 'failed': '❌', 'running': '🔄'}.get(log.get('status'), '⚪')
                
                log_data.append({
                    "상태": f"{status_emoji} {log.get('status')}",
                    "타입": log.get('task_type', '-'),
                    "작업명": log.get('schedule_name'),
                    "시작 시간": log.get('start_time'),
                    "종료 시간": log.get('end_time'),
                    "메시지": (log.get('message') or log.get('error_message') or "")[:60]
                })
            
            st.dataframe(pd.DataFrame(log_data), width="stretch", hide_index=True)
        else:
            st.info(f"{market_str} 조건에 맞는 로그가 없습니다.")