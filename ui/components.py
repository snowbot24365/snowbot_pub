"""
공통 UI 컴포넌트 (KR/US 분리 적용 + Key Suffix 지원)
- 계좌 정보 표시 (시장별)
- 로그 그리드 (시장 컬럼 추가)
- 페이징 컴포넌트
- 장 운영 정보 (시장별)
- 공통 스케줄 설정
- 로그 섹션 (키 중복 방지)
"""

import streamlit as st
from datetime import datetime, date, time as dt_time, timedelta
from typing import List, Dict, Optional
import textwrap
import holidays
import logging
import pytz
import pandas as pd
from sqlalchemy import func, desc, distinct
from config.database import get_session, VirtualAccount, VirtualHolding, ScheduleLog, ItemMst, EvaluationResult, VirtualHolding, UserBuyTarget
from config.settings import get_settings_manager
from core.definition import MarketType
import yfinance as yf
import time
import requests
from bs4 import BeautifulSoup
import re

# Fetcher 임포트
from impl.kr.kr_fetcher import KrFetcher
from impl.us.us_fetcher import UsFetcher

logger = logging.getLogger(__name__)


def render_account_info(settings_manager):
    """계좌 정보 표시 (시뮬레이션/모의투자/실계좌 구분 - 시장별)"""
    settings = settings_manager.settings
    
    # 1. 현재 시장 확인
    current_market = st.session_state.get('current_market', MarketType.KR)
    market_str = current_market.value
    currency = "원" if current_market == MarketType.KR else "$"

    # 2. 시장별 실행 모드 확인
    if current_market == MarketType.KR:
        mode = settings.execution_mode_kr
        api_account_mode = settings.api.kis_trading_account_mode_kr
        real_acct = settings.api.kis_real_account_no_kr
        mock_acct = settings.api.kis_mock_account_no_kr
        real_cd = settings.api.kis_real_account_cd_kr
        mock_cd = settings.api.kis_mock_account_cd_kr
    else:
        mode = settings.execution_mode_us
        api_account_mode = settings.api.kis_trading_account_mode_us
        real_acct = settings.api.kis_real_account_no_us
        mock_acct = settings.api.kis_mock_account_no_us
        real_cd = settings.api.kis_real_account_cd_us
        mock_cd = settings.api.kis_mock_account_cd_us

    # 변수 초기화
    deposit = 0
    total_eval = 0
    profit = 0
    profit_rate = 0.0
    holdings_cnt = 0
    
    # 3. 계좌 유형 및 데이터 조회
    if mode == "simulation":
        account_type = "simulation"
        account_label = "🎮 시뮬레이션 계좌"
        bg_color = "#f0f2f6" 
        border_color = "#d1d5db"
        text_color = "#1f2937"
        account_no = "SIMULATION"
        
        # 데이터 조회 (시장별 가상 계좌)
        data = _get_simulation_account_info(current_market)
        if data:
            deposit, total_eval, profit, profit_rate, holdings_cnt = data
        
    elif api_account_mode == "real":
        account_type = "real"
        account_label = "💰 실전투자 계좌 (실거래)"
        bg_color = "#fee2e2" 
        border_color = "#ef4444"
        text_color = "#991b1b"
        account_no = real_acct or "미설정"
        
        data = _get_kis_account_info("real", current_market, real_acct, real_cd)
        if data:
            deposit, total_eval, profit, profit_rate, holdings_cnt = data
        
    else: # mock
        account_type = "mock"
        account_label = "🧪 모의투자 계좌"
        bg_color = "#dbeafe" 
        border_color = "#3b82f6"
        text_color = "#1e40af"
        account_no = mock_acct or "미설정"
        
        data = _get_kis_account_info("mock", current_market, mock_acct, mock_cd)
        if data:
            deposit, total_eval, profit, profit_rate, holdings_cnt = data

    # 4. HTML 렌더링
    profit_color = text_color
    if profit > 0: profit_color = "#ef4444"
    elif profit < 0: profit_color = "#3b82f6"

    html_content = textwrap.dedent(f"""
        <style>
            .account-container {{
                background-color: {bg_color};
                border: 1px solid {border_color};
                border-radius: 8px;
                padding: 20px;
                color: {text_color};
                margin-bottom: 20px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }}
            .account-header {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                margin-bottom: 15px;
                border-bottom: 1px solid {border_color}40;
                padding-bottom: 10px;
            }}
            .account-title {{
                font-size: 1.1rem;
                font-weight: 700;
                margin: 0;
            }}
            .account-number {{
                font-size: 0.9rem;
                opacity: 0.8;
                font-family: monospace;
            }}
            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 15px;
                text-align: center;
            }}
            @media (max-width: 640px) {{
                .metrics-grid {{
                    grid-template-columns: repeat(2, 1fr);
                }}
            }}
            .metric-item {{
                display: flex;
                flex-direction: column;
                gap: 4px;
            }}
            .metric-label {{
                font-size: 0.8rem;
                opacity: 0.7;
            }}
            .metric-value {{
                font-size: 1.2rem;
                font-weight: 700;
            }}
        </style>

        <div class="account-container">
            <div class="account-header">
                <div class="account-title">{account_label} ({market_str})</div>
                <div class="account-number">No. {account_no}</div>
            </div>
            <div class="metrics-grid">
                <div class="metric-item">
                    <span class="metric-label">예수금</span>
                    <span class="metric-value">{deposit:,.0f}{currency}</span>
                </div>
                <div class="metric-item">
                    <span class="metric-label">총 평가금액</span>
                    <span class="metric-value">{total_eval:,.0f}{currency}</span>
                </div>
                <div class="metric-item">
                    <span class="metric-label">손익 (수익률)</span>
                    <span class="metric-value" style="color: {profit_color}">
                        {profit:,.0f}{currency} ({profit_rate:+.2f}%)
                    </span>
                </div>
                <div class="metric-item">
                    <span class="metric-label">보유종목</span>
                    <span class="metric-value">{holdings_cnt}개</span>
                </div>
            </div>
        </div>
    """)

    st.markdown(html_content, unsafe_allow_html=True)
    
    return account_type


def _get_simulation_account_info(market: MarketType):
    """시뮬레이션 계좌 정보 조회 (DB - 시장별)"""
    try:
        with get_session() as session:
            # 시장별 계좌 필터링
            account = session.query(VirtualAccount).filter_by(market_type=market.value).first()
            
            if not account:
                return 0, 0, 0, 0.0, 0
            
            initial_balance = account.total_eval - account.total_profit
            if initial_balance <= 0:
                # 설정에서 초기값 로드
                settings_mgr = get_settings_manager()
                if market == MarketType.KR:
                    initial_balance = settings_mgr.settings.trading.kr.initial_balance
                else:
                    initial_balance = settings_mgr.settings.trading.us.initial_balance

            deposit = account.balance or 0
            
            # 보유 종목 조회
            holdings = session.query(VirtualHolding).filter(
                VirtualHolding.quantity > 0,
                VirtualHolding.market_type == market.value
            ).all()
            
            holdings_cnt = len(holdings)
            
            # Fetcher 선택 (시세 조회용)
            fetcher = KrFetcher() if market == MarketType.KR else UsFetcher()
            
            total_stock_eval = 0
            
            for h in holdings:
                price_data = fetcher.get_current_price(h.item_cd)
                if price_data and price_data.get('price') and price_data['price'] > 0:
                    current_price = price_data['price']
                else:
                    current_price = h.avg_price
                
                total_stock_eval += (current_price * h.quantity)
            
            current_total_asset = deposit + total_stock_eval
            current_profit = current_total_asset - initial_balance
            
            profit_rate = 0.0
            if initial_balance > 0:
                profit_rate = (current_profit / initial_balance) * 100
            
            return deposit, current_total_asset, current_profit, profit_rate, holdings_cnt

    except Exception as e:
        logger.error(f"시뮬레이션 계좌 조회 오류: {e}")
        return 0, 0, 0, 0.0, 0


def _get_kis_account_info(mode: str, market: MarketType, account_no: str, account_cd: str):
    """KIS API 잔고 조회 (실전/모의 - 시장별)"""
    try:
        # Fetcher 선택
        fetcher = None
        if market == MarketType.KR:
            fetcher = KrFetcher(mode=mode)
        else:
            fetcher = UsFetcher(mode=mode)
        
        if not fetcher.is_configured():
            return 0, 0, 0, 0.0, 0
        
        if not account_no or not account_cd:
            return 0, 0, 0, 0.0, 0
            
        balance_info = fetcher.get_account_balance(account_no, account_cd)
        
        if balance_info:
            profit = balance_info.get('profit', 0)
            if 'profit' not in balance_info:
                # profit 필드가 없으면 계산
                profit = balance_info.get('total_eval', 0) - balance_info.get('total_buy_amt', 0)

            return (
                balance_info.get('deposit', 0),
                balance_info.get('total_eval', 0),
                profit,
                balance_info.get('profit_rate', 0.0),
                balance_info.get('holdings_count', len(balance_info.get('holdings', [])))
            )
            
        return 0, 0, 0, 0.0, 0
        
    except Exception as e:
        logger.error(f"KIS 계좌 조회 오류: {e}")
        return 0, 0, 0, 0.0, 0


import pytz # 시간대 처리를 위해 필요
import holidays
from datetime import datetime, time as dt_time

def render_market_status():
    """장 운영 정보 표시 (한국/미국 분기)"""
    current_market = st.session_state.get('current_market', MarketType.KR)
    
    now = datetime.now()
    today = now.date()
    current_time = now.time()

    # 1. 미국 동부 시간(EST/EDT) 기준 변환 (서머타임 자동 적용)
    us_timezone = pytz.timezone('US/Eastern')
    now_us = datetime.now(us_timezone)
    today_us = now_us.date()
    current_time_us = now_us.time()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info(f"📅 **오늘 날짜**: {today.strftime('%Y-%m-%d')} ({['월','화','수','목','금','토','일'][today.weekday()]})")
    with col2:
        if current_market == MarketType.KR:
            st.info(f"🕐 **현재 시간** (KR): {current_time.strftime('%H:%M:%S')}")
        else:
            st.info(f"🕐 **현재 시간** (KR): {current_time.strftime('%H:%M:%S')}, (US): {current_time_us.strftime('%H:%M:%S')}")

    # ---------------------------------------------------------
    # [1] 한국 시장 (KR)
    # ---------------------------------------------------------
    if current_market == MarketType.KR:
        # 한국 공휴일 및 장 시간 체크
        kr_holidays = holidays.KR()
        is_weekend = today.weekday() >= 5
        is_holiday = today in kr_holidays
        
        market_open = dt_time(9, 0)
        market_close = dt_time(15, 30)
        is_market_hours = market_open <= current_time <= market_close
        
        if is_weekend:
            status = "휴장 (주말)"
            status_color = "🔴"
        elif is_holiday:
            holiday_name = kr_holidays.get(today, "공휴일")
            status = f"휴장 ({holiday_name})"
            status_color = "🔴"
        elif is_market_hours:
            status = "장 운영 중"
            status_color = "🟢"
        elif current_time < market_open:
            status = "장 시작 전"
            status_color = "🟡"
        else:
            status = "장 마감"
            status_color = "🟡"
            
        with col3:
            if status_color == "🟢":
                st.success(f"{status_color} **{status}**")
            elif status_color == "🔴":
                st.error(f"{status_color} **{status}**")
            else:
                st.warning(f"{status_color} **{status}**")
            
        return {
            'is_market_open': is_market_hours and not is_weekend and not is_holiday,
            'is_trading_day': not is_weekend and not is_holiday,
            'status': status
        }
    
    # ---------------------------------------------------------
    # [2] 미국 시장 (US) - 완성된 코드
    # ---------------------------------------------------------
    else:
                
        # 2. 미국 공휴일 체크 (NYSE 시장 기준)
        us_holidays = holidays.US(years=today_us.year)
        
        is_weekend_us = today_us.weekday() >= 5 # 토, 일
        is_holiday_us = today_us in us_holidays
        
        # 3. 미국 정규장 시간 (09:30 ~ 16:00)
        market_open_us = dt_time(9, 30)
        market_close_us = dt_time(16, 0)
        is_market_hours_us = market_open_us <= current_time_us <= market_close_us
        
        # 4. 상태 판별
        if is_weekend_us:
            status = "휴장 (주말)"
            status_color = "🔴"
        elif is_holiday_us:
            holiday_name = us_holidays.get(today_us, "US 공휴일")
            status = f"휴장 ({holiday_name})"
            status_color = "🔴"
        elif is_market_hours_us:
            status = "장 운영 중 (정규장)"
            status_color = "🟢"
        elif current_time_us < market_open_us:
            status = "장 시작 전 (Pre-market)"
            status_color = "🟡"
        else:
            status = "장 마감 (After-hours)"
            status_color = "🟡"
            
        # UI 표시 (미국 현지 시간 함께 표시)
        with col3:
            if status_color == "🟢":
                st.success(f"{status_color} **{status}**")
            elif status_color == "🔴":
                st.error(f"{status_color} **{status}**")
            else:
                st.warning(f"{status_color} **{status}**")
            
        return {
            'is_market_open': is_market_hours_us and not is_weekend_us and not is_holiday_us,
            'is_trading_day': not is_weekend_us and not is_holiday_us,
            'status': status
        }


def render_log_grid(logs: List[Dict], task_type_filter: Optional[str] = None, show_filter: bool = True, height: int = 300):
    """실행 로그 그리드"""
    if not logs:
        st.info("실행 로그가 없습니다.")
        return
    
    # 필터링 옵션
    if show_filter:
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            task_types = ["전체"] + list(set(log.get('task_type', '') for log in logs))
            default_idx = task_types.index(task_type_filter) if task_type_filter in task_types else 0
            selected_type = st.selectbox("작업 유형", task_types, index=default_idx, key=f"log_type_{id(logs)}")
        with col2:
            statuses = ["전체", "success", "failed", "running"]
            selected_status = st.selectbox("상태", statuses, key=f"log_status_{id(logs)}")
    else:
        selected_type = task_type_filter or "전체"
        selected_status = "전체"
    
    filtered_logs = logs
    if selected_type != "전체":
        filtered_logs = [log for log in filtered_logs if log.get('task_type') == selected_type]
    if selected_status != "전체":
        filtered_logs = [log for log in filtered_logs if log.get('status') == selected_status]
    
    log_data = []
    for log in filtered_logs:
        status_emoji = {'success': '✅', 'failed': '❌', 'running': '🔄'}.get(log.get('status', ''), '⚪')
        log_data.append({
            "상태": f"{status_emoji} {log.get('status', '')}",
            "시장": log.get('market_type', '-'),
            "작업": log.get('task_type', ''),
            "이름": log.get('schedule_name', ''),
            "시작": log.get('start_time', '')[:19] if log.get('start_time') else "",
            "메시지": log.get('message') or log.get('error_message') or ""
        })
    
    if log_data:
        st.dataframe(log_data, width="stretch", hide_index=True, height=height)
    else:
        st.info("조건에 맞는 로그가 없습니다.")


def render_data_grid_with_paging(data: List[Dict], columns: List[str], page_size: int = 20, key_prefix: str = "grid"):
    """페이징 데이터 그리드"""
    if not data:
        st.info("데이터가 없습니다.")
        return
    
    total_count = len(data)
    total_pages = (total_count + page_size - 1) // page_size
    
    page_key = f"{key_prefix}_page"
    if page_key not in st.session_state: st.session_state[page_key] = 1
    current_page = st.session_state[page_key]
    
    c1, c2, c3, c4, c5 = st.columns([1,1,2,1,1])
    with c1: 
        if st.button("⏮️", key=f"{key_prefix}_first", disabled=current_page==1): 
            st.session_state[page_key] = 1; st.rerun()
    with c2:
        if st.button("◀️", key=f"{key_prefix}_prev", disabled=current_page==1):
            st.session_state[page_key] -= 1; st.rerun()
    with c3: st.markdown(f"<center>{current_page} / {total_pages} ({total_count}건)</center>", unsafe_allow_html=True)
    with c4:
        if st.button("▶️", key=f"{key_prefix}_next", disabled=current_page>=total_pages):
            st.session_state[page_key] += 1; st.rerun()
    with c5:
        if st.button("⏭️", key=f"{key_prefix}_last", disabled=current_page>=total_pages):
            st.session_state[page_key] = total_pages; st.rerun()
            
    start = (current_page - 1) * page_size
    end = start + page_size
    
    # 컬럼 필터링
    display_data = [{col: row.get(col, '') for col in columns} for row in data[start:end]]
    st.dataframe(display_data, width="stretch", hide_index=True)


def render_schedule_config(task_type: str, schedule_key: str, default_cron: str = "0 18 * * *", market_str: str = None):
    """스케줄 설정"""
    from scheduler.task_manager import get_scheduler
    
    st.markdown("#### 📅 스케줄 설정")

    default_presets_kr = {
        "매일 16시 (장마감후)": "0 16 * * *",
        "매일 18시": "0 18 * * *",
        "매일 20시": "0 20 * * *",
        "매일 22시": "0 22 * * *",
        "매일 00시": "0 0 * * *",
        "매일 02시": "0 2 * * *",
        "매일 04시": "0 4 * * *",
        "매일 06시": "0 6 * * *",
        "사용자 정의": "custom"
    }

    # 자동 매매용 프리셋 (장중 위주)
    auto_trade_presets_kr = {
        "1분마다 (장중)": "*/1 9-15 * * mon-fri",
        "5분마다 (장중)": "*/5 9-15 * * mon-fri",
        "10분마다 (장중)": "*/10 9-15 * * mon-fri",
        "15분마다 (장중)": "*/15 9-15 * * mon-fri",
        "20분마다 (장중)": "*/20 9-15 * * mon-fri",
        "30분마다 (장중)": "*/30 9-15 * * mon-fri",
        "1시간마다 (장중)": "0 9-15 * * mon-fri",
        "사용자 정의": "custom"
    }

    default_presets_us = {
        "매일 07시 (장마감후)": "0 7 * * *",
        "매일 09시": "0 9 * * *",
        "매일 11시": "0 11 * * *",
        "매일 13시": "0 13 * * *",
        "매일 15시": "0 15 * * *",
        "매일 17시": "0 17 * * *",
        "매일 19시": "0 19 * * *",
        "사용자 정의": "custom"
    }

    # 자동 매매용 프리셋 (장중 위주)
    auto_trade_presets_us = {
        # KST 기준 22시~06시 (썸머타임 고려하여 넉넉하게 설정)
        # 요일은 금요일 장이 토요일 새벽에 끝나므로 매일(*)로 설정하고 로직에서 장 운영 여부 체크 권장
        "1분마다 (미국장)": "*/1 22-23,0-6 * * *",
        "5분마다 (미국장)": "*/5 22-23,0-6 * * *",
        "10분마다 (미국장)": "*/10 22-23,0-6 * * *",
        "15분마다 (미국장)": "*/15 22-23,0-6 * * *",
        "20분마다 (미국장)": "*/20 22-23,0-6 * * *",
        "30분마다 (미국장)": "*/30 22-23,0-6 * * *",
        "1시간마다 (미국장)": "0 22-23,0-6 * * *",
        "사용자 정의": "custom"
    }
    
    scheduler = get_scheduler()
    
    # 기존 스케줄 조회
    schedules = scheduler.get_schedules(market_str)
    existing = [s for s in schedules if s.task_type == task_type]
    
    if existing:
        st.markdown("**등록된 스케줄:**")
        for sch in existing:
            c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
            with c1: st.text(f"📌 {sch.name} ({sch.market_type})")
            with c2: st.text(f"⏰ {sch.cron_expression}")
            with c3: st.text("✅ 활성" if sch.enabled else "❌ 비활성")
            with c4:
                if st.button("삭제", key=f"del_{sch.id}"):
                    scheduler.delete_schedule(sch.id)
                    st.success("삭제됨")
                    st.rerun()

    with st.expander("➕ 새 스케줄 추가"):
        c1, c2 = st.columns(2)
        with c1:
            schedule_name = st.text_input("스케줄 이름", value=f"{task_type}_job_{market_str}", key=f"{schedule_key}_name_{market_str}")
        with c2:
            # 작업 유형에 따라 프리셋 교체
            if task_type == "auto_trade":
                cron_presets = auto_trade_presets_kr if market_str == "KR" else auto_trade_presets_us
            else:
                cron_presets = default_presets_kr if market_str == "KR" else default_presets_us
            
            preset = st.selectbox(
                "실행 시간",
                list(cron_presets.keys()),
                key=f"{schedule_key}_preset_{market_str}"
            )
        
        if preset == "사용자 정의":
            cron_expr = st.text_input(
                "Cron 표현식",
                value=default_cron,
                key=f"{schedule_key}_cron_{market_str}",
                help="분 시 일 월 요일 (예: 0 18 * * * = 매일 오후 6시)"
            )
        else:
            cron_expr = cron_presets[preset]
            st.caption(f"Cron: `{cron_expr}`")
        
        enabled = st.checkbox("활성화", value=True, key=f"{schedule_key}_enabled_{market_str}")
        
        if st.button("스케줄 추가", key=f"{schedule_key}_add_{market_str}", type="primary"):
            try:
                scheduler.add_schedule(
                    name=schedule_name,
                    task_type=task_type,
                    cron_expression=cron_expr,
                    market_type=market_str,
                    enabled=enabled
                )
                st.success(f"스케줄이 추가되었습니다: {schedule_name}")
                st.rerun()
            except Exception as e:
                st.error(f"스케줄 추가 실패: {e}")

def render_log_section(
    task_type: str, 
    title: str = "📜 최근 실행 로그", 
    key_suffix: str = ""
):
    """
    로그 섹션: st.expander를 활용한 리스트 형태
    - 체크박스 없이 행(헤더)을 클릭하면 상세 내용이 펼쳐짐
    """
    from config.database import get_session, ScheduleLog
    
    # 1. 상단 헤더 및 새로고침 버튼
    c1, c2 = st.columns([8, 2])
    with c1:
        st.markdown(f"#### {title}")
    with c2:
        unique_key = f"ref_{task_type}_{key_suffix}"
        if st.button("🔄 새로고침", key=unique_key): st.rerun()
        
    try:
        with get_session() as session:
            # 2. 쿼리 조회
            query = session.query(ScheduleLog).filter(ScheduleLog.task_type == task_type)
            if key_suffix:
                query = query.filter(ScheduleLog.market_type == key_suffix)
            
            # 최신순 20개 조회
            logs = query.order_by(ScheduleLog.start_time.desc()).limit(20).all()
            
            if not logs:
                st.info(f"{key_suffix} 로그가 없습니다.")
                return

            for log in logs:
                # 데이터 가공
                emoji = {'success':'✅', 'failed':'❌'}.get(log.status, '🔄')
                time_str = log.start_time.strftime('%m-%d %H:%M') if log.start_time else "-"
                
                raw_msg = log.message or log.error_message or ""
                # 헤더에 보여줄 한 줄 요약 (개행 제거 및 길이 제한)
                summary_msg = raw_msg.replace('\n', ' ').strip()
                if len(summary_msg) > 40:
                    summary_msg = summary_msg[:40] + "..."
                if not summary_msg:
                    summary_msg = "(내용 없음)"

                # [핵심] Expander의 라벨을 표의 행처럼 구성
                # **굵게** 표시하거나 이모지를 섞어 가독성을 높임
                # 예: ✅  02-05 10:00  |  매수주문  |  매수 주문이 성공적으로...
                label = f"{emoji}  {time_str}  |  {log.schedule_name}  |  {summary_msg}"
                
                with st.expander(label):
                    # --- 상세 내용 영역 ---
                    st.markdown(f"**📌 상세 정보**")
                    st.text(f"• ID: {log.id}")
                    st.text(f"• 구분: {log.market_type} / {log.task_type}")
                    st.text(f"• 종료 시간: {log.end_time.strftime('%H:%M:%S') if log.end_time else '-'}")
                    
                    st.markdown("**📝 전체 메시지**")
                    if raw_msg:
                        # st.code를 사용하면 긴 텍스트도 스크롤/개행이 완벽하게 지원됨
                        st.code(raw_msg, language="text", wrap_lines=True)
                    else:
                        st.caption("메시지 내용이 없습니다.")

    except Exception as e:
        st.error(f"로그 조회 실패: {e}")

# [1] 저장 확인용 다이얼로그 (로직 반전 반영)
@st.dialog("⚠️ 매수 후보 변경 확인")
def confirm_save_dialog(valid_changes, invalid_names, base_date, market_value):
    """
    저장 전 확인 모달 창
    valid_changes: [(code, is_buy_candidate, item_name), ...] 
                   -> 여기서는 is_buy_candidate(True/False)가 넘어옴
    """
    # 1. 사용자 종목 제외 시도 알림
    if invalid_names:
        st.error(
            f"🚫 **제외 불가 ({len(invalid_names)}개)**\n\n"
            f"{', '.join(invalid_names)}\n\n"
            "사용자가 추가한 관심 종목은 **매수 제외할 수 없습니다.**"
        )
        st.markdown("---")

    # 2. 실제 저장될 변경 사항 확인
    if valid_changes:
        # 제외되는 종목 (is_buy_candidate가 False가 되는 것들)
        excluded_list = [name for _, is_candidate, name in valid_changes if not is_candidate]
        
        if excluded_list:
            st.warning(
                f"📉 **제외 예정 ({len(excluded_list)}개)**\n\n"
                f"{', '.join(excluded_list)}\n\n"
                "위 종목들을 금일 매수 대상에서 **제외**하시겠습니까?"
            )
        
        # 다시 포함되는 종목 (is_buy_candidate가 True가 되는 것들)
        included_cnt = len(valid_changes) - len(excluded_list)
        if included_cnt > 0:
            st.info(f"🔄 **매수 복구**: {included_cnt}개 종목을 다시 매수 대상으로 포함합니다.")
            
        st.markdown("---")
        
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("취소", width='stretch'):
                st.rerun()
                
        with col2:
            if st.button("확인 (저장)", type="primary", width='stretch'):
                try:
                    with get_session() as update_session:
                        for code, is_candidate, _ in valid_changes:
                            update_session.query(EvaluationResult).filter(
                                EvaluationResult.item_cd == code,
                                EvaluationResult.base_date == base_date,
                                EvaluationResult.market_type == market_value
                            ).update({"is_buy_candidate": is_candidate})
                        update_session.commit()
                    
                    st.toast(f"✅ {len(valid_changes)}건 저장 완료!", icon="💾")
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"저장 실패: {e}")
    else:
        st.warning("저장할 수 있는 유효한 변경 사항이 없습니다.")
        if st.button("닫기", width='stretch'):
            st.rerun()

# [2] 메인 렌더링 함수
def render_buy_candidates_table(base_date, min_score, current_market):
    """매수 후보 종목 테이블 (KR:네이버증권, US:Perplexity 연결)"""
    if not base_date:
        return

    try:
        with get_session() as session:

            # 사용자가 선택한 날짜(base_date) 대신 DB에 있는 가장 최신 데이터를 기준으로 함
            max_date_query = session.query(func.max(EvaluationResult.base_date))\
                .filter(EvaluationResult.market_type == current_market.value)
            
            target_date = max_date_query.scalar()

            if not target_date:
                st.warning(f"{current_market.value} 시장의 평가 데이터가 존재하지 않습니다.")
                return

            # (옵션) 사용자에게 현재 보여주는 데이터의 날짜를 인지시켜줌
            st.caption(f"📅 조회 기준일: {target_date} (최신 데이터)")

            candidates_map = {}
            user_target_codes = set()

            # --- [A] 사용자 관심종목 로드 ---
            user_targets = session.query(UserBuyTarget, ItemMst.mrkt_ctg, ItemMst.itms_nm)\
                .outerjoin(ItemMst, UserBuyTarget.item_cd == ItemMst.item_cd)\
                .filter(UserBuyTarget.market_type == current_market.value).all()
            
            for t, mrkt_ctg, mst_nm in user_targets:
                user_target_codes.add(t.item_cd)
                final_name = mst_nm if mst_nm else t.item_nm
                final_market = mrkt_ctg if mrkt_ctg else t.exch_code

                # [수정 1] 시장별 URL 생성 분기
                if current_market.value == "KR":
                    # 네이버 증권 (종목코드만 있으면 됨)
                    full_url = f"https://finance.naver.com/item/main.naver?code={t.item_cd}"
                else:
                    # 미국 주식 (Perplexity)
                    full_url = f"https://www.perplexity.ai/finance/{t.item_cd}"

                candidates_map[t.item_cd] = {
                    "구분": "⭐",
                    "종목코드": full_url,
                    "종목명": final_name,
                    "시장": final_market or "-",
                    "제외": False, 
                    "총점": 0, "재무": 0, "추세": 0, "수급": 0, "주가": 0, "KPI": 0, "시총": 0, "PER": 0, "PBR": 0,
                    "SRIM": "-", "현금흐름": "-", "활동성": "-", "배당": "-", "ROE(3Y)": "-"
                }

            # --- [B] 최신 평가 데이터 조회 ---
            mst_sub = session.query(ItemMst.item_cd, ItemMst.itms_nm, ItemMst.mrkt_ctg).distinct().subquery()
            
            # 사용자 종목 업데이트
            if user_target_codes:
                eval_rows = session.query(EvaluationResult).filter(
                    EvaluationResult.item_cd.in_(user_target_codes),
                    EvaluationResult.base_date == target_date,
                    EvaluationResult.market_type == current_market.value 
                ).all()
                
                for res in eval_rows:
                    if res.item_cd in candidates_map:
                        candidates_map[res.item_cd].update({
                            "제외": False, 
                            "총점": res.total_score,
                            "재무": res.sheet_score, "추세": res.trend_score, "수급": res.buy_score, "주가": res.price_score,
                            "KPI": res.kpi_score, "시총": res.avls_score, "PER": res.per_score, "PBR": res.pbr_score,
                            "SRIM": "Pass" if res.srim_pass == 1 else "Fail",
                            "현금흐름": "Pass" if res.cashflow_pass == 1 else "Fail",
                            "활동성": "Pass" if res.activity_pass == 1 else "Fail",
                            "배당": "Pass" if res.dividend_pass == 1 else "Fail",
                            "ROE(3Y)": "Pass" if res.roe_pass == 1 else "Fail",
                        })

            # 알고리즘 추천 종목 조회 (Top 20)
            query = session.query(
                EvaluationResult, mst_sub.c.itms_nm, mst_sub.c.mrkt_ctg
            ).outerjoin(
                mst_sub, EvaluationResult.item_cd == mst_sub.c.item_cd
            ).filter(
                EvaluationResult.base_date == target_date,
                EvaluationResult.market_type == current_market.value,
                EvaluationResult.total_score >= min_score,
                EvaluationResult.is_buy_candidate == True 
            ).order_by(desc(EvaluationResult.total_score)).limit(20)
            
            rows = query.all()
            for res, nm, mkt in rows:
                item_cd = res.item_cd
                
                # [수정 1] 시장별 URL 생성 분기
                if current_market.value == "KR":
                    full_url = f"https://finance.naver.com/item/main.naver?code={item_cd}"
                else:
                    full_url = f"https://www.perplexity.ai/finance/{item_cd}"

                row_data = {
                    "종목코드": full_url,
                    "종목명": nm, 
                    "시장": mkt,
                    "제외": False,
                    "총점": res.total_score,
                    "재무": res.sheet_score, "추세": res.trend_score, "수급": res.buy_score, "주가": res.price_score,
                    "KPI": res.kpi_score, "시총": res.avls_score, "PER": res.per_score, "PBR": res.pbr_score,
                    "SRIM": "Pass" if res.srim_pass == 1 else "Fail",
                    "현금흐름": "Pass" if res.cashflow_pass == 1 else "Fail",
                    "활동성": "Pass" if res.activity_pass == 1 else "Fail",
                    "배당": "Pass" if res.dividend_pass == 1 else "Fail",
                    "ROE(3Y)": "Pass" if res.roe_pass == 1 else "Fail",
                }

                if item_cd in candidates_map:
                    candidates_map[item_cd].update(row_data)
                    candidates_map[item_cd]["구분"] = "⭐+📊"
                else:
                    row_data["구분"] = "📊"
                    candidates_map[item_cd] = row_data

            # --- [C] 화면 출력 및 저장 로직 ---
            final_list = list(candidates_map.values())
            final_list.sort(key=lambda x: (1 if x['제외'] else 0, -x['총점']))
            
            if final_list:
                df = pd.DataFrame(final_list)
                
                cols_order = ["제외", "구분", "종목코드", "종목명", "시장", "총점", 
                              "재무", "추세", "수급", "주가", "KPI", "시총", "PER", "PBR", 
                              "SRIM", "현금흐름", "활동성", "배당", "ROE(3Y)"]
                df = df[[c for c in cols_order if c in df.columns]]

                # [수정 2] 화면 표시용 정규식 설정
                if current_market.value == "KR":
                    # 네이버: code=123456 -> 123456
                    link_display = r"code=(\d+)"
                else:
                    # Perplexity: finance/AAPL -> AAPL
                    link_display = r"finance/(.*)"

                # 1. 편집 가능한 테이블 출력
                edited_df = st.data_editor(
                    df,
                    width='stretch', # 기존 요청 유지
                    hide_index=True,
                    column_config={
                        "제외": st.column_config.CheckboxColumn(
                            "제외",
                            help="체크하면 금일 매수 대상에서 **제외**됩니다.",
                            default=False,
                        ),
                        "종목코드": st.column_config.LinkColumn(
                            "종목코드",
                            help="클릭 시 상세 정보 페이지로 이동합니다.",
                            display_text=link_display # 정규식 적용
                        ),
                        "총점": st.column_config.ProgressColumn(format="%.1f", min_value=0, max_value=40),
                        "구분": st.column_config.TextColumn("출처", width="small"),
                    },
                    disabled=[c for c in df.columns if c != "제외"],
                    key=f"editor_{current_market.value}_{target_date}"
                )

                # 2. 저장 버튼
                _, col_btn = st.columns([6, 1])
                with col_btn:
                    # 기존 요청 유지 (width='content' -> 'stretch' 등 원하시는 대로 조정 가능)
                    if st.button("💾 저장", type="secondary", width='content'):
                        if not df.equals(edited_df):
                            original_status = df.set_index("종목코드")["제외"]
                            new_status = edited_df.set_index("종목코드")["제외"]
                            name_map = df.set_index("종목코드")["종목명"].to_dict()
                            
                            valid_changes = []
                            invalid_names = []
                            
                            for url_key, is_excluded in new_status.items():
                                if url_key in original_status and original_status[url_key] != is_excluded:
                                    
                                    # [수정 3] URL에서 종목코드 추출 (시장별 분기)
                                    if current_market.value == "KR":
                                        # 네이버 URL: ...?code=005930
                                        real_code = url_key.split("code=")[-1]
                                    else:
                                        # Perplexity URL: .../AAPL
                                        real_code = url_key.split("/")[-1]
                                    
                                    target_candidate_status = not bool(is_excluded)
                                    item_name = name_map.get(url_key, real_code)
                                    
                                    if real_code in user_target_codes and is_excluded:
                                        invalid_names.append(item_name)
                                    else:
                                        valid_changes.append((real_code, target_candidate_status, item_name))
                            
                            if valid_changes or invalid_names:
                                confirm_save_dialog(valid_changes, invalid_names, target_date, current_market.value)
                            else:
                                st.info("변경 사항이 없습니다.")
                        else:
                            st.info("변경 사항이 없습니다.")
            else:
                st.info(f"{current_market.value} 매수 후보 종목이 없습니다.")

    except Exception as e:
        st.error(f"매수 후보 조회 오류: {e}")

# [1] 지수 데이터 가져오기
@st.cache_data(ttl=60) # 1분 캐시 (네이버는 실시간이므로 짧게)
def fetch_market_indices(market_type):
    """
    주요 지수 및 환율 데이터 수집 (네이버 크롤링 오류 수정 버전)
    """
    data = {}
    
    # ---------------------------------------------------------
    # [1] 환율 (USD/KRW) - yfinance 사용
    # ---------------------------------------------------------
    try:
        df = yf.download("KRW=X", period="5d", progress=False)
        if not df.empty and len(df) >= 2:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            curr = float(df['Close'].iloc[-1])
            prev = float(df['Close'].iloc[-2])
            chg = curr - prev
            pct = (chg / prev) * 100
            
            data["USD/KRW"] = {
                "current": curr,
                "change": chg,
                "pct_change": pct
            }
    except Exception as e:
        print(f"Exchange rate fetch error: {e}")

    # ---------------------------------------------------------
    # [2] 시장별 지수 데이터 수집
    # ---------------------------------------------------------
    if market_type == MarketType.KR:
        # === 한국 시장: 네이버 증권 크롤링 (오류 수정됨) ===
        targets = {
            "KOSPI": "KOSPI",
            "KOSDAQ": "KOSDAQ"
        }
        order = ["KOSPI", "KOSDAQ", "USD/KRW"]
        
        for name, code in targets.items():
            try:
                url = f"https://finance.naver.com/sise/sise_index.naver?code={code}"
                res = requests.get(url)
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # 1. 현재가 (쉼표 제거 후 변환)
                now_val = soup.select_one('#now_value').text.replace(',', '')
                current = float(now_val)
                
                # 2. 변동폭 및 등락률 파싱 (정규식 사용으로 안전하게 처리)
                # 원본 텍스트 예: "12.34 1.20%상승" 처럼 섞여 있을 수 있음
                change_area_text = soup.select_one('#change_value_and_rate').text
                
                # 1. 정규식 수정: 숫자 앞의 마이너스(-) 기호까지 포함하여 추출
                # 수정 전: r'\d+\.?\d*' -> 수정 후: r'-?\d+\.?\d*'
                numbers = re.findall(r'-?\d+\.?\d*', change_area_text)

                if len(numbers) >= 2:
                    change_amt = float(numbers[0]) # 변동폭
                    pct_str = float(numbers[1])    # 등락률
                else:
                    change_amt = 0.0
                    pct_str = 0.0

                # 2. 하락 여부 판단 로직 강화
                # 기존: HTML 클래스(.down)나 '하락' 텍스트만 확인
                # 수정: 추출한 숫자(pct_str) 자체가 음수(-)인 경우도 하락으로 포함

                is_down_class = soup.select_one('.down') 
                parent_down_class = soup.select_one('#change_value_and_rate.rate_down')
                is_text_down = '하락' in change_area_text
                is_number_negative = (change_amt < 0) or (pct_str < 0) # 정규식으로 뽑은 값이 음수인지 확인

                # 3. 최종 부호 결정
                # 하나라도 하락 신호가 있거나, 이미 숫자가 음수라면 최종 값을 음수로 통일
                if is_down_class or parent_down_class or is_text_down or is_number_negative:
                    change_amt = -abs(change_amt)
                    pct_change = -abs(pct_str)
                else:
                    change_amt = abs(change_amt)
                    pct_change = abs(pct_str)

                data[name] = {
                    "current": current,
                    "change": change_amt,
                    "pct_change": pct_change
                }
            except Exception as e:
                # 에러 로그 자세히 출력하여 디버깅 용이하게 함
                print(f"Naver index fetch error ({name}): {e}")
                # 에러 발생 시 0.0으로 처리하거나 패스
                pass

    else:
        # === 미국 시장: yfinance 유지 ===
        tickers = {
            "NASDAQ": "^IXIC",
            "S&P 500": "^GSPC",
            "DOW Jones": "^DJI"
        }
        order = ["NASDAQ", "S&P 500", "DOW Jones", "USD/KRW"]
        
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now() + timedelta(days=1)
        
        for name, symbol in tickers.items():
            try:
                df = yf.download(symbol, start=start_date, end=end_date, progress=False)
                if not df.empty and len(df) >= 2:
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                    
                    curr = float(df['Close'].iloc[-1])
                    prev = float(df['Close'].iloc[-2])
                    chg = curr - prev
                    pct = (chg / prev) * 100
                    
                    data[name] = {
                        "current": curr,
                        "change": chg,
                        "pct_change": pct
                    }
            except Exception as e:
                print(f"US Index fetch error ({name}): {e}")

    # 정렬된 리스트 반환
    sorted_data = []
    for key in order:
        if key in data:
            sorted_data.append((key, data[key]))
            
    return sorted_data

# [2] 지수 렌더링 함수 (심플 버전)
def render_market_indices(market_type):
    """지수 및 환율 정보를 Metric으로만 표시"""
    indices = fetch_market_indices(market_type)
    
    if not indices:
        return

    st.markdown("#### 🌍 시장 주요 지수")
    
    # 데이터 개수만큼 컬럼 생성
    cols = st.columns(len(indices))
    
    for idx, (name, info) in enumerate(indices):
        with cols[idx]:
            # 환율만 소수점 2자리, 지수는 소수점 2자리 (취향에 따라 조정 가능)
            val_fmt = "{:,.2f}" 
            
            st.metric(
                label=name,
                value=val_fmt.format(info['current']),
                delta=f"{info['change']:+.2f} ({info['pct_change']:+.2f}%)"
            )