"""
자동매매 페이지 (KR/US 분리 적용 - 완성)
- 계좌 정보 표시
- 장 운영 정보 (시장별)
- 자동매매 실행 (Trader Factory 적용)
- 스케줄 설정
- 자동매매 결과 조회 (시장별 필터링)
- 실행 로그 (키 중복 방지 적용)
"""

import streamlit as st
from datetime import datetime, date
import time
import pandas as pd
from sqlalchemy import func, desc, distinct

from config.settings import get_settings_manager
from config.database import get_session, TradeHistory, Holdings, VirtualHolding, EvaluationResult, ItemMst, UserBuyTarget
from scheduler.task_manager import get_scheduler, TaskType
from core.definition import MarketType
from utils.common import custom_metric

# Trader 및 Fetcher 임포트
from impl.kr.kr_trader import KrTrader
from impl.us.us_trader import UsTrader
from impl.kr.kr_fetcher import KrFetcher
from impl.us.us_fetcher import UsFetcher

from ui.components import (
    render_account_info,
    render_market_status,
    render_log_grid,
    render_data_grid_with_paging,
    render_schedule_config,
    render_log_section,
    render_buy_candidates_table
)


def render_auto_trading():
    """자동매매 페이지 렌더링"""
    
    # 1. 현재 선택된 시장 확인
    current_market = st.session_state.get('current_market', MarketType.KR)
    market_str = current_market.value
    
    st.markdown(f'<div class="main-header">🤖 자동매매 ({market_str})</div>', unsafe_allow_html=True)
    
    settings_manager = get_settings_manager()
    settings = settings_manager.settings
    
    # ========== 계좌 정보 ==========
    # components.py의 render_account_info는 내부적으로 current_market을 세션에서 읽어 처리함
    account_type = render_account_info(settings_manager)
    
    # ========== 장 운영 정보 (시장별 처리) ==========
    market_status = render_market_status()
    
    st.divider()
    
    # ========== 매매 날짜 (오늘 고정) ==========
    today = date.today()
    st.info(f"📅 매매 날짜: **{today.strftime('%Y-%m-%d')}** (오늘)")
    
    # ========== 자동매매 설정 및 실행 ==========
    col1, col2 = st.columns([1, 1])
    
    # [설정] 시장별 TradingSettings 로드
    if current_market == MarketType.KR:
        trading_settings = settings.trading.kr
        currency = "원"
    else:
        trading_settings = settings.trading.us
        currency = "$"
    
    with col1:
        st.markdown(f"#### ⚙️ {market_str} 자동매매 설정")
        
        # 1. 설정 내용 확인 (매수/매도 분리)
        # 공간 효율을 위해 내부 컬럼으로 분리하여 표시
        info_c1, info_c2 = st.columns(2)
        
        with info_c1:
            st.markdown("**✅ 매수 설정**")
            st.caption(f"• 종목당 한도: **{trading_settings.max_buy_amount:,.0f}{currency}**")
            st.caption(f"• 매수 비중: **{trading_settings.buy_rate}%**")
            st.caption(f"• 최대 보유: **{trading_settings.limit_count}종목**")
            
            criteria_map = {"current": "현재가", "pvt": "피벗", "pvt_sup1": "1차지지", "pvt_sup2": "2차지지", "pvt_avg": "피벗평균"}
            crit = getattr(trading_settings, 'buy_price_criteria', 'current')
            st.caption(f"• 매수 기준: **{criteria_map.get(crit, crit)}**")

        with info_c2:
            st.markdown("**✅ 매도 설정**")
            st.caption(f"• 익절 목표: **+{trading_settings.sell_up_rate}%**")
            st.caption(f"• 손절 기준: **{trading_settings.sell_down_rate}%**")
            
            # 트레일링 스탑 표시 로직
            if trading_settings.trailing_stop_enabled:
                st.caption(f"• T.Stop: **{trading_settings.trailing_stop_rate}%** (사용)")
            else:
                st.caption(f"• T.Stop: **미사용**")
                
            # 분할 매도 표시 (속성이 있는 경우)
            split_rate = getattr(trading_settings, 'sell_split_rate', 100.0)
            st.caption(f"• 처분 비중: **{split_rate}%**")
        
        # 체크박스를 아래로 배치
        buy_enabled = st.checkbox(
            "매수 로직 활성화 (체크 해제 시 매수 중단)",
            value=trading_settings.buy_enabled,
            key=f"at_buy_enabled_{market_str}"
        )
        
        # 저장 버튼과 시각적으로 가깝게 배치
        if st.button("💾 설정 저장 (활성화 상태 적용)", type="primary", key=f"at_save_{market_str}"):
            # 시장별 업데이트 메서드 호출
            settings_manager.update_trading(market=market_str, buy_enabled=buy_enabled)
            st.success(f"✅ [{market_str}] 설정이 저장되었습니다.")
        st.warning("⚠️ 설정 변경 후 반드시 프로그램 재시작 해야 합니다.")
                
    with col2:
        st.markdown("#### 🚀 실행")
        
        # 실행 조건 체크
        can_trade = True
        warnings = []
        
        if not market_status.get('is_trading_day', True):
            warnings.append("⚠️ 오늘은 휴장일입니다.")
            can_trade = False
        
        if not market_status.get('is_market_open', True):
            warnings.append("⚠️ 현재 장 운영 시간이 아닙니다.")
        
        # 실전투자 동의 여부 체크
        if current_market == MarketType.KR:
            if account_type == "real" and not settings.api.kis_real_confirmed_kr:
                warnings.append("⚠️ 실전투자 동의가 필요합니다 (설정 > API 키).")
                can_trade = False
        else:
            if account_type == "real" and not settings.api.kis_real_confirmed_us:
                warnings.append("⚠️ 실전투자 동의가 필요합니다 (설정 > API 키).")
                can_trade = False
        
        for warn in warnings:
            st.warning(warn)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # 실행 버튼
        if account_type == "real":
            st.error(f"🚨 **{market_str} 실계좌 모드** - 실제 자금으로 거래됩니다!")
            
            confirm = st.checkbox("실거래 자동매매를 실행합니다.", key=f"at_confirm_{market_str}")
            
            if st.button(
                "🚀 자동매매 실행 (1회)", 
                type="primary", 
                width="stretch", 
                key=f"at_run_{market_str}",
                disabled=not (can_trade and confirm)
            ):
                run_auto_trading_logic(account_type, current_market)
        else:
            if st.button(
                "🚀 자동매매 실행 (1회)", 
                type="primary", 
                width="stretch", 
                key=f"at_run_{market_str}",
                disabled=not can_trade
            ):
                run_auto_trading_logic(account_type, current_market)
        
        # 스케줄러 상태
        st.markdown("<br>", unsafe_allow_html=True)
        scheduler = get_scheduler()
        if scheduler and scheduler.is_running():
             if st.button("⏹️ 스케줄러 중지", key="at_stop"):
                scheduler.stop()
                st.info("스케줄러가 중지되었습니다.")
                st.rerun()
        else:
             st.info("스케줄러가 현재 중지 상태입니다.")
    
    st.divider()

    st.markdown(f"#### 🎁 금일 {market_str} 매수 후보")
    settings = settings_manager.settings
    e_settings = settings.evaluation.kr if current_market == MarketType.KR else settings.evaluation.us
    min_score = e_settings.min_total_score
    base_date = today.strftime('%Y%m%d')
    render_buy_candidates_table(base_date, min_score, current_market)

    st.divider()
    
    # ========== 스케줄 설정 ==========
    # render_schedule_config가 market_type을 지원하지 않는다면 공통으로 뜸
    # (components.py 수정이 필요할 수 있음. 현재는 기본 호출)
    render_schedule_config(
        task_type="auto_trade",
        schedule_key=f"at_schedule_{market_str}",
        default_cron="10 9 * * mon-fri" if current_market == MarketType.KR else "30 23 * * mon-fri",
        market_str=market_str
    )
    
    st.divider()
    
    # ========== 보유 종목 현황 ==========
    st.markdown(f"#### 📋 {market_str} 보유 종목 현황")
    render_holdings_summary(settings_manager, account_type, current_market)
    
    st.divider()
        
    # ========== 자동매매 결과 조회 ==========
    st.markdown(f"#### 📊 {market_str} 자동매매 결과 조회")
    render_auto_trade_history_grid(current_market)

    st.divider()

    # ========== 실행 로그 ==========
    # key_suffix 추가하여 키 중복 방지
    render_log_section("auto_trade", f"📜 {market_str} 최근 실행 로그", key_suffix=market_str)
    

def run_auto_trading_logic(account_type: str, current_market: MarketType):
    """자동매매 로직 실행 (시장별 분기)"""
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    log_container = st.container()
    log_area = log_container.empty()
    log_messages = []
    
    def update_log(message):
        log_messages.append(message)
        display_logs = log_messages[-20:]
        log_area.text_area(
            "실행 로그",
            value="\n".join(display_logs),
            height=300,
            key=f"at_log_{len(log_messages)}"
        )
    
    try:
        status_text.text(f"{current_market.value} 자동매매 로직 시작...")
        update_log(f"[시작] 자동매매 로직 실행 ({current_market.value} / {account_type} 모드)")
        
        progress_bar.progress(10)
        
        # 시장에 맞는 Trader 생성
        trader = None
        if current_market == MarketType.KR:
            trader = KrTrader()
        else:
            trader = UsTrader()
        
        update_log("[준비] 계좌 잔고 조회 및 매매 조건 확인...")
        progress_bar.progress(30)
        
        # 실행
        result_log = trader.run()
        
        # 로그 출력
        for line in result_log.split('\n'):
            update_log(f"> {line}")
            time.sleep(0.05)
            
        progress_bar.progress(100)
        status_text.text("✅ 완료")
        st.success(f"{current_market.value} 자동매매 로직이 완료되었습니다.")
        update_log("[종료] 로직 실행 종료")
        
    except Exception as e:
        progress_bar.progress(100)
        status_text.text(f"❌ 오류 발생")
        st.error(f"자동매매 오류: {e}")
        update_log(f"[오류] {e}")


def render_holdings_summary(settings_manager, account_type, current_market):
    """보유 종목 현황 (링크 및 포맷팅 적용)"""
    
    data = []
    total_buy_amount = 0
    total_eval_amount = 0
    
    # [설정] 시장별 변수 설정 (통화, URL 패턴, 가격 포맷)
    if current_market == MarketType.KR:
        currency = "원"
        # 네이버 증권: code=숫자 추출
        link_regex = r"code=(\d+)"
        # KR 포맷: 1,000원
        price_format = "%d원"
    else:
        currency = "$"
        # Perplexity: finance/문자 추출
        link_regex = r"finance/(.*)"
        # US 포맷: $100.00
        price_format = "$%.2f"
    
    # ---------------------------------------------------------
    # 1. 시뮬레이션 모드: DB 조회
    # ---------------------------------------------------------
    if account_type == "simulation":
        try:
            with get_session() as session:
                holdings = session.query(VirtualHolding).filter(
                    VirtualHolding.quantity > 0,
                    VirtualHolding.market_type == current_market.value
                ).all()
                
                # 하위 호환성 (KR 구 데이터)
                if not holdings and current_market == MarketType.KR:
                    holdings = session.query(Holdings).filter(Holdings.quantity > 0).all()
                
                # Fetcher 선택
                fetcher = KrFetcher() if current_market == MarketType.KR else UsFetcher()

                if holdings:
                    for h in holdings:
                        price_data = fetcher.get_current_price(h.item_cd)
                        current_price = price_data['price'] if price_data else h.avg_price
                        
                        eval_amount = current_price * h.quantity
                        buy_amount = h.avg_price * h.quantity
                        profit_rate = ((current_price - h.avg_price) / h.avg_price * 100) if h.avg_price > 0 else 0
                        
                        total_buy_amount += buy_amount
                        total_eval_amount += eval_amount
                        
                        # [링크 생성]
                        if current_market == MarketType.KR:
                            full_url = f"https://finance.naver.com/item/main.naver?code={h.item_cd}"
                        else:
                            full_url = f"https://www.perplexity.ai/finance/{h.item_cd}"
                        
                        data.append({
                            "종목코드": full_url, # URL 저장
                            "종목명": h.item_nm or h.item_cd,
                            "수량": h.quantity,
                            "평균단가": h.avg_price,
                            "현재가": current_price,
                            "평가금액": eval_amount,
                            "수익률": profit_rate
                        })
        except Exception as e:
            st.error(f"보유 종목 조회 오류 (DB): {e}")

    # ---------------------------------------------------------
    # 2. 실전/모의투자 모드: API 조회
    # ---------------------------------------------------------
    else:
        try:
            settings = settings_manager.settings
            api_settings = settings.api
            
            acc_mode = "real" if account_type == "real" else "mock"
            
            if current_market == MarketType.KR:
                account_no = api_settings.kis_real_account_no_kr if acc_mode == "real" else api_settings.kis_mock_account_no_kr
                account_cd = api_settings.kis_real_account_cd_kr if acc_mode == "real" else api_settings.kis_mock_account_cd_kr
                api_mode = "real" if acc_mode == "real" else "mock"
                fetcher = KrFetcher(mode=api_mode)
            else:
                account_no = api_settings.kis_real_account_no_us if acc_mode == "real" else api_settings.kis_mock_account_no_us
                account_cd = api_settings.kis_real_account_cd_us if acc_mode == "real" else api_settings.kis_mock_account_cd_us
                api_mode = "real" if acc_mode == "real" else "mock"
                fetcher = UsFetcher(mode=api_mode)
            
            if account_no and account_cd:
                balance_info = fetcher.get_account_balance(account_no, account_cd)
                
                if balance_info and 'holdings' in balance_info:
                    for h in balance_info['holdings']:
                        code = h.get('pdno')
                        qty = int(h.get('hldg_qty', 0))
                        avg_price = float(h.get('pchs_avg_pric', 0))
                        cur_price = float(h.get('prpr', 0))
                        eval_amt = float(h.get('evlu_amt', 0))
                        
                        if eval_amt == 0 and qty > 0:
                            eval_amt = cur_price * qty
                            
                        buy_amt = avg_price * qty
                        
                        total_buy_amount += buy_amt
                        total_eval_amount += eval_amt
                        
                        # [링크 생성]
                        if current_market == MarketType.KR:
                            full_url = f"https://finance.naver.com/item/main.naver?code={code}"
                        else:
                            full_url = f"https://www.perplexity.ai/finance/{code}"
                        
                        data.append({
                            "종목코드": full_url, # URL 저장
                            "종목명": h.get('prdt_name'),
                            "수량": qty,
                            "평균단가": avg_price,
                            "현재가": cur_price,
                            "평가금액": eval_amt,
                            "수익률": float(h.get('evlu_pfls_rt', 0))
                        })
        except Exception as e:
            st.error(f"보유 종목 조회 오류 (API): {e}")

    # ---------------------------------------------------------
    # 3. 화면 출력
    # ---------------------------------------------------------
    if data:
        # 상단 요약 지표
        total_profit_rate = ((total_eval_amount - total_buy_amount) / total_buy_amount * 100) if total_buy_amount > 0 else 0
        
        c1, c2, c3, c4 = st.columns(4)
        with c1: custom_metric("보유 종목수", f"{len(data)}개")
        with c2: custom_metric("총 매입금액", f"{total_buy_amount:,.0f}{currency}")
        with c3: custom_metric("총 평가금액", f"{total_eval_amount:,.0f}{currency}")
        with c4:
            color = "#ef4444" if total_profit_rate > 0 else "#3b82f6"
            if total_profit_rate == 0: color = "inherit"
            custom_metric("총 수익률", f"{total_profit_rate:+.2f}%", value_color=color)
                
        # 데이터프레임 (링크 컬럼 적용)
        st.dataframe(
            data,
            width="stretch",
            hide_index=True,
            column_config={
                "종목코드": st.column_config.LinkColumn(
                    "종목코드", 
                    display_text=link_regex,
                    help="클릭 시 상세 정보 페이지로 이동합니다."
                ),
                "종목명": st.column_config.TextColumn("종목명"),
                "수량": st.column_config.NumberColumn(
                    "수량", 
                    format="%d"
                ),
                "평균단가": st.column_config.NumberColumn(
                    "평균단가", 
                    format=price_format
                ),
                "현재가": st.column_config.NumberColumn(
                    "현재가", 
                    format=price_format
                ),
                "평가금액": st.column_config.NumberColumn(
                    "평가금액", 
                    format=price_format
                ),
                "수익률": st.column_config.NumberColumn(
                    "수익률", 
                    format="%.2f%%"
                )
            }
        )
    else:
        st.info("보유 종목이 없습니다.")

def render_auto_trade_history_grid(current_market):
    """자동매매 결과 그리드 (시장별 필터링)"""
    col1, col2 = st.columns([1, 3])
    
    with col1:
        selected_date = st.date_input(
            "조회 날짜",
            value=date.today(),
            max_value=date.today(),
            key=f"at_query_date_{current_market.value}"
        )
    
    currency = "원" if current_market == MarketType.KR else "$"

    try:
        with get_session() as session:
            date_str = selected_date.strftime('%Y%m%d')
            
            # market_type 필터 추가
            query = session.query(TradeHistory, ItemMst.itms_nm).outerjoin(
                ItemMst, TradeHistory.item_cd == ItemMst.item_cd
            ).filter(
                TradeHistory.trade_date == date_str,
                TradeHistory.trade_source == 'auto',
                TradeHistory.market_type == current_market.value
            ).order_by(TradeHistory.created_at.desc()).all()
            
            data = []
            for row, item_name in query:
                trade_type_kr = "매수" if row.trade_type in ['buy', 'B'] else "매도"
                color = '🟢' if row.trade_type in ['buy', 'B'] else '🔴'
                
                data.append({
                    "시간": row.trade_time[:4] if row.trade_time else "",
                    "종류": f"{color} {trade_type_kr}",
                    "종목코드": row.item_cd,
                    "종목명": item_name or row.item_cd,
                    "수량": f"{row.quantity:,}",
                    "단가": f"{int(row.price):,}{currency}" if current_market == MarketType.KR else f"{row.price:,.2f}{currency}",
                    "금액": f"{int(row.amount):,}{currency}" if current_market == MarketType.KR else f"{row.amount:,.2f}{currency}",
                    "사유": row.trade_reason or row.rmk
                })
            
            if data:
                st.markdown(f"**조회 결과: {len(data)}건**")
                render_data_grid_with_paging(
                    data=data,
                    columns=["시간", "종류", "종목코드", "종목명", "수량", "단가", "금액", "사유"],
                    page_size=20,
                    key_prefix=f"at_history_{current_market.value}"
                )
            else:
                st.info(f"{selected_date} 날짜의 {current_market.value} 자동매매 기록이 없습니다.")
                
    except Exception as e:
        st.error(f"데이터 조회 오류: {e}")