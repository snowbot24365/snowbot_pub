"""
수동매매 페이지 (KR/US 분리 적용)
- 계좌 정보 표시 (시장별)
- 수동 매수/매도 (시장별 종목 및 통화 적용)
- 매매 결과 조회 (시장별 필터링)
- 실행 로그
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date
import time
from sqlalchemy import func

from config.settings import get_settings_manager
from config.database import get_session, TradeHistory, Holdings, VirtualHolding, ScheduleLog, EvaluationResult, ItemMst, ItemPrice, UserBuyTarget
from core.definition import MarketType
from impl.kr.kr_fetcher import KrFetcher
from impl.us.us_fetcher import UsFetcher
from utils.common import custom_metric
from ui.components import (
    render_account_info, 
    render_market_status, 
    render_log_grid, 
    render_data_grid_with_paging,
    render_log_section
)


def render_manual_trading():
    """수동매매 페이지 렌더링"""
    
    # 1. 현재 선택된 시장 확인
    current_market = st.session_state.get('current_market', MarketType.KR)
    market_str = current_market.value

    # 'mt_last_market' 키에 마지막으로 조회한 시장 정보를 저장해두고, 
    # 현재 시장과 다르면 매수/매도 관련 캐시 데이터를 모두 삭제합니다.
    if st.session_state.get('mt_last_market') != current_market:
        # 삭제할 세션 키 목록 (매수/매도 데이터프레임, 선택된 종목 등)
        keys_to_clear = [
            'mt_buy_candidates_df', 'mt_buy_raw', 'mt_selected_buy_item', 
            'mt_realtime_price_cache', 'mt_buy_base_date',
            'mt_holdings_df', 'mt_holdings_raw', 'mt_selected_item'
        ]
        for k in keys_to_clear:
            if k in st.session_state:
                del st.session_state[k]
        
        # 현재 시장 상태 업데이트
        st.session_state['mt_last_market'] = current_market
        # 화면을 재실행하여 초기화된 상태로 렌더링 (데이터 새로 로딩)
        st.rerun()
    
    st.markdown(f'<div class="main-header">🖐️ 수동매매 ({market_str})</div>', unsafe_allow_html=True)
    
    settings_manager = get_settings_manager()
    
    # ========== 계좌 정보 (components.py에서 시장별 처리됨) ==========
    account_type = render_account_info(settings_manager)
    
    # ========== 장 운영 정보 ==========
    render_market_status()
    
    st.divider()
    
    # ========== 매매 날짜 ==========
    today = date.today()
    st.info(f"📅 매매 날짜: **{today.strftime('%Y-%m-%d')}** (오늘)")
    
    # ========== 매수/매도 탭 ==========
    tab1, tab2 = st.tabs(["💵 매수 (추천종목)", "💸 매도 (보유종목)"])
    
    # ========== 매수 탭 ==========
    with tab1:
        render_buy_section(settings_manager, account_type, current_market)
    
    # ========== 매도 탭 ==========
    with tab2:
        render_sell_section(settings_manager, account_type, current_market)
    
    st.divider()
    
    # ========== 실행 로그 ==========
    render_log_section("manual_trade", f"📜 {market_str} 최근 실행 로그", key_suffix=market_str)
    
    st.divider()
    
    # ========== 매매 결과 조회 ==========
    st.markdown(f"#### 📊 {market_str} 매매 결과 조회")
    
    render_trade_history_grid(current_market)


def get_buy_candidates(settings_manager, current_market):
    """
    매수 후보군 조회 (시장별 필터링)
    - UserBuyTarget 및 EvaluationResult에서 ItemMst의 mrkt_ctg를 함께 조회
    """
    try:
        with get_session() as session:
            candidates_map = {} 

            # ---------------------------------------------------------
            # 1. 사용자 지정 관심종목 조회 (UserBuyTarget - 시장 필터)
            # ---------------------------------------------------------
            # ItemMst와 조인하여 mrkt_ctg 조회
            user_targets = session.query(UserBuyTarget, ItemMst.mrkt_ctg).outerjoin(
                ItemMst, UserBuyTarget.item_cd == ItemMst.item_cd
            ).filter(
                UserBuyTarget.market_type == current_market.value
            ).all()
            
            user_codes = [] 

            for t, mrkt_ctg in user_targets:
                user_codes.append(t.item_cd)

                final_market_ctg = mrkt_ctg if mrkt_ctg else t.exch_code
                
                # 전일 종가 조회 (시장 필터)
                price_row = session.query(ItemPrice.stck_clpr).filter(
                    ItemPrice.item_cd == t.item_cd,
                    ItemPrice.market_type == current_market.value
                ).order_by(ItemPrice.trade_date.desc()).first()
                
                yesterday_close = price_row[0] if price_row else 0
                
                candidates_map[t.item_cd] = {
                    'item_cd': t.item_cd,
                    'item_nm': t.item_nm,
                    'market_ctg': final_market_ctg or "", # 시장 구분 (KOSPI, NASDAQ 등)
                    'total_score': 0, 
                    'is_candidate': False, 
                    'ref_price': yesterday_close,
                    'source': '⭐사용자'
                }

            # 관심종목 최신 평가 점수 업데이트
            if user_codes:
                subq = session.query(
                    EvaluationResult.item_cd,
                    func.max(EvaluationResult.base_date).label('max_date')
                ).filter(
                    EvaluationResult.item_cd.in_(user_codes),
                    EvaluationResult.market_type == current_market.value
                ).group_by(EvaluationResult.item_cd).subquery()

                scores = session.query(EvaluationResult).join(
                    subq,
                    (EvaluationResult.item_cd == subq.c.item_cd) & 
                    (EvaluationResult.base_date == subq.c.max_date)
                ).filter(EvaluationResult.market_type == current_market.value).all()

                for s in scores:
                    if s.item_cd in candidates_map:
                        candidates_map[s.item_cd]['total_score'] = s.total_score
                        if s.is_buy_candidate == 1:
                            candidates_map[s.item_cd]['is_candidate'] = True

            # ---------------------------------------------------------
            # 2. 알고리즘 추천 종목 조회 (Top 100 - 시장 필터)
            # ---------------------------------------------------------
            latest_date = session.query(func.max(EvaluationResult.base_date)).filter(
                EvaluationResult.market_type == current_market.value
            ).scalar()
            
            if latest_date:
                # KR/US 설정 분리
                if current_market == MarketType.KR:
                    min_score = settings_manager.settings.evaluation.kr.min_total_score
                else:
                    min_score = settings_manager.settings.evaluation.us.min_total_score
                
                # ItemMst와 조인하여 mrkt_ctg 조회
                results = session.query(EvaluationResult, ItemMst.mrkt_ctg).join(
                    ItemMst, EvaluationResult.item_cd == ItemMst.item_cd
                ).filter(
                    EvaluationResult.base_date == latest_date,
                    EvaluationResult.market_type == current_market.value,
                    EvaluationResult.total_score >= min_score,
                    EvaluationResult.is_buy_candidate == 1
                ).order_by(EvaluationResult.total_score.desc()).all()
                
                for r, mrkt_ctg in results:
                    # 가격 조회
                    price_row = session.query(ItemPrice.stck_clpr).filter(
                        ItemPrice.item_cd == r.item_cd,
                        ItemPrice.market_type == current_market.value
                    ).order_by(ItemPrice.trade_date.desc()).first()
                    yesterday_close = price_row[0] if price_row else 0
                    
                    if r.item_cd in candidates_map:
                        candidates_map[r.item_cd].update({
                            'total_score': r.total_score,
                            'is_candidate': r.is_buy_candidate,
                            'market_ctg': mrkt_ctg or "", # [업데이트]
                            'source': '⭐사용자+📊추천'
                        })
                    else:
                        candidates_map[r.item_cd] = {
                            'item_cd': r.item_cd,
                            'item_nm': r.item_nm,
                            'market_ctg': mrkt_ctg or "", # 'total_score': r.total_score,
                            'is_candidate': r.is_buy_candidate,
                            'ref_price': yesterday_close,
                            'source': '📊추천'
                        }
            
            # 리스트 변환 및 정렬
            final_list = list(candidates_map.values())
            final_list.sort(key=lambda x: (0 if '사용자' in x['source'] else 1, -x['total_score']))
            
            return final_list, latest_date
            
    except Exception as e:
        st.error(f"매수 후보 조회 오류: {e}")
        return [], None


def render_buy_section(settings_manager, account_type, current_market):
    """매수 섹션"""
    st.markdown("#### 💵 수동 매수 (추천 및 관심 종목)")
    
    currency = "원" if current_market == MarketType.KR else "$"
    price_col_name = f"전일종가 ({currency})"
    
    # 1. 데이터 조회 (DB only)
    if 'mt_buy_candidates_df' not in st.session_state:
        with st.spinner("매수 추천 종목 조회 중..."):
            candidates, base_date = get_buy_candidates(settings_manager, current_market)
            
            if candidates:
                df = pd.DataFrame(candidates)
                
                # 표시용 DF 생성
                display_df = df[['source', 'market_ctg','item_nm', 'item_cd', 'total_score', 'ref_price', 'is_candidate']].copy()
                display_df.columns = ['구분', '시장', '종목명', '종목코드', '점수', price_col_name, '매수추천']
                
                # 포맷팅
                display_df['매수추천'] = display_df['매수추천'].apply(lambda x: '✅' if x else '')

                if current_market == MarketType.KR:
                    display_df[price_col_name] = display_df[price_col_name].fillna(0).astype(int)
                else:
                    display_df[price_col_name] = display_df[price_col_name].fillna(0).astype(float)
                                
                st.session_state.mt_buy_raw = candidates
                st.session_state.mt_buy_candidates_df = display_df
                st.session_state.mt_buy_base_date = base_date
            else:
                st.session_state.mt_buy_candidates_df = None
                st.session_state.mt_buy_raw = []

    # 2. 상단 정보
    col_info, col_refresh = st.columns([3, 1])
    with col_info:
        base_date = st.session_state.get('mt_buy_base_date')
        if base_date:
            formatted_date = f"{base_date[:4]}-{base_date[4:6]}-{base_date[6:]}"
            st.caption(f"평가 기준일: {formatted_date} ({current_market.value})")
            
    with col_refresh:
        if st.button("🔄 목록 새로고침", key="mt_refresh_buy", width="stretch"):
            for k in ['mt_buy_candidates_df', 'mt_buy_raw', 'mt_selected_buy_item', 'mt_realtime_price_cache']:
                if k in st.session_state: del st.session_state[k]
            st.rerun()

    df_display = st.session_state.mt_buy_candidates_df
    
    if df_display is None or df_display.empty:
        st.info("매수 추천 종목이 없습니다.")
        return

    st.info("👇 목록에서 종목을 선택하면 **실시간 시세**를 조회하여 주문창을 띄웁니다.")

    fmt_str = "{:,}" if current_market == MarketType.KR else "{:,.2f}"
    
    # Styler 객체 생성
    styled_df = df_display.style.format({
        price_col_name: fmt_str
    })

    # 3. 그리드 표시
    event = st.dataframe(
        styled_df,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "구분": st.column_config.TextColumn("구분"),
            "점수": st.column_config.NumberColumn(format="%d점"),
            price_col_name: st.column_config.NumberColumn(price_col_name),
        },
        key="mt_buy_grid"
    )

    # 4. 선택 시 실시간 가격 조회
    selected_item = None
    
    if event.selection.rows:
        selected_idx = event.selection.rows[0]
        
        if 'mt_buy_raw' in st.session_state:
            raw_item = st.session_state.mt_buy_raw[selected_idx]
            
            prev_selected = st.session_state.get('mt_selected_buy_item')
            cached_price = st.session_state.get('mt_realtime_price_cache', 0)
            
            if not prev_selected or prev_selected['item_cd'] != raw_item['item_cd'] or cached_price == 0:
                with st.spinner(f"📡 '{raw_item['item_nm']}' 실시간 시세 조회 중..."):
                    realtime_price = 0
                    try:
                        # 실행 모드 확인 (KR/US 별도)
                        if current_market == MarketType.KR:
                            api_mode = "real" if account_type == "real" else "mock"
                            fetcher = KrFetcher(mode=api_mode)
                        else:
                            # 미국 모드 확인
                            s = settings_manager.settings.api
                            api_mode = "real" if account_type == "real" else "mock"
                            fetcher = UsFetcher(mode=api_mode)
                            
                        stock_info = fetcher.get_current_price(raw_item['item_cd'])
                        
                        if stock_info:
                            realtime_price = stock_info.get('price', 0)
                    except Exception:
                        pass
                    
                    if realtime_price == 0:
                        realtime_price = raw_item['ref_price']
                        
                    st.session_state.mt_realtime_price_cache = realtime_price
                    st.session_state.mt_selected_buy_item = raw_item
            
            selected_item = st.session_state.mt_selected_buy_item

    elif 'mt_selected_buy_item' in st.session_state:
        del st.session_state.mt_selected_buy_item
        if 'mt_realtime_price_cache' in st.session_state:
            del st.session_state.mt_realtime_price_cache
        selected_item = None

    # 5. 매수 주문 UI
    if selected_item:
        st.divider()
        st.markdown(f"##### 📈 매수 주문: **{selected_item['item_nm']}** ({selected_item['item_cd']})")
        st.caption(f"구분: {selected_item['source']}")

        realtime_price = st.session_state.get('mt_realtime_price_cache', selected_item['ref_price'])
        yesterday_price = selected_item['ref_price']
        
        # 등락 계산
        if yesterday_price > 0:
            diff = realtime_price - yesterday_price
            diff_rate = (diff / yesterday_price) * 100
            diff_color = "red" if diff > 0 else "blue" if diff < 0 else "black"
            
            if current_market == MarketType.KR:
                diff_str = f"{diff:+,.0f}{currency} ({diff_rate:+.2f}%)"
                price_fmt = f"{realtime_price:,}{currency}"
                prev_fmt = f"{yesterday_price:,}{currency}"
            else:
                diff_str = f"{diff:+,.2f}{currency} ({diff_rate:+.2f}%)"
                price_fmt = f"{realtime_price:,.2f}{currency}"
                prev_fmt = f"{yesterday_price:,.2f}{currency}"
        else:
            diff_str = "-"
            diff_color = ""
            price_fmt = f"{realtime_price:,}{currency}" if current_market == MarketType.KR else f"{realtime_price:,.2f}{currency}"
            prev_fmt = "-"

        with st.container(border=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                custom_metric("전일종가", prev_fmt)
            with c2:
                custom_metric("현재가(실시간)", price_fmt)
            if diff_color:
                c3.markdown(f"전일대비: :{diff_color}[{diff_str}]")

            col1, col2 = st.columns(2)
            u_key = f"buy_{selected_item['item_cd']}"
            
            with col1:
                buy_quantity = st.number_input(
                    "매수 수량",
                    min_value=1, max_value=100000, value=1,
                    key=f"qty_{u_key}"
                )
            
            with col2:
                # 시장 타입에 따라 데이터 타입(int/float)과 포맷을 일치시킴
                if current_market == MarketType.KR:
                    # [KR] 정수 설정 (%d) -> 모든 값을 int로 변환
                    step = 100
                    fmt = "%d"
                    val = int(realtime_price)      # int 변환
                    min_v = 0                      # int
                    max_v = 10000000               # int
                else:
                    # [US] 실수 설정 (%.2f) -> 모든 값을 float로 변환
                    step = 0.01                    # 보통 미국 주식 호가는 0.01달러 단위가 많음
                    fmt = "%.2f"
                    val = float(realtime_price)    # float 변환
                    min_v = 0.0                    # float
                    max_v = 10000000.0             # float
                
                buy_price = st.number_input(
                    f"매수 가격 (0=시장가) [{currency}]",
                    min_value=min_v, 
                    max_value=max_v, 
                    value=val, 
                    step=step,     # step의 타입도 value와 일치해야 함
                    format=fmt,
                    key=f"price_{u_key}"
                )
            
            if buy_price > 0:
                total = buy_price * buy_quantity
                if current_market == MarketType.KR:
                    st.info(f"💰 예상 매수금액: **{total:,}{currency}**")
                else:
                    st.info(f"💰 예상 매수금액: **{total:,.2f}{currency}**")
            else:
                st.info("💰 **시장가** 매수")

            if st.button("💵 매수 주문 실행", type="primary", width="stretch", key=f"btn_{u_key}"):
                 execute_buy_order(
                    settings_manager=settings_manager,
                    stock_code=selected_item['item_cd'],
                    quantity=buy_quantity,
                    price=buy_price,
                    account_type=account_type,
                    current_market=current_market
                )


def render_sell_section(settings_manager, account_type, current_market):
    """매도 섹션"""
    st.markdown("#### 💸 수동 매도 (보유 종목)")
    
    currency = "원" if current_market == MarketType.KR else "$"
    
    # 1. 데이터 조회
    if 'mt_holdings_df' not in st.session_state:
        with st.spinner("보유 종목 조회 중..."):
            holdings = get_holdings(settings_manager, account_type, current_market)
            if holdings:
                df = pd.DataFrame(holdings)
                
                # 표시 포맷팅
                display_df = df[['item_nm', 'item_cd', 'quantity', 'profit_rate', 'current_price', 'avg_price']].copy()
                display_df.columns = ['종목명', '종목코드', '보유수량', '수익률', '현재가', '매입가']
                
                # 가격 포맷팅
                if current_market == MarketType.KR:
                    display_df['현재가'] = display_df['현재가'].apply(lambda x: f"{int(x):,}")
                    display_df['매입가'] = display_df['매입가'].apply(lambda x: f"{int(x):,}")
                else:
                    display_df['현재가'] = display_df['현재가'].apply(lambda x: f"{x:,.2f}")
                    display_df['매입가'] = display_df['매입가'].apply(lambda x: f"{x:,.2f}")
                
                st.session_state.mt_holdings_raw = holdings 
                st.session_state.mt_holdings_df = display_df
            else:
                st.session_state.mt_holdings_df = None
                st.session_state.mt_holdings_raw = []

    # 2. 새로고침 버튼
    col_refresh, _ = st.columns([1, 4])
    with col_refresh:
        if st.button("🔄 목록 새로고침", key="mt_refresh_holdings"):
            for k in ['mt_holdings_df', 'mt_holdings_raw', 'mt_selected_item']:
                if k in st.session_state: del st.session_state[k]
            st.rerun()
    
    df_display = st.session_state.mt_holdings_df
    
    if df_display is None or df_display.empty:
        st.info("보유 종목이 없습니다.")
        return

    st.info("👇 목록에서 종목을 선택(체크)하면 아래에 매도 주문창이 표시됩니다.")

    # 3. 그리드 표시
    event = st.dataframe(
        df_display,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "수익률": st.column_config.NumberColumn(format="%.2f%%"),
            "보유수량": st.column_config.NumberColumn(format="%d주"),
        },
        key="mt_holdings_grid"
    )
    
    # 4. 선택된 종목 파악
    selected_item = None
    if event.selection.rows:
        selected_idx = event.selection.rows[0]
        if 'mt_holdings_raw' in st.session_state and len(st.session_state.mt_holdings_raw) > selected_idx:
            selected_item = st.session_state.mt_holdings_raw[selected_idx]
            st.session_state.mt_selected_item = selected_item
            
    elif 'mt_selected_item' in st.session_state:
        del st.session_state.mt_selected_item
        selected_item = None

    # 5. 매도 주문 UI
    if selected_item:
        st.divider()
        st.markdown(f"##### 📉 매도 주문: **{selected_item['item_nm']}** ({selected_item['item_cd']})")
        
        cur_fmt = f"{selected_item['current_price']:,.0f}{currency}" if current_market == MarketType.KR else f"{selected_item['current_price']:,.2f}{currency}"
        
        with st.container(border=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                custom_metric("보유수량", f"{selected_item['quantity']:,}주")
            with c2:
                custom_metric("현재가", cur_fmt)
            profit_color = "red" if selected_item['profit_rate'] > 0 else "blue"
            c3.markdown(f"수익률: :{profit_color}[{selected_item['profit_rate']:+.2f}%]")
            
            col1, col2 = st.columns(2)
            u_key = selected_item['item_cd']
            
            with col1:
                sell_quantity = st.number_input(
                    "매도 수량",
                    min_value=1,
                    max_value=selected_item['quantity'],
                    value=selected_item['quantity'],
                    key=f"mt_sell_qty_{u_key}"
                )
            
            with col2:
                # 시장 타입에 따라 데이터 타입(int/float)과 포맷을 일치시킴
                if current_market == MarketType.KR:
                    # [KR] 정수 설정 (%d) -> 값들도 모두 int여야 함
                    step = 100
                    fmt = "%d"
                    val = 0                # int (기존 0.0 수정)
                    min_v = 0              # int (기존 0.0 수정)
                    max_v = 10000000       # int (기존 10000000.0 수정)
                else:
                    # [US] 실수 설정 (%.2f) -> 값들도 모두 float여야 함
                    step = 0.01
                    fmt = "%.2f"
                    val = 0.0              # float
                    min_v = 0.0            # float
                    max_v = 10000000.0     # float
                
                sell_price = st.number_input(
                    f"매도가격 (0=시장가) [{currency}]",
                    min_value=min_v,
                    max_value=max_v,
                    value=val,
                    step=step,
                    format=fmt,
                    key=f"mt_sell_price_{u_key}"
                )
            
            if sell_price > 0:
                total = sell_price * sell_quantity
                if current_market == MarketType.KR:
                    st.info(f"💰 예상 매도금액: **{total:,}{currency}**")
                else:
                    st.info(f"💰 예상 매도금액: **{total:,.2f}{currency}**")
            else:
                st.info("💰 **시장가** 매도")
            
            b1, b2 = st.columns(2)
            with b1:
                if st.button("💸 매도 주문 실행", type="primary", width="stretch", key=f"btn_sell_{u_key}"):
                    success = execute_sell_order(
                        settings_manager=settings_manager,
                        stock_code=selected_item['item_cd'],
                        quantity=sell_quantity,
                        price=sell_price,
                        account_type=account_type,
                        current_market=current_market
                    )
                    if success:
                        for k in ['mt_holdings_df', 'mt_holdings_raw', 'mt_selected_item']:
                            if k in st.session_state: del st.session_state[k]
                        time.sleep(0.5)
                        st.rerun()
                        
            with b2:
                if st.button("🔄 전량 시장가 매도", width="stretch", key=f"btn_all_{u_key}"):
                    success = execute_sell_order(
                        settings_manager=settings_manager,
                        stock_code=selected_item['item_cd'],
                        quantity=selected_item['quantity'],
                        price=0,
                        account_type=account_type,
                        current_market=current_market
                    )
                    if success:
                        for k in ['mt_holdings_df', 'mt_holdings_raw', 'mt_selected_item']:
                            if k in st.session_state: del st.session_state[k]
                        time.sleep(0.5)
                        st.rerun()


def get_holdings(settings_manager, account_type, current_market):
    """보유 종목 조회 (시장별 분기)"""
    
    # 1. 시뮬레이션: DB 조회
    if account_type == "simulation":
        try:
            with get_session() as session:
                # 시장 필터 적용
                holdings = session.query(VirtualHolding).filter(
                    VirtualHolding.quantity > 0,
                    VirtualHolding.market_type == current_market.value
                ).all()
                
                # Fetcher
                fetcher = KrFetcher() if current_market == MarketType.KR else UsFetcher()
                
                result = []
                for h in holdings:
                    price_data = fetcher.get_current_price(h.item_cd)
                    current_price = price_data['price'] if price_data else h.avg_price
                    
                    profit_rate = 0.0
                    if h.avg_price > 0:
                        profit_rate = ((current_price - h.avg_price) / h.avg_price) * 100
                    
                    result.append({
                        'item_cd': h.item_cd,
                        'item_nm': h.item_nm or h.item_cd,
                        'quantity': h.quantity,
                        'avg_price': h.avg_price,
                        'current_price': current_price,
                        'profit_rate': profit_rate
                    })
                return result
        except:
            return []
            
    # 2. 실전/모의: API 조회
    else:
        try:
            settings = settings_manager.settings
            api = settings.api
            
            api_mode = "real" if account_type == "real" else "mock"
            if current_market == MarketType.KR:
                acct_no = api.kis_real_account_no_kr if api_mode == "real" else api.kis_mock_account_no_kr
                acct_cd = api.kis_real_account_cd_kr if api_mode == "real" else api.kis_mock_account_cd_kr
                fetcher = KrFetcher(mode=api_mode)
            else:
                acct_no = api.kis_real_account_no_us if api_mode == "real" else api.kis_mock_account_no_us
                acct_cd = api.kis_real_account_cd_us if api_mode == "real" else api.kis_mock_account_cd_us
                fetcher = UsFetcher(mode=api_mode)
            
            if not acct_no or not acct_cd: return []
                
            balance = fetcher.get_account_balance(acct_no, acct_cd)
            
            result = []
            if balance and 'holdings' in balance:
                for h in balance['holdings']:
                    # Fetcher 표준 키 사용
                    qty = int(h.get('hldg_qty', 0))
                    if qty > 0:
                        avg = float(h.get('pchs_avg_pric', 0))
                        cur = float(h.get('prpr', 0))
                        rate = float(h.get('evlu_pfls_rt', 0))
                        
                        result.append({
                            'item_cd': h.get('pdno', ''),
                            'item_nm': h.get('prdt_name', ''),
                            'quantity': qty,
                            'avg_price': avg,
                            'current_price': cur,
                            'profit_rate': rate
                        })
            return result
        except Exception as e:
            st.error(f"API 조회 오류: {e}")
            return []


def log_manual_trade(message: str, status: str = "success", error_msg: str = None, market_type: str = "KR"):
    """수동 매매 로그 DB 저장"""
    try:
        with get_session() as session:
            log = ScheduleLog(
                schedule_id=f"manual_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                schedule_name="수동매매",
                task_type="manual_trade",
                market_type=market_type, # 시장 정보 기록
                status=status,
                start_time=datetime.now(),
                end_time=datetime.now(),
                message=message,
                error_message=error_msg
            )
            session.add(log)
            session.commit()
    except Exception as e:
        print(f"로그 저장 실패: {e}")


def execute_buy_order(settings_manager, stock_code: str, quantity: int, price: float, account_type: str, current_market: MarketType):
    """매수 주문 실행"""
    order_type = "시장가" if price == 0 else "지정가"
    market_str = current_market.value
    st.info(f"{market_str} 매수 주문 전송 중... ({order_type})")
    
    # 1. 시뮬레이션
    if account_type == "simulation":
        # 시뮬레이터에 시장 정보 전달
        from trading.simulator import SimulationEngine
        engine = SimulationEngine(market_type=market_str)
        result = engine.buy(stock_code, quantity, price)
        
        if result.success:
            st.success(f"✅ 매수 완료: {stock_code} {quantity}주 @ {result.price:,.2f}")
            log_manual_trade(f"[매수] {stock_code} {quantity}주 @ {result.price} (Sim)", market_type=market_str)
        else:
            st.error(f"❌ 매수 실패: {result.message}")
            log_manual_trade(f"[매수실패] {stock_code} - {result.message}", "failed", result.message, market_type=market_str)
            
    # 2. 실전/모의투자 (API)
    else:
        settings = settings_manager.settings
        api = settings.api
        
        # 시장별 API 설정 로드
        if current_market == MarketType.KR:
            api_mode = "real" if account_type == "real" else "mock"
            acct_no = api.kis_real_account_no_kr if api_mode == "real" else api.kis_mock_account_no_kr
            acct_cd = api.kis_real_account_cd_kr if api_mode == "real" else api.kis_mock_account_cd_kr
            fetcher = KrFetcher(mode=api_mode)
        else:
            api_mode = "real" if account_type == "real" else "mock"
            acct_no = api.kis_real_account_no_us if api_mode == "real" else api.kis_mock_account_no_us
            acct_cd = api.kis_real_account_cd_us if api_mode == "real" else api.kis_mock_account_cd_us
            fetcher = UsFetcher(mode=api_mode)
            
        if not acct_no or not acct_cd:
            st.error("계좌 정보가 설정되지 않았습니다.")
            return

        # 주문 전송
        res = fetcher.send_order('buy', stock_code, quantity, price, acct_no, acct_cd)
        
        if res['success']:
            st.success(f"✅ 주문 전송 완료 (주문번호: {res.get('order_no', '-')})")
            
            # 기록용 가격
            record_price = price
            if record_price == 0:
                info = fetcher.get_current_price(stock_code)
                if info: record_price = info.get('price', 0)
            
            save_trade_history(stock_code, 'buy', quantity, record_price, date.today(), current_market)
            log_manual_trade(f"[매수] {stock_code} {quantity}주 ({order_type}) API", market_type=market_str)
        else:
            st.error(f"❌ 주문 실패: {res['message']}")
            log_manual_trade(f"[매수실패] {stock_code} - {res['message']}", "failed", res['message'], market_type=market_str)


def execute_sell_order(settings_manager, stock_code: str, quantity: int, price: float, account_type: str, current_market: MarketType) -> bool:
    """매도 주문 실행"""
    order_type = "시장가" if price == 0 else "지정가"
    market_str = current_market.value
    st.info(f"{market_str} 매도 주문 전송 중... ({order_type})")
    
    # 1. 시뮬레이션
    if account_type == "simulation":
        from trading.simulator import SimulationEngine
        engine = SimulationEngine(market_type=market_str)
        result = engine.sell(stock_code, quantity, price)
        
        if result.success:
            st.success(f"✅ 매도 완료: {stock_code} {quantity}주 @ {result.price:,.2f}")
            log_manual_trade(f"[매도] {stock_code} {quantity}주 @ {result.price} (Sim)", market_type=market_str)
            return True
        else:
            st.error(f"❌ 매도 실패: {result.message}")
            log_manual_trade(f"[매도실패] {stock_code} - {result.message}", "failed", result.message, market_type=market_str)
            return False
            
    # 2. 실전/모의 (API)
    else:
        settings = settings_manager.settings
        api = settings.api
        
        if current_market == MarketType.KR:
            api_mode = "real" if account_type == "real" else "mock"
            acct_no = api.kis_real_account_no_kr if api_mode == "real" else api.kis_mock_account_no_kr
            acct_cd = api.kis_real_account_cd_kr if api_mode == "real" else api.kis_mock_account_cd_kr
            fetcher = KrFetcher(mode=api_mode)
        else:
            api_mode = "real" if account_type == "real" else "mock"
            acct_no = api.kis_real_account_no_us if api_mode == "real" else api.kis_mock_account_no_us
            acct_cd = api.kis_real_account_cd_us if api_mode == "real" else api.kis_mock_account_cd_us
            fetcher = UsFetcher(mode=api_mode)
            
        if not acct_no or not acct_cd:
            st.error("계좌 정보가 설정되지 않았습니다.")
            return False

        res = fetcher.send_order('sell', stock_code, quantity, price, acct_no, acct_cd)
        
        if res['success']:
            st.success(f"✅ 주문 전송 완료 (주문번호: {res.get('order_no', '-')})")
            
            record_price = price
            if record_price == 0:
                info = fetcher.get_current_price(stock_code)
                if info: record_price = info.get('price', 0)
            
            save_trade_history(stock_code, 'sell', quantity, record_price, date.today(), current_market)
            log_manual_trade(f"[매도] {stock_code} {quantity}주 ({order_type}) API", market_type=market_str)
            return True
        else:
            st.error(f"❌ 주문 실패: {res['message']}")
            log_manual_trade(f"[매도실패] {stock_code} - {res['message']}", "failed", res['message'], market_type=market_str)
            return False


def save_trade_history(item_cd: str, trade_type: str, quantity: int, price: float, trade_date: date, market: MarketType):
    """거래 기록 저장"""
    try:
        with get_session() as session:
            trade = TradeHistory(
                item_cd=item_cd,
                market_type=market.value, # 시장 정보 저장
                trade_type=trade_type,
                quantity=quantity,
                price=price,
                amount=quantity * price,
                trade_date=trade_date.strftime('%Y%m%d'),
                trade_time=datetime.now().strftime('%H%M%S'),
                created_at=datetime.now()
            )
            session.add(trade)
            session.commit()
    except Exception as e:
        st.error(f"거래 기록 저장 오류: {e}")


def render_trade_history_grid(current_market):
    """매매 결과 그리드 (시장별 필터링)"""
    market_str = current_market.value
    currency = "원" if current_market == MarketType.KR else "$"
    
    col1, col2 = st.columns([1, 3])
    with col1:
        selected_date = st.date_input(
            "조회 날짜",
            value=date.today(),
            max_value=date.today(),
            key=f"mt_query_date_{market_str}"
        )
    
    try:
        with get_session() as session:
            date_str = selected_date.strftime('%Y%m%d')
            
            # 시장별 필터 적용
            query = session.query(TradeHistory, ItemMst.itms_nm).outerjoin(
                ItemMst, TradeHistory.item_cd == ItemMst.item_cd
            ).filter(
                TradeHistory.trade_date == date_str,
                TradeHistory.market_type == market_str,
                # TradeHistory.trade_source == 'manual' # 수동매매만 볼지 전체 볼지 결정 (여기선 전체)
            ).order_by(TradeHistory.created_at.desc()).all()
            
            data = []
            for row, item_name in query:
                t_type = "매수" if row.trade_type in ['buy', 'B'] else "매도"
                color = "🟢" if row.trade_type in ['buy', 'B'] else "🔴"
                
                data.append({
                    "시간": row.trade_time[:4] if row.trade_time else "",
                    "구분": f"{color} {t_type}",
                    "종목코드": row.item_cd,
                    "종목명": item_name or row.item_cd,
                    "수량": f"{row.quantity:,}",
                    "단가": f"{row.price:,.2f}{currency}",
                    "금액": f"{row.amount:,.2f}{currency}"
                })
            
            if data:
                st.markdown(f"**조회 결과: {len(data)}건**")
                render_data_grid_with_paging(
                    data=data,
                    columns=["시간", "구분", "종목코드", "종목명", "수량", "단가", "금액"],
                    page_size=20,
                    key_prefix=f"mt_history_{market_str}"
                )
            else:
                st.info(f"{selected_date} 날짜의 {market_str} 매매 기록이 없습니다.")
                
    except Exception as e:
        st.error(f"데이터 조회 오류: {e}")