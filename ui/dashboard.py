"""
통합 대시보드 (KR/US 분리 적용)
- 계좌 현황 (시장별)
- 보유 종목 상세 (시장별)
- 데이터 통계 및 매수 후보 리스트 (시장별)
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import func, desc, distinct

from config.settings import get_settings_manager
from config.database import get_session, ItemMst, ItemPrice, EvaluationResult, VirtualHolding, Holdings, UserBuyTarget
from core.definition import MarketType
from impl.kr.kr_fetcher import KrFetcher
from impl.us.us_fetcher import UsFetcher
from utils.common import custom_metric
from ui.components import (
    render_account_info,
    render_buy_candidates_table,
    fetch_market_indices,
    render_market_indices
)

def render_dashboard():
    """대시보드 렌더링"""
    
    # 1. 현재 선택된 시장 확인
    current_market = st.session_state.get('current_market', MarketType.KR)
    market_str = current_market.value
    
    st.markdown(f'<div class="main-header">📊 대시보드 ({market_str})</div>', unsafe_allow_html=True)
    
    settings_manager = get_settings_manager()
    settings = settings_manager.settings

    render_market_indices(current_market)
    
    st.markdown("---") # 구분선 추가
    
    # 2. 계좌 정보 (요약 박스 - components.py에서 시장별 처리됨)
    st.markdown("#### 💰 계좌 현황")
    account_type = render_account_info(settings_manager)
    
    # 3. 보유 종목 상세 현황 (테이블 - 시장별 필터링)
    render_holdings_detail(settings_manager, account_type, current_market)
    
    st.divider()
    
    # 4. 데이터 및 평가 현황 (시장별 통계)
    st.markdown(f"#### 📈 {market_str} 데이터 및 평가 현황")
    
    # 통계 데이터 계산
    stats = calculate_statistics(settings, current_market)
    
    col1, col2, col3 = st.columns(3)
    
    # 설정된 최소 점수 로드
    e_settings = settings.evaluation.kr if current_market == MarketType.KR else settings.evaluation.us
    min_score = e_settings.min_total_score
    
    with col1:
        custom_metric("총 관리 종목", f"{stats['total_items']:,}개")
        
    with col2:
        date_display = f"({stats['latest_eval_date']})" if stats['latest_eval_date'] else ""
        custom_metric(f"총 평가 완료 {date_display}", f"{stats['evaluated_count']:,}개")
        
    with col3:
        custom_metric("매수 후보", f"{stats['buy_candidates_count']:,}개")
    
    # 5. 매수 후보 상세 (테이블 - 시장별 필터링)
    if stats['buy_candidates_count'] > 0:
        st.markdown(f"##### {market_str} 매수 후보 리스트", help=f"사용자 관심종목 + 평가점수 {min_score}점 이상 + 추가 안전망 통과")
        render_buy_candidates_table(stats['latest_eval_date'], min_score, current_market)
    else:
        st.info(f"현재 {market_str} 매수 후보 종목이 없습니다.")


def calculate_statistics(settings, current_market):
    """대시보드 통계 데이터 계산 (시장별)"""
    stats = {
        'total_items': 0,
        'evaluated_count': 0,
        'buy_candidates_count': 0,
        'latest_eval_date': None
    }
    
    e_settings = settings.evaluation.kr if current_market == MarketType.KR else settings.evaluation.us
    min_score = e_settings.min_total_score
    
    try:
        with get_session() as session:
            # 1. 총 관리 종목 수 (시장별 필터)
            # ItemMst에 market_type이 있다고 가정 (없다면 종목 코드 패턴 등으로 구분 필요하지만, DB 모델 수정했으므로 사용 가능)
            stats['total_items'] = session.query(func.count(distinct(ItemMst.item_cd)))\
                .filter(ItemMst.market_type == current_market.value).scalar()
            
            # 2. 가장 최근 평가 날짜 조회 (해당 시장 기준)
            latest_date = session.query(func.max(EvaluationResult.base_date))\
                .filter(EvaluationResult.market_type == current_market.value).scalar()
            
            if latest_date:
                stats['latest_eval_date'] = latest_date
                
                # 3. 최근 평가 종목 수
                stats['evaluated_count'] = session.query(EvaluationResult).filter(
                    EvaluationResult.base_date == latest_date,
                    EvaluationResult.market_type == current_market.value
                ).count()
                
                # 4. 매수 후보 수 (사용자 관심종목 + 알고리즘 추천 합집합)
                # 4-1. 알고리즘 추천 종목 코드 집합
                algo_candidates = {
                    row[0] for row in session.query(EvaluationResult.item_cd).filter(
                        EvaluationResult.base_date == latest_date,
                        EvaluationResult.market_type == current_market.value,
                        EvaluationResult.total_score >= min_score,
                        EvaluationResult.is_buy_candidate == True
                    ).all()
                }
                
                # 4-2. 사용자 관심종목 코드 집합
                user_candidates = {
                    row[0] for row in session.query(UserBuyTarget.item_cd).filter(
                        UserBuyTarget.market_type == current_market.value
                    ).all()
                }
                
                # 4-3. 합집합 개수
                stats['buy_candidates_count'] = len(algo_candidates | user_candidates)
                
    except Exception as e:
        st.error(f"통계 계산 오류: {e}")
        
    return stats


def render_holdings_detail(settings_manager, account_type, current_market):
    """보유 종목 상세 리스트 출력 (링크 및 포맷팅 적용)"""
    
    # 1. 시장별 설정 (통화, URL 패턴, 포맷)
    if current_market == MarketType.KR:
        execution_mode = settings_manager.settings.execution_mode_kr
        currency_symbol = "원"
        # 네이버 증권: code=숫자 추출
        link_regex = r"code=(\d+)"
        # KR 포맷: 소수점 없음
        price_fmt = "{:,.0f}" 
        df_price_fmt = "%d" 
    else:
        execution_mode = settings_manager.settings.execution_mode_us
        currency_symbol = "$"
        # Perplexity: finance/문자 추출
        link_regex = r"finance/(.*)"
        # US 포맷: 소수점 2자리
        price_fmt = "{:,.2f}"
        df_price_fmt = "%.2f"

    holdings_data = []

    # ---------------------------------------------------------
    # 데이터 조회 로직 (시뮬레이션 / 실거래 공통화)
    # ---------------------------------------------------------
    try:
        raw_holdings = []
        fetcher = None

        # A. 시뮬레이션 모드: DB 조회
        if execution_mode == "simulation":
            with get_session() as session:
                holdings = session.query(VirtualHolding).filter(
                    VirtualHolding.quantity > 0,
                    VirtualHolding.market_type == current_market.value
                ).all()
                
                # 하위 호환성 (KR 구 데이터)
                if not holdings and current_market == MarketType.KR:
                    holdings = session.query(Holdings).filter(Holdings.quantity > 0).all()
                
                # DB 객체를 딕셔너리 형태로 표준화
                for h in holdings:
                    raw_holdings.append({
                        'code': h.item_cd,
                        'name': h.item_nm or h.item_cd,
                        'qty': getattr(h, 'quantity', 0),
                        'avg_price': float(h.avg_price),
                        'current_price': 0.0, # 아래에서 채움
                        'eval_amt': 0.0,      # 아래에서 채움
                        'profit_rate': 0.0    # 아래에서 채움
                    })
            
            # Fetcher 초기화 (현재가 조회용)
            fetcher = KrFetcher() if current_market == MarketType.KR else UsFetcher()

        # B. 실거래 모드: API 조회
        else:
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

            if acct_no and acct_cd:
                balance = fetcher.get_account_balance(acct_no, acct_cd)
                if balance and 'holdings' in balance:
                    for h in balance['holdings']:
                        raw_holdings.append({
                            'code': h.get('pdno'),
                            'name': h.get('prdt_name'),
                            'qty': int(h.get('hldg_qty', 0)),
                            'avg_price': float(h.get('pchs_avg_pric', 0)),
                            'current_price': float(h.get('prpr', 0)),
                            'eval_amt': float(h.get('evlu_amt', 0)),
                            'profit_rate': float(h.get('evlu_pfls_rt', 0))
                        })

        # ---------------------------------------------------------
        # 데이터 가공 및 리스트 생성
        # ---------------------------------------------------------
        for item in raw_holdings:
            code = item['code']
            qty = item['qty']
            avg_price = item['avg_price']
            
            # 시뮬레이션인 경우 현재가/수익률 직접 계산 필요
            if execution_mode == "simulation":
                price_data = fetcher.get_current_price(code)
                if price_data and price_data.get('price'):
                    current_price = float(price_data['price'])
                else:
                    current_price = avg_price
                
                eval_amt = current_price * qty
                profit_rate = 0.0
                if avg_price > 0:
                    profit_rate = ((current_price - avg_price) / avg_price) * 100
            else:
                # 실거래는 이미 API에서 가져옴
                current_price = item['current_price']
                eval_amt = item['eval_amt']
                profit_rate = item['profit_rate']

            # [핵심] URL 생성
            if current_market == MarketType.KR:
                full_url = f"https://finance.naver.com/item/main.naver?code={code}"
            else:
                full_url = f"https://www.perplexity.ai/finance/{code}"

            holdings_data.append({
                "종목코드": full_url, # 링크용 URL
                "종목명": item['name'],
                "보유수량": qty,
                # 숫자형으로 저장 (화면 표시 시 포맷팅)
                "매입가": avg_price,
                "현재가": current_price,
                "평가금액": eval_amt,
                "수익률": profit_rate
            })

    except Exception as e:
        st.error(f"보유 종목 조회 중 오류 발생: {e}")

    # ---------------------------------------------------------
    # 화면 출력 (DataFrame)
    # ---------------------------------------------------------
    if holdings_data:
        with st.expander("📋 보유 종목 상세 보기", expanded=True):
            df = pd.DataFrame(holdings_data)
            
            st.dataframe(
                df,
                width='stretch',
                hide_index=True,
                column_config={
                    "종목코드": st.column_config.LinkColumn(
                        "종목코드",
                        help="클릭 시 상세 정보 페이지로 이동합니다.",
                        display_text=link_regex # 정규식으로 코드만 표시
                    ),
                    "종목명": st.column_config.TextColumn("종목명"),
                    "보유수량": st.column_config.NumberColumn("보유수량", format="%d"),
                    "매입가": st.column_config.NumberColumn(
                        f"매입가 ({currency_symbol})", 
                        format=df_price_fmt
                    ),
                    "현재가": st.column_config.NumberColumn(
                        f"현재가 ({currency_symbol})", 
                        format=df_price_fmt
                    ),
                    "평가금액": st.column_config.NumberColumn(
                        f"평가금액 ({currency_symbol})", 
                        format=df_price_fmt
                    ),
                    "수익률": st.column_config.NumberColumn(
                        "수익률", 
                        format="%.2f%%" # % 표시
                    ),
                }
            )
    else:
        st.info(f"{current_market.value} 보유 중인 종목이 없습니다.")
