"""
관심종목 관리 페이지 (KR/US 분리 적용)
- KIS HTS 관심종목 연동 (KR/US 시장별 Fetcher 사용)
- 티커 직접 입력 (시장별)
- 사용자 지정 매수 대상 목록 관리 (시장별 필터링)
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import time

from config.database import get_session, UserBuyTarget, ItemMst
from config.settings import get_settings_manager
from core.definition import MarketType
from impl.kr.kr_fetcher import KrFetcher
from impl.us.us_fetcher import UsFetcher 

def render_favorite_page():
    """관심종목 관리 페이지 렌더링"""
    
    # 1. 현재 선택된 시장 확인
    current_market = st.session_state.get('current_market', MarketType.KR)
    market_str = current_market.value

    if 'last_fav_market' not in st.session_state:
        st.session_state['last_fav_market'] = market_str
    
    if st.session_state['last_fav_market'] != market_str:
        # 시장이 변경되었으므로 관련 세션 상태 초기화
        keys_to_clear = ['kis_groups', 'kis_stocks', 'selected_group_name']
        for k in keys_to_clear:
            if k in st.session_state:
                del st.session_state[k]
        
        # 현재 시장 상태 업데이트
        st.session_state['last_fav_market'] = market_str
        st.rerun() # 초기화 후 페이지 다시 로드
    
    st.markdown(f'<div class="main-header">⭐ 관심종목 관리 ({market_str})</div>', unsafe_allow_html=True)
    settings_manager = get_settings_manager()

    # 탭 구성
    # tab1, tab2, tab3 = st.tabs(["📥 KIS 관심종목 가져오기", "⌨️ 종목 직접 추가", "📋 매수 목록 관리"])
    tab1, tab2 = st.tabs(["📥 KIS 관심종목 가져오기", "📋 매수 목록 관리"])
    
    # --- 탭 1: KIS API 연동 ---
    with tab1:
        render_kis_import_section(settings_manager, current_market)

    # --- 탭 2: 직접 입력 ---
    # with tab2:
    #     render_manual_input_section(current_market)

    # --- 탭 2: 매수 대상 관리 ---
    with tab2:
        render_user_buy_targets(current_market)


def render_kis_import_section(settings_manager, current_market):
    """KIS HTS 관심종목 가져오기 (시장별 분기)"""
    market_str = current_market.value
    
    st.markdown(f"#### 📥 KIS HTS 관심종목 가져오기 ({market_str})")
    st.caption("※ KIS 실전투자 API가 설정되어 있어야 하며, HTS에 등록된 관심종목 그룹을 불러옵니다.")
    
    # 필터링 기준 안내
    if current_market == MarketType.KR:
        target_exchanges = ['KRX']
        st.info("💡 **한국 시장 모드**: 거래소 코드가 'KRX' (코스피/코스닥)인 종목만 필터링합니다.")
    else:
        # 미국 주요 거래소 코드 (나스닥, 뉴욕, 아멕스)
        target_exchanges = ['NAS', 'NYS', 'AMS']
        st.info("💡 **미국 시장 모드**: 거래소 코드가 'NAS'(나스닥), 'NYS'(뉴욕)인 종목만 필터링합니다.")

    # 1. HTS User ID 설정
    current_id = settings_manager.settings.api.hts_user_id
    
    col1, col2 = st.columns([3, 1])
    with col1:
        input_id = st.text_input("HTS User ID (@포함)", value=current_id, placeholder="@아이디_입력")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("ID 저장"):
            if input_id.startswith("@"):
                settings_manager.update_api(hts_user_id=input_id)
                st.success("저장됨")
                time.sleep(0.5)
                st.rerun()
            else:
                st.error("'@'로 시작해야 합니다.")

    # 2. API 객체 생성 (시장별 Fetcher 사용)
    settings = settings_manager.settings.api
    
    if current_market == MarketType.KR:
        api_mode = settings.kis_api_mode_kr
        fetcher = KrFetcher(mode=api_mode)
    else:
        api_mode = settings.kis_api_mode_us
        fetcher = UsFetcher(mode=api_mode) 
    
    if not fetcher.is_configured():
        st.error(f"⚠️ {market_str} API 키 설정이 필요합니다. (설정 페이지 이동)")
        return

    # 3. 그룹 조회 버튼
    if st.button("📂 관심종목 그룹 조회"):
        if not input_id or not input_id.startswith("@"):
            st.warning("HTS User ID를 먼저 설정해주세요.")
        else:
            with st.spinner("KIS 서버에서 그룹 정보를 가져오는 중..."):
                try:
                    # UsFetcher에 해당 메서드가 없다면 예외 처리 필요
                    if hasattr(fetcher, 'get_kis_favorite_groups'):
                        groups = fetcher.get_kis_favorite_groups()
                    else:
                        # UsFetcher 미구현 시 KrFetcher 시도 (토큰 문제 가능성 있음)
                        temp_kr = KrFetcher(mode=settings.kis_api_mode_kr)
                        groups = temp_kr.get_kis_favorite_groups()

                    if groups:
                        st.session_state['kis_groups'] = groups
                        st.success(f"총 {len(groups)}개 그룹을 발견했습니다.")
                    else:
                        st.error("그룹 조회 실패. (ID가 틀리거나 API 권한 문제일 수 있습니다)")
                except Exception as e:
                    st.error(f"API 호출 오류: {e}")

    # 4. 그룹 선택 및 종목 리스트 조회
    if 'kis_groups' in st.session_state:
        groups = st.session_state['kis_groups']
        group_map = {f"{g['inter_grp_name']} ({g['inter_grp_code']})": g['inter_grp_code'] for g in groups}
        
        selected_group_str = st.selectbox("가져올 그룹 선택", list(group_map.keys()))
        
        if selected_group_str:
            group_code = group_map[selected_group_str]
            group_name = selected_group_str.split(' (')[0]
            
            if st.button(f"⬇️ '{group_name}' 그룹 종목 불러오기"):
                with st.spinner("종목 리스트 필터링 중..."):
                    try:
                        # UsFetcher 메서드 확인
                        if hasattr(fetcher, 'get_kis_group_stocks'):
                            stocks = fetcher.get_kis_group_stocks(group_code)
                        else:
                            temp_kr = KrFetcher(mode=settings.kis_api_mode_kr)
                            stocks = temp_kr.get_kis_group_stocks(group_code)
                        
                        # [핵심] 시장별 거래소 코드로 필터링
                        filtered_stocks = [
                            s for s in stocks 
                            if s.get('exch_code') in target_exchanges
                        ]
                        
                        st.session_state['kis_stocks'] = filtered_stocks
                        st.session_state['selected_group_name'] = group_name
                        
                        if not filtered_stocks:
                            st.warning(f"해당 그룹에 {market_str} 시장({target_exchanges})에 해당하는 종목이 없습니다.")
                        else:
                            st.success(f"{market_str} 종목 {len(filtered_stocks)}개를 불러왔습니다.")
                            
                    except Exception as e:
                        st.error(f"종목 조회 오류: {e}")

    # 5. 종목 선택 및 저장 Grid
    if 'kis_stocks' in st.session_state and st.session_state['kis_stocks']:
        stocks = st.session_state['kis_stocks']
        
        # 이미 DB에 저장된 종목 확인
        with get_session() as session:
            existing_codes = {
                row.item_cd for row in session.query(UserBuyTarget.item_cd)
                .filter(UserBuyTarget.market_type == market_str).all()
            }

        df_list = []
        for s in stocks:
            code = s.get('jong_code')
            is_exist = code in existing_codes
            
            df_list.append({
                '선택': False,
                '등록여부': '✅ 등록됨' if is_exist else '미등록',
                '거래소': s.get('exch_code'),
                '종목코드': code,
                '종목명': s.get('hts_kor_isnm') or s.get('prdt_name'),
                '_is_exist': is_exist
            })
            
        df = pd.DataFrame(df_list)
        
        edited_df = st.data_editor(
            df,
            column_config={
                "선택": st.column_config.CheckboxColumn("선택", default=False),
                "등록여부": st.column_config.TextColumn("상태", disabled=True),
                "_is_exist": None,
            },
            disabled=["등록여부", "거래소", "종목코드", "종목명"],
            hide_index=True,
            width='content'
        )
        
        if st.button("💾 선택한 종목 저장"):
            to_add = edited_df[
                (edited_df['선택'] == True) & 
                (edited_df['_is_exist'] == False)
            ]
            
            if to_add.empty:
                st.warning("새로 추가할 종목이 선택되지 않았습니다.")
            else:
                count = 0
                with get_session() as session:
                    for _, row in to_add.iterrows():
                        try:
                            exists = session.query(UserBuyTarget).filter_by(
                                item_cd=row['종목코드'],
                                market_type=market_str
                            ).first()
                            
                            if not exists:
                                new_target = UserBuyTarget(
                                    item_cd=row['종목코드'],
                                    market_type=market_str, 
                                    item_nm=row['종목명'],
                                    exch_code=row['거래소'],
                                    group_name=st.session_state.get('selected_group_name', 'KIS Import'),
                                    created_at=datetime.now()
                                )
                                session.add(new_target)
                                count += 1
                        except Exception:
                            pass
                    session.commit()
                
                st.success(f"✅ {count}개 종목이 매수 목록에 추가되었습니다.")
                time.sleep(1)
                st.rerun()


def render_manual_input_section(current_market):
    """[KR/US] 종목 직접 입력 추가"""
    market_str = current_market.value
    st.markdown(f"#### ⌨️ {market_str} 종목 직접 추가")
    
    if current_market == MarketType.KR:
        placeholder = "005930, 035720 (종목코드 6자리)"
        label = "종목코드 입력 (쉼표로 구분)"
    else:
        placeholder = "AAPL, TSLA, NVDA (티커)"
        label = "티커 입력 (쉼표로 구분)"
        
    col1, col2 = st.columns([3, 1])
    with col1:
        input_txt = st.text_input(label, placeholder=placeholder).upper()
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        add_btn = st.button("추가하기", type="primary", width='content')
        
    if add_btn and input_txt:
        codes = [c.strip() for c in input_txt.split(',') if c.strip()]
        
        if not codes:
            st.warning("코드를 입력해주세요.")
            return
            
        with get_session() as session:
            count = 0
            for code in codes:
                exists = session.query(UserBuyTarget).filter_by(
                    item_cd=code, 
                    market_type=market_str
                ).first()
                
                if not exists:
                    item_nm = code
                    mst = session.query(ItemMst).filter_by(
                        item_cd=code, 
                        market_type=market_str
                    ).first()
                    
                    if mst:
                        item_nm = mst.itms_nm
                    
                    new_target = UserBuyTarget(
                        item_cd=code,
                        market_type=market_str,
                        item_nm=item_nm,
                        exch_code='MANUAL',
                        group_name='User Input',
                        created_at=datetime.now()
                    )
                    session.add(new_target)
                    count += 1
            
            session.commit()
            
            if count > 0:
                st.success(f"✅ {count}개 종목이 추가되었습니다.")
                time.sleep(1)
                st.rerun()
            else:
                st.warning("이미 등록된 종목들입니다.")


def render_user_buy_targets(current_market):
    """사용자 지정 매수 목록 관리 (시장별 필터링)"""
    market_str = current_market.value
    
    st.markdown(f"#### 📋 {market_str} 사용자 지정 매수 대상 목록")
    st.info("이 목록의 종목은 자동매매 시 **최우선 검토 대상**이 되며, 대시보드에서도 별도로 관리됩니다.")
    
    with get_session() as session:
        targets = session.query(UserBuyTarget).filter(
            UserBuyTarget.market_type == market_str
        ).order_by(UserBuyTarget.created_at.desc()).all()
        
        if targets:
            data = [{
                '종목코드': t.item_cd, 
                '종목명': t.item_nm, 
                '거래소': t.exch_code,
                '출처': t.group_name, 
                '등록일': t.created_at.strftime('%Y-%m-%d') if t.created_at else "-",
                '삭제': False
            } for t in targets]
            
            df = pd.DataFrame(data)
            
            edited_targets = st.data_editor(
                df,
                column_config={
                    "삭제": st.column_config.CheckboxColumn("삭제", default=False)
                },
                hide_index=True,
                width='content'
            )
            
            if st.button("🗑️ 선택한 종목 삭제"):
                to_delete = edited_targets[edited_targets['삭제'] == True]
                
                if not to_delete.empty:
                    deleted_count = 0
                    for _, row in to_delete.iterrows():
                        session.query(UserBuyTarget).filter(
                            UserBuyTarget.item_cd == row['종목코드'],
                            UserBuyTarget.market_type == market_str
                        ).delete()
                        deleted_count += 1
                        
                    session.commit()
                    st.success(f"✅ {deleted_count}개 종목이 삭제되었습니다.")
                    time.sleep(1)
                    st.rerun()
        else:
            st.info(f"등록된 {market_str} 관심종목이 없습니다.")