"""
가격 정보 및 주식 데이터 조회 모듈
- KIS API: 시세, 수급, PER/PBR, 시가총액 등 (실거래/데이터수집용)
- Yahoo Finance: 시뮬레이션 백업용
- 자동 토큰 갱신 (500 에러 대응)
"""

import logging
import requests
import json
import os
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List
from pathlib import Path
import time
from bs4 import BeautifulSoup
import re

try:
    import yfinance as yf
except ImportError:
    yf = None

from config.settings import get_settings_manager
from config.database import get_session, ItemPrice, ItemEquity, ItemMst
from data.kr.fnguide_fetcher import FnGuideFetcher
from impl.kr.kr_fetcher import KrFetcher

logger = logging.getLogger(__name__)

class StockDataCollector:
    """주식 데이터 수집 서비스"""
    
    def __init__(self, mode: str = None):
        self.kis_api = KrFetcher(mode=mode)
    
    def collect_stock_data(self, stock_code: str, base_date_str: str) -> Dict:
        result = {'success': False, 'price_saved': False, 'equity_saved': False, 'error': None}
        
        if not self.kis_api.is_configured():
            result['error'] = "KIS API 미설정"
            return result
            
        try:
            # API 데이터 수집 (DB 연결 전에 수행하여 트랜잭션 시간 최소화)
            stock_info = self.kis_api.get_stock_info(stock_code)
            investor_data = self.kis_api.get_investor_trading(stock_code)
            
            with get_session() as session:
                # =========================================================
                # 1. 시세 데이터 수집 및 준비
                # =========================================================
                BATCH_SIZE = 100
                EPOCH = 4
                today = date.today()
                
                # 최신 데이터 날짜 확인하여 가져올 기간 최적화
                latest_price = session.query(ItemPrice).filter(
                    ItemPrice.item_cd == stock_code
                ).order_by(ItemPrice.trade_date.desc()).first()
                
                if latest_price:
                    try:
                        last_date = datetime.strptime(latest_price.trade_date, "%Y%m%d").date()
                        if (today - last_date).days < 3: EPOCH = 1
                    except: pass
                
                collected_prices = []
                for i in range(EPOCH):
                    days_ago_to = i * BATCH_SIZE
                    days_ago_from = ((i + 1) * BATCH_SIZE) - 1
                    
                    to_date = today - timedelta(days=days_ago_to)
                    from_date = today - timedelta(days=days_ago_from)
                    
                    batch_data = self.kis_api.get_period_prices(
                        stock_code, 
                        from_date.strftime("%Y%m%d"), 
                        to_date.strftime("%Y%m%d")
                    )
                    
                    if batch_data:
                        collected_prices.extend(batch_data)
                    time.sleep(0.1) # API 부하 조절
                
                # DB 저장 로직
                if collected_prices:
                    # A. 기존 날짜 미리 조회 (중복 방지 핵심)
                    existing_dates = set()
                    existing_rows = session.query(ItemPrice.trade_date).filter(
                        ItemPrice.item_cd == stock_code
                    ).all()
                    existing_dates = {row.trade_date for row in existing_rows}

                    # B. 중복 제외하고 INSERT
                    new_prices_added = False
                    for p in collected_prices:
                        t_date = p['stck_bsop_date']
                        if not t_date: continue
                        if t_date in existing_dates: continue # 메모리단 중복 체크
                        
                        existing_dates.add(t_date) # 세트에도 추가하여 이번 루프 내 중복 방지

                        new_p = ItemPrice(
                            item_cd=stock_code,
                            trade_date=t_date,
                            stck_clpr=p['stck_clpr'],
                            stck_oprc=p['stck_oprc'],
                            stck_hgpr=p['stck_hgpr'],
                            stck_lwpr=p['stck_lwpr'],
                            acml_vol=p['acml_vol'],
                            prdy_vrss=p['prdy_vrss']
                        )
                        session.add(new_p)
                        new_prices_added = True
                    
                    # [변경] 중간 flush 제거 -> 마지막 commit에 맡김
                    if new_prices_added:
                        result['price_saved'] = True

                    # C. 이동평균 계산 (새 데이터가 추가되었을 때만 수행 권장)
                    # 주의: session.add 후 commit 전이므로, 여기서 query하면 Auto-flush 발생 가능
                    # 하지만 위에서 existing_dates로 방어했으므로 IntegrityError 확률 낮음
                    if new_prices_added:
                        all_prices = session.query(ItemPrice).filter(
                            ItemPrice.item_cd == stock_code
                        ).order_by(ItemPrice.trade_date.desc()).limit(300).all()
                        
                        if all_prices:
                            # ... (이동평균 계산 로직 동일) ...
                            closes = [p.stck_clpr for p in all_prices if p.stck_clpr]
                            ma = {}
                            if len(closes) >= 5: ma['ma5'] = sum(closes[:5])/5
                            if len(closes) >= 10: ma['ma10'] = sum(closes[:10])/10
                            if len(closes) >= 20: ma['ma20'] = sum(closes[:20])/20
                            if len(closes) >= 60: ma['ma60'] = sum(closes[:60])/60
                            if len(closes) >= 120: ma['ma120'] = sum(closes[:120])/120
                            if len(closes) >= 240: ma['ma240'] = sum(closes[:240])/240
                            
                            latest = all_prices[0]
                            latest.ma5 = ma.get('ma5', 0)
                            latest.ma10 = ma.get('ma10', 0)
                            latest.ma20 = ma.get('ma20', 0)
                            latest.ma60 = ma.get('ma60', 0)
                            latest.ma120 = ma.get('ma120', 0)
                            latest.ma240 = ma.get('ma240', 0)
                    
                    # D. 오래된 데이터 정리 (선택 사항)
                    one_year_ago = (today - timedelta(days=400)).strftime('%Y%m%d')
                    session.query(ItemPrice).filter(
                        ItemPrice.item_cd == stock_code,
                        ItemPrice.trade_date < one_year_ago
                    ).delete(synchronize_session=False)

                # =========================================================
                # 2. 주식 기본 정보 (Equity) 저장
                # =========================================================
                if stock_info:
                    eq = session.query(ItemEquity).filter(ItemEquity.item_cd == stock_code).first()
                    u_data = {
                        'bstp_kor_isnm': stock_info.get('bstp_kor_isnm'),
                        'lstn_stcn': stock_info.get('lstn_stcn'),
                        'hts_avls': stock_info.get('hts_avls'),
                        'per': stock_info.get('per'),
                        'pbr': stock_info.get('pbr'),
                        'eps': stock_info.get('eps'),
                        'bps': stock_info.get('bps'),
                        'stck_dryy_hgpr': stock_info.get('stck_dryy_hgpr'),
                        'stck_dryy_lwpr': stock_info.get('stck_dryy_lwpr'),
                        'dryy_hgpr_vrss_prpr_rate': stock_info.get('dryy_hgpr_vrss_prpr_rate'),
                        'dryy_lwpr_vrss_prpr_rate': stock_info.get('dryy_lwpr_vrss_prpr_rate'),
                        'hts_frgn_ehrt': stock_info.get('hts_frgn_ehrt'),
                        'frgn_hldn_qty': stock_info.get('frgn_hldn_qty'),
                        'dividend_yield': stock_info.get('dividend_yield'),
                        'loan_rate': stock_info.get('loan_rate'),
                        'stat_code': stock_info.get('stat_code'),
                        'is_short_over': stock_info.get('is_short_over'),
                        'vol_turnover': stock_info.get('vol_turnover'),
                        'w52_hgpr': stock_info.get('w52_hgpr'),
                        'w52_hgpr_date': stock_info.get('w52_hgpr_date'),
                        'w52_lwpr': stock_info.get('w52_lwpr'),
                        'w52_lwpr_date': stock_info.get('w52_lwpr_date'),
                        'pvt_res': stock_info.get('pvt_res'),
                        'pvt_res1': stock_info.get('pvt_res1'), 
                        'pvt_res2': stock_info.get('pvt_res2'), 
                        'pvt_sup': stock_info.get('pvt_sup'), 
                        'pvt_sup1': stock_info.get('pvt_sup1'), 
                        'pvt_sup2': stock_info.get('pvt_sup2'), 
                        'pvt': stock_info.get('pvt')
                    }
                    if investor_data:
                        u_data['frgn_ntby_qty'] = investor_data.get('frgn_ntby_qty')
                        u_data['pgtr_ntby_qty'] = investor_data.get('orgn_ntby_qty')
                        
                    if eq:
                        for k, v in u_data.items():
                            if v is not None: setattr(eq, k, v)
                        eq.updated_date = datetime.now()
                    else:
                        session.add(ItemEquity(item_cd=stock_code, **u_data))
                    result['equity_saved'] = True
                
                # =========================================================
                # 3. 통합 커밋 (한 번에 저장)
                # =========================================================
                session.commit()
                result['success'] = True
                
        except Exception as e:
            # 에러 발생 시 전체 롤백 (부분 저장 방지)
            result['error'] = str(e)
            logger.error(f"주식데이터 수집 통합 오류 ({stock_code}): {e}")
            # session은 context manager가 닫아주지만, 명시적 롤백이 안전함 (context 내라면 자동 롤백됨)
            
        return result

class PriceFetcher:
    def __init__(self):
        self.kis = KrFetcher()
        self.yf = YahooFinanceFetcher() if yf else None
    
    def get_current_price(self, code: str) -> Optional[Dict]:
        if self.kis.is_configured():
            r = self.kis.get_stock_price_info(code)
            if r:
                return {
                    'price': r.get('stck_clpr', 0),
                    'open': r.get('stck_oprc', 0),
                    'high': r.get('stck_hgpr', 0),
                    'low': r.get('stck_lwpr', 0),
                    'volume': r.get('acml_vol', 0),
                    'change': 0
                }
        
        # KIS 실패 시 야후 파이낸스 시도 (백업)
        if self.yf: return self.yf.get_current_price(code)
        return None

class YahooFinanceFetcher:
    def get_current_price(self, code: str) -> Optional[Dict]:
        try:
            # 코스피 우선 시도
            t = yf.Ticker(f"{code}.KS")
            h = t.history(period="1d")
            
            # 코스닥 재시도
            if h.empty:
                t = yf.Ticker(f"{code}.KQ")
                h = t.history(period="1d")
            
            if h.empty: return None
            
            l = h.iloc[-1]
            return {
                'price': int(l['Close']),
                'open': int(l['Open']),
                'high': int(l['High']),
                'low': int(l['Low']),
                'volume': int(l['Volume']),
                'change': 0
            }
        except: return None

class BondYieldFetcher:
    """채권 수익률 수집기 (NICE신용평가 크롤링)"""
    
    def get_bbb_5y_yield(self) -> float:
        """
        BBB- 등급 5년 만기 회사채 수익률 조회
        (데이터가 없으면 최대 5일 전까지 역추적하여 조회, 실패 시 기본값 8.0 반환)
        """
        max_retries = 10  # 최대 10일 전까지 확인
        
        # 오늘부터 시작해서 하루씩 뒤로 가며 데이터 확인
        # (NICE신용평가는 당일 데이터가 늦게 뜰 수 있으므로 안전하게 어제부터 조회 권장)
        start_date = datetime.now() - timedelta(days=1)

        for i in range(max_retries):
            target_date = start_date - timedelta(days=i)
            date_str = target_date.strftime("%Y-%m-%d")
            
            try:
                url = f"https://www.nicerating.com/disclosure/spreadRates.do?strDate={date_str}"
                
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                
                # 1. 요청
                response = requests.get(url, headers=headers, timeout=5)
                if response.status_code != 200:
                    continue # 접속 실패 시 다음 날짜 시도

                # 2. 파싱
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 테이블 확인 (데이터가 없으면 테이블 자체가 없거나 내용이 비어있음)
                table = soup.select_one("div.tbl_type01 table")
                if not table:
                    continue # 테이블 없으면 다음 날짜 시도

                # 3. 'BBB-' 행과 '5년' 열 찾기
                headers_text = [th.get_text().strip() for th in table.select("thead th")]
                
                # '5년' 컬럼 인덱스 찾기
                col_idx = -1
                for idx, h in enumerate(headers_text):
                    if "5년" in h:
                        col_idx = idx
                        break
                
                if col_idx == -1: 
                    continue # 5년 컬럼 없으면 다음 날짜 시도

                # 본문에서 'BBB-' 행 찾기
                rows = table.select("tbody tr")
                found_value = None
                
                for row in rows:
                    cols = row.select("td, th")
                    if not cols: continue
                    
                    row_title = cols[0].get_text().strip()
                    
                    if "BBB-" in row_title:
                        # 값 추출 (값이 비어있거나 '-'인 경우 대비)
                        val_str = cols[col_idx].get_text().strip()
                        if val_str and val_str != '-':
                            found_value = float(val_str)
                            break
                
                if found_value is not None:
                    logger.info(f"BBB- 5년 채권 수익률 수집 성공 ({date_str} 기준): {found_value}%")
                    return found_value
                
                # 여기까지 왔다면 해당 날짜에 데이터가 없는 것 -> 다음 루프(하루 전)로 이동
                
            except Exception as e:
                # 개별 날짜 에러는 무시하고 계속 시도
                pass
        
        # 모든 시도 실패 시
        logger.warning("최근 10일간 BBB- 5년 채권 수익률 데이터를 찾지 못했습니다. (기본값 8.0% 사용)")
        return 8.0