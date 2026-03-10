import logging
import pandas as pd
import FinanceDataReader as fdr
import yfinance as yf
from datetime import datetime, date
import time
from typing import Callable, Optional, Dict, Any
import numpy as np
import re
from utils.common import safe_cast
from config.database import (
    get_session, ItemMst, ItemEquity, FinancialSheet, ItemPrice
)
from config.settings import get_settings_manager

logger = logging.getLogger(__name__)

class UsDataCollector:
    """
    미국 주식 데이터 수집기
    - 종목 리스트: FinanceDataReader (NASDAQ, NYSE, AMEX)
    - 재무/기본 정보: yfinance (Yahoo Finance)
    """
    def __init__(self):
        self.settings_manager = get_settings_manager()
        self.settings = self.settings_manager.settings.collection

    def collect_stock_list(self, log_callback: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
        """미국 주요 거래소(NASDAQ, NYSE, AMEX) 종목 리스트 수집"""
        all_dfs = []
        
        # 1. NASDAQ
        if self.settings.collect_nasdaq:
            try:
                if log_callback: log_callback("🇺🇸 NASDAQ 종목 리스트 다운로드...")
                df = fdr.StockListing('NASDAQ')
                df['Market'] = 'NASDAQ'
                limit = self.settings.nasdaq_top_n
                if limit > 0:
                    df = df.head(limit)
                    if log_callback: log_callback(f"   -> NASDAQ 상위 {limit}개로 제한")
                all_dfs.append(df)
            except Exception as e:
                logger.error(f"NASDAQ 리스트 실패: {e}")

        # 2. NYSE
        if self.settings.collect_nyse:
            try:
                if log_callback: log_callback("🇺🇸 NYSE 종목 리스트 다운로드...")
                df = fdr.StockListing('NYSE')
                df['Market'] = 'NYSE'
                limit = self.settings.nyse_top_n
                if limit > 0:
                    df = df.head(limit)
                    if log_callback: log_callback(f"   -> NYSE 상위 {limit}개로 제한")
                all_dfs.append(df)
            except Exception as e:
                logger.error(f"NYSE 리스트 실패: {e}")

        # 3. AMEX
        if self.settings.collect_amex:
            try:
                if log_callback: log_callback("🇺🇸 AMEX 종목 리스트 다운로드...")
                df = fdr.StockListing('AMEX')
                df['Market'] = 'AMEX'
                limit = self.settings.amex_top_n
                if limit > 0:
                    df = df.head(limit)
                    if log_callback: log_callback(f"   -> AMEX 상위 {limit}개로 제한")
                all_dfs.append(df)
            except Exception as e:
                logger.error(f"AMEX 리스트 실패: {e}")

        if not all_dfs:
            return pd.DataFrame()

        merged_df = pd.concat(all_dfs)
        merged_df = merged_df.drop_duplicates(subset=['Symbol'])
        return merged_df

    def run_collection(
        self,
        base_date: date = None,
        collect_source: str = 'auto',
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        log_callback: Optional[Callable[[str], None]] = None
    ) -> Dict:
        """
        통합 데이터 수집 로직 (KR run_collection과 구조 통일)
        """
        # 1. 기준 날짜 설정
        if base_date is None:
            now = datetime.now()
            # 새벽(0시~8시) 실행 시 전일 데이터로 간주 (미국장은 한국시간 아침 종료)
            if now.hour < 9:
                base_date = (now - timedelta(days=1)).date()
            else:
                base_date = now.date()
        
        base_date_str = base_date.strftime('%Y%m%d')
        
        # 2. 결과 객체 초기화
        result = {
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'base_date': base_date_str,
            'collect_source': collect_source,
            'items_collected': 0,
            'financial_collected': 0,
            'financial_skipped': 0,
            'errors': [],
            'logs': []
        }
        
        # 내부 로그 함수
        def log(message: str, level: str = "INFO"):
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_msg = f"[{timestamp}] {message}"
            result['logs'].append(log_msg)
            
            if level == "ERROR": logger.error(message)
            else: logger.info(message)
            
            if log_callback: log_callback(log_msg)

        try:
            # 설정 로드
            mode_label = "무작위 N개" if self.settings.us_collection_mode == "random_n" else "전체"
            log("=" * 50)
            log(f"🇺🇸 미국 주식 데이터 수집 시작 (기준일: {base_date_str}, 모드: {mode_label})")
            
            # -------------------------------------------------------
            # 1단계: 종목 리스트 확보 (FDR)
            # -------------------------------------------------------
            df_list = self.collect_stock_list(lambda msg: log(msg))
            
            if df_list.empty:
                log("❌ 수집된 종목 리스트가 없습니다.", "ERROR")
                return result

            total_count = len(df_list)
            
            # -------------------------------------------------------
            # 2단계: 수집 대상 선정 (모드 적용)
            # -------------------------------------------------------
            target_df = df_list
            mode = self.settings.us_collection_mode

            if mode == 'random_n':
                sample_n = min(self.settings.us_random_n_stocks, total_count)
                target_df = df_list.sample(n=sample_n)
                log(f"🎲 랜덤 모드: {sample_n}개 종목 샘플링")
            
            targets = target_df.to_dict('records')
            target_count = len(targets)
            
            # -------------------------------------------------------
            # 3단계: 상세 데이터 수집 (yfinance)
            # -------------------------------------------------------
            log(f"전체 {total_count}개 종목 중 설정에 따라 무작위 {target_count}개 종목만 선택하여 저장")
            
            for idx, row in enumerate(targets):
                ticker = row['Symbol']

                # 앞뒤 공백 제거 (안전 장치)
                ticker = ticker.strip()
                
                # 오직 알파벳과 숫자로만 구성된 경우만 통과 (공백, -, . 모두 제외됨)
                if not ticker.isalnum():
                    result['errors'].append(ticker)
                    continue

                name = row.get('Name', ticker) 
                market = row['Market']
                
                # 진행률 업데이트
                if progress_callback:
                    progress = int((idx / target_count) * 100)
                    progress_callback(progress, 100, f"[{idx+1}/{target_count}] {ticker}")
                
                # 작업 시작 표시
                self._update_item_collect_source(ticker, base_date_str, collect_source or 'manual')

                try:
                    # yfinance 데이터 로드
                    yf_ticker = yf.Ticker(ticker)
                    
                    # 상세 데이터 (재무, 대차대조표, 현금흐름표)
                    info = yf_ticker.info

                    if not info or 'regularMarketPrice' not in info:
                        # 일부 종목은 info가 비어있어도 history는 될 수 있으나, 
                        # 404 에러가 명확하면 건너뛰는 게 좋음
                        pass

                    financials = yf_ticker.financials
                    balance_sheet = yf_ticker.balance_sheet
                    cashflow = yf_ticker.cashflow
                    
                    # DB 저장
                    self._save_to_db(ticker, name, market, info, financials, balance_sheet, cashflow, collect_source)
                    
                    result['items_collected'] += 1
                    result['financial_collected'] += 1
                    
                except Exception as e:
                    result['errors'].append(ticker)
                    continue
                
                # API Rate Limit 고려 (0.5초 대기)
                time.sleep(0.5)

            # -------------------------------------------------------
            # 4단계: 완료 처리
            # -------------------------------------------------------
            success_count = result['items_collected']
            error_count = len(result['errors'])
            
            log("=" * 50)
            log(f"✅ 수집 완료: 성공 {success_count}건, 실패 {error_count}건")
            
            result['end_time'] = datetime.now().isoformat()
            if progress_callback: progress_callback(100, 100, "수집 완료")
            
            return result

        except Exception as e:
            log(f"us collector 프로세스 치명적 오류: {e}", "ERROR")
            result['errors'].append(str(e))
            return result
        
    def _update_item_collect_source(self, stock_code: str, base_date_str: str, source: str):
        try:
            with get_session() as session:
                item = session.query(ItemMst).filter(
                    ItemMst.item_cd == stock_code,
                    ItemMst.base_date == base_date_str
                ).first()
                if item:
                    item.collect_source = source
                    item.updated_date = datetime.now()
                    session.commit()
        except: pass        

    # --- 계산 로직 (레퍼런스 코드 기반) ---
    
    def _calculate_avg_roe(self, financials: pd.DataFrame, balance_sheet: pd.DataFrame, years: int = 3) -> float:
        """3년 평균 ROE 계산"""
        try:
            if financials.empty or balance_sheet.empty: return 0.0
            
            # 항목명 매핑 (yfinance 버전에 따라 다를 수 있음)
            net_income_row = 'Net Income' if 'Net Income' in financials.index else 'Net Income Common Stockholders'
            equity_row = 'Stockholders Equity' if 'Stockholders Equity' in balance_sheet.index else 'Total Stockholder Equity'
            
            if net_income_row not in financials.index or equity_row not in balance_sheet.index:
                return 0.0

            # 공통 컬럼(날짜) 찾기
            common_cols = financials.columns.intersection(balance_sheet.columns)
            if len(common_cols) == 0: return 0.0
            
            net_income = financials.loc[net_income_row, common_cols].head(years)
            equity = balance_sheet.loc[equity_row, common_cols].head(years)
            
            roe_series = (net_income / equity.replace(0, np.nan)) * 100
            return roe_series.mean()
        except:
            return 0.0
    
    def _save_to_db(self, ticker, name, market, info, financials, balance_sheet, cashflow, collect_source):
        # 1. 앞뒤 공백 제거 (안전 장치)
        ticker = ticker.strip()
        
        if ' ' in ticker:
            return

        today_str = date.today().strftime('%Y%m%d')
        
        with get_session() as session:
            try:
                # 1. ItemMst
                mst = session.query(ItemMst).filter_by(item_cd=ticker).first()
                if not mst:
                    mst = ItemMst(item_cd=ticker)
                    session.add(mst)
                
                mst.base_date = today_str
                mst.market_type = "US"
                mst.mrkt_ctg = market
                mst.itms_nm = name
                mst.corp_nm = info.get('longName', name)
                mst.sector = info.get('sector', '')
                mst.collect_source = collect_source
                mst.updated_date = datetime.now()

                # 2. ItemEquity (기본 지표)
                eq = session.query(ItemEquity).filter_by(item_cd=ticker).first()
                if not eq:
                    eq = ItemEquity(item_cd=ticker)
                    session.add(eq)
                
                eq.market_type = "US"
                eq.bstp_kor_isnm = info.get('industry', '')
                eq.lstn_stcn = safe_cast(info.get('sharesOutstanding')) or 0 # shares는 정수형 권장이나 float도 안전
                eq.hts_avls = safe_cast(info.get('marketCap')) or 0
                
                eq.per = safe_cast(info.get('trailingPE')) or 0
                eq.pbr = safe_cast(info.get('priceToBook')) or 0
                eq.eps = safe_cast(info.get('trailingEps')) or 0
                eq.bps = safe_cast(info.get('bookValue')) or 0
                
                # 배당수익률 계산 안전 처리
                div_rate = safe_cast(info.get('dividendYield'))
                eq.dividend_yield = (div_rate * 100) if div_rate else 0
                
                eq.w52_hgpr = safe_cast(info.get('fiftyTwoWeekHigh')) or 0
                eq.w52_lwpr = safe_cast(info.get('fiftyTwoWeekLow')) or 0
                eq.updated_date = datetime.now()

                # 3. ItemPrice (현재가)
                pr = session.query(ItemPrice).filter_by(item_cd=ticker, trade_date=today_str).first()
                if not pr:
                    pr = ItemPrice(item_cd=ticker, trade_date=today_str)
                    session.add(pr)
                
                pr.market_type = "US"
                curr_p = safe_cast(info.get('currentPrice')) or safe_cast(info.get('regularMarketPreviousClose')) or 0
                pr.stck_clpr = curr_p
                pr.stck_oprc = safe_cast(info.get('open')) or 0
                pr.stck_hgpr = safe_cast(info.get('dayHigh')) or 0
                pr.stck_lwpr = safe_cast(info.get('dayLow')) or 0
                pr.acml_vol = safe_cast(info.get('volume')) or 0

                # 4. FinancialSheet (재무제표)
                fs = session.query(FinancialSheet).filter_by(item_cd=ticker, base_date=today_str).first()
                if not fs:
                    fs = FinancialSheet(item_cd=ticker, base_date=today_str, sheet_cl='0', stac_yymm=today_str[:6])
                    session.add(fs)
                
                fs.revenue = safe_cast(info.get('totalRevenue')) or 0
                fs.thtr_ntin = safe_cast(info.get('netIncomeToCommon')) or 0
                
                avg_roe = self._calculate_avg_roe(financials, balance_sheet, 3)
                cleaned_roe = safe_cast(avg_roe)
                if cleaned_roe is not None:
                    fs.roe_val = cleaned_roe
                else:
                    roe_info = safe_cast(info.get('returnOnEquity'))
                    fs.roe_val = (roe_info * 100) if roe_info else 0
                
                # 성장률 및 이익률
                rev_growth = safe_cast(info.get('revenueGrowth'))
                fs.grs = (rev_growth * 100) if rev_growth else 0
                
                prof_margin = safe_cast(info.get('profitMargins'))
                fs.bsop_prfi_inrt = (prof_margin * 100) if prof_margin else 0
                
                fs.lblt_rate = safe_cast(info.get('debtToEquity')) or 0
                
                # 현금흐름 (DataFrame 접근 시 안전 처리)
                cf_oa = safe_cast(info.get('operatingCashflow'))
                if cf_oa is None and not cashflow.empty and 'Operating Cash Flow' in cashflow.index:
                    # iloc[0]은 numpy 타입이므로 safe_cast 필수
                    cf_oa = safe_cast(cashflow.loc['Operating Cash Flow'].iloc[0])
                fs.cf_oa = cf_oa or 0
                fs.cf_ia = 0 

                # 총자산/자본 (DataFrame 접근 시 안전 처리)
                if not balance_sheet.empty:
                    if 'Total Assets' in balance_sheet.index:
                        fs.total_assets = safe_cast(balance_sheet.loc['Total Assets'].iloc[0]) or 0
                    if 'Stockholders Equity' in balance_sheet.index:
                        fs.total_equity = safe_cast(balance_sheet.loc['Stockholders Equity'].iloc[0]) or 0

                session.commit()
                
            except Exception as e:
                session.rollback() # 에러 발생 시 롤백
                logger.error(f"DB 저장 중 오류 발생 ({ticker}): {e}")