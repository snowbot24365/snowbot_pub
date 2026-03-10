"""
데이터 수집 모듈
- KRX 종목 목록: FinanceDataReader 활용 (업종/섹터 정보 포함)
- OpenDart API: 재무제표, 기업정보 수집
- 통합 수집 로직 (run_collection)
"""

import logging
import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Callable
import time
import io
import requests
import pandas as pd
import FinanceDataReader as fdr
from data.kr.fnguide_fetcher import FnGuideFetcher
from utils.common import safe_cast

try:
    import OpenDartReader as odr
except ImportError:
    odr = None

from config.settings import get_settings_manager
from config.database import (
    get_session, ItemMst, FinancialSheet
)

logger = logging.getLogger(__name__)


class KRXCollector:
    """FinanceDataReader 기반 종목 목록 수집기"""
    
    def __init__(self):
        self.settings_manager = get_settings_manager()
    
    def collect_stock_list(
        self,
        market: str = "ALL",
        base_date: date = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> List[Dict]:
        """
        FinanceDataReader를 이용하여 상장 종목 목록 수집
        :param market: ALL, KOSPI, KOSDAQ
        """
        results = []
        logger.info(f"종목 목록 수집 시작 (Source: FinanceDataReader, Market: {market})")

        if progress_callback:
            progress_callback(f"KRX 전 종목 데이터 다운로드 중... (Source: FDR)")
        
        try:
            # 1. [핵심 수정] KRX-DESC로 상세 정보(Sector 포함) 가져오기
            try:
                df = fdr.StockListing('KRX-DESC')
            except Exception:
                # KRX-DESC가 지원되지 않는 구버전일 경우 예외처리
                logger.warning("'KRX-DESC' 조회 실패. 'KRX'로 재시도합니다. (Sector 정보가 없을 수 있습니다.)")
                df = fdr.StockListing('KRX')

            # 2. 데이터 정제
            # 컬럼명이 'Code' 또는 'Symbol'로 다를 수 있으므로 통일
            if 'Symbol' in df.columns and 'Code' not in df.columns:
                df.rename(columns={'Symbol': 'Code'}, inplace=True)
            
            # Sector 컬럼 존재 여부 확인 및 처리
            if 'Sector' not in df.columns:
                # 대체 컬럼 확인 (Industry 등) 또는 빈 값 처리
                if 'Industry' in df.columns:
                    df['Sector'] = df['Industry']
                else:
                    df['Sector'] = '' 
            
            df['Sector'] = df['Sector'].fillna('')
            
            # 3. 시장 필터링 및 데이터 변환
            target_markets = []
            if market == "ALL":
                target_markets = ["KOSPI", "KOSDAQ"]
            elif market == "KOSPI":
                target_markets = ["KOSPI"]
            elif market == "KOSDAQ":
                target_markets = ["KOSDAQ", "KOSDAQ GLOBAL"]
            
            count = 0
            for _, row in df.iterrows():
                # Market 컬럼 확인
                raw_market = row.get('Market', 'ETC')
                
                # KOSDAQ GLOBAL 등을 KOSDAQ으로 통일
                market_category = "KOSDAQ" if "KOSDAQ" in raw_market else raw_market
                
                if market != "ALL" and market_category not in target_markets:
                    continue
                
                if market == "ALL" and market_category not in ["KOSPI", "KOSDAQ"]:
                    continue

                # 4. 결과 리스트 생성
                results.append({
                    'item_cd': str(row['Code']), # Code 컬럼 사용
                    'itms_nm': row['Name'],
                    'corp_nm': row['Name'],
                    'mrkt_ctg': market_category,
                    'sector': row['Sector'],     # [해결] Sector 정보 정상 수집
                    'list_shares': int(row['Stocks']) if 'Stocks' in row and pd.notnull(row['Stocks']) else 0,
                    'mkt_cap': int(row['Marcap']) if 'Marcap' in row and pd.notnull(row['Marcap']) else 0,
                })
                count += 1

            logger.info(f"전체 종목 수집 완료: {count}개 (대상 시장: {market})")
            return results
            
        except Exception as e:
            logger.error(f"FDR 종목 목록 수집 오류: {e}")
            raise


class DartCollector:
    """OpenDart 데이터 수집기 (재무제표)"""
    
    def __init__(self):
        self.settings_manager = get_settings_manager()
        self._dart = None
        self.fnguide = FnGuideFetcher()
    
    @property
    def dart(self):
        if self._dart is None:
            api_key = self.settings_manager.settings.api.opendart_api_key
            if not api_key:
                raise ValueError("OpenDart API 키가 설정되지 않았습니다.")
            if odr is None:
                raise ImportError("OpenDartReader 패키지가 설치되지 않았습니다.")
            self._dart = odr(api_key)
        return self._dart
    
    def get_corp_code(self, stock_code: str) -> Optional[str]:
        """종목코드로 기업고유번호(corp_code) 조회"""
        try:
            corp_list = self.dart.corp_codes
            match = corp_list[corp_list['stock_code'] == stock_code]
            if len(match) > 0:
                return match.iloc[0]['corp_code']
            return None
        except Exception as e:
            logger.debug(f"기업코드 조회 실패: {stock_code} - {e}")
            return None
    
    def collect_financial_ratios(self, stock_code: str, year: int) -> Optional[Dict]:
        """재무비율 수집 (DART API 직접 활용)"""
        try:
            corp_code = self.get_corp_code(stock_code)
            if not corp_code:
                return None
            
            # 우선순위: 사업보고서(11011) -> 3분기 -> 반기 -> 1분기
            codes = ['11011', '11014', '11012', '11013']
            
            for r_code in codes:
                try:
                    fs = self.dart.finstate(corp_code, year, reprt_code=r_code)
                    if fs is not None and not isinstance(fs, dict) and not fs.empty:
                        res = self._parse_financial_data(fs, stock_code, year)
                        if res.get('has_data'):
                            return res
                except:
                    continue
            
            # 전년도 시도
            try:
                fs = self.dart.finstate(corp_code, year - 1, reprt_code='11011')
                if fs is not None and not isinstance(fs, dict) and not fs.empty:
                    res = self._parse_financial_data(fs, stock_code, year - 1)
                    if res.get('has_data'):
                        return res
            except:
                pass
            
            return None
            
        except Exception as e:
            logger.debug(f"재무비율 수집 오류 ({stock_code}): {e}")
            return None
    
    def _parse_financial_data(self, fs_df, stock_code: str, year: int) -> Dict:
        """재무제표 DataFrame에서 필요한 데이터 추출 (DART + FnGuide 하이브리드 전략)"""
        result = {
            'item_cd': stock_code,
            'year': year,
            'has_data': False
        }
        
        try:
            # =========================================================
            # [Step A] DART 데이터 추출 (기존 4단계 전략 유지)
            # =========================================================
            
            # 1. 데이터셋 분리
            cons_df = fs_df[fs_df['fs_nm'].str.contains('연결', na=False)]
            sep_df = fs_df[~fs_df['fs_nm'].str.contains('연결', na=False)]
            main_df = cons_df if not cons_df.empty else sep_df

            # 내부 함수: 값 추출
            def get_amount_from_df(target_df, keywords, column='thstrm_amount'):
                if target_df.empty: return None
                temp_account = target_df['account_nm'].astype(str).str.replace(' ', '').str.strip()
                for keyword in keywords:
                    clean_keyword = keyword.replace(' ', '')
                    mask = temp_account.str.contains(clean_keyword, na=False, regex=False)
                    rows = target_df[mask]
                    if not rows.empty:
                        val = rows.iloc[0].get(column)
                        if val and pd.notna(val):
                            if isinstance(val, str):
                                val = val.replace(',', '').replace(' ', '')
                                try: return float(val)
                                except: pass
                            else:
                                return float(val)
                return None

            # 2. [BS/IS] 데이터 추출 (DART)
            revenue = get_amount_from_df(main_df, ['매출액', '영업수익', '수익(매출액)'])
            operating_profit = get_amount_from_df(main_df, ['영업이익', '영업손익'])
            net_income = get_amount_from_df(main_df, ['당기순이익', '당기순손익', '분기순이익'])
            total_equity = get_amount_from_df(main_df, ['자본총계'])
            total_liabilities = get_amount_from_df(main_df, ['부채총계'])
            retained_earnings = get_amount_from_df(main_df, ['이익잉여금', '결손금'])
            prev_revenue = get_amount_from_df(main_df, ['매출액', '영업수익'], 'frmtrm_amount')
            prev_operating_profit = get_amount_from_df(main_df, ['영업이익', '영업손익'], 'frmtrm_amount')
            capital = get_amount_from_df(main_df, ['자본금', '납입자본'])
            total_assets = get_amount_from_df(main_df, ['자산총계'])

            # 3. [CF] 현금흐름표 데이터 추출 (DART)
            cf_oa = None
            cf_ia = None
            oa_keywords = ['영업활동으로인한현금흐름', '영업활동현금흐름', '영업활동순현금흐름', '영업활동']
            ia_keywords = ['투자활동으로인한현금흐름', '투자활동현금흐름', '투자활동순현금흐름', '투자활동']

            # DART 4단계 검색 로직 (생략 없이 수행)
            if not cons_df.empty:
                cons_cf = cons_df[cons_df['sj_div'] == 'CF']
                if not cons_cf.empty:
                    cf_oa = get_amount_from_df(cons_cf, oa_keywords)
                    cf_ia = get_amount_from_df(cons_cf, ia_keywords)
            
            if cf_oa is None and not sep_df.empty:
                sep_cf = sep_df[sep_df['sj_div'] == 'CF']
                if not sep_cf.empty:
                    cf_oa = get_amount_from_df(sep_cf, oa_keywords)
                    if cf_ia is None: cf_ia = get_amount_from_df(sep_cf, ia_keywords)

            if cf_oa is None and not cons_df.empty:
                cf_oa = get_amount_from_df(cons_df, oa_keywords)
                if cf_ia is None: cf_ia = get_amount_from_df(cons_df, ia_keywords)

            if cf_oa is None:
                cf_oa = get_amount_from_df(fs_df, oa_keywords)
                if cf_ia is None: cf_ia = get_amount_from_df(fs_df, ia_keywords)

            # =========================================================
            # [Step B] FnGuide Fallback (DART 데이터 누락 시 보완)
            # - 현금흐름(CF)이나 자본총계(Equity)가 없으면 FnGuide에서 해당 연도 데이터를 찾아옴
            # =========================================================
            if cf_oa is None or total_equity is None:
                try:
                    # FnGuide 크롤링 호출
                    fn_data = self.fnguide.fetch_financial_statement(stock_code, year)
                    if fn_data:
                        # DART 값이 없으면 FnGuide 값으로 채움
                        if cf_oa is None: cf_oa = fn_data.get('cf_oa')
                        if cf_ia is None: cf_ia = fn_data.get('cf_ia')
                        if total_equity is None: total_equity = fn_data.get('total_equity')
                        if revenue is None: revenue = fn_data.get('revenue')
                        if total_assets is None: total_assets = fn_data.get('total_assets')
                        if net_income is None: net_income = fn_data.get('net_income')
                        
                        logger.info(f"[{stock_code}] DART 누락 데이터 FnGuide로 보완 완료 ({year})")
                except Exception as e:
                    logger.debug(f"FnGuide 보완 실패: {e}")

            # =========================================================

            # 4. 결과 매핑
            if revenue: result['revenue'] = revenue
            if total_assets: result['total_assets'] = total_assets
            if total_equity: result['total_equity'] = total_equity
            if cf_oa is not None: result['cf_oa'] = cf_oa
            if cf_ia is not None: result['cf_ia'] = cf_ia
            
            # 비율 계산 (필요한 데이터가 모두 확보된 상태에서 수행)
            if revenue and prev_revenue and prev_revenue != 0:
                result['grs'] = self._validate_ratio(((revenue - prev_revenue) / abs(prev_revenue)) * 100, '매출액증가율', -100, 500)
            
            if operating_profit and prev_operating_profit and prev_operating_profit != 0:
                result['bsop_prfi_inrt'] = self._validate_ratio(((operating_profit - prev_operating_profit) / abs(prev_operating_profit)) * 100, '영업이익증가율', -100, 500)
            
            if retained_earnings and capital and capital != 0:
                result['rsrv_rate'] = self._validate_ratio((retained_earnings / capital) * 100, '유보율', -1000, 50000)
            
            if total_liabilities and total_equity and total_equity != 0:
                result['lblt_rate'] = self._validate_ratio((total_liabilities / total_equity) * 100, '부채비율', 0, 5000)
            
            if net_income:
                result['thtr_ntin'] = net_income
            
            if net_income and total_equity and total_equity != 0:
                result['roe'] = self._validate_ratio((net_income / total_equity) * 100, 'ROE', -100, 200)
            
            # 데이터 존재 여부 판단
            check_keys = ['grs', 'bsop_prfi_inrt', 'lblt_rate', 'roe', 'thtr_ntin']
            if any(result.get(k) is not None for k in check_keys) or result.get('cf_oa') is not None:
                result['has_data'] = True
            
            return result
            
        except Exception as e:
            # logger.debug(f"재무데이터 파싱 오류 ({stock_code}): {e}")
            return result
    
    def _validate_ratio(self, value: float, name: str, min_val: float = -1000, max_val: float = 1000) -> Optional[float]:
        if value is None: return None
        if value < min_val or value > max_val: return None
        return round(value, 2)


class DataCollectionService:
    """데이터 수집 통합 서비스"""
    
    def __init__(self):
        self.krx_collector = KRXCollector()
        self.dart_collector = DartCollector()
        self.settings_manager = get_settings_manager()

    # =========================================================================
    # [통합] 데이터 수집 메인 함수 (Full + Incremental)
    # =========================================================================
    def run_collection(
        self,
        base_date: date = None,
        collect_source: str = 'auto',  # 'auto' or 'manual'
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        log_callback: Optional[Callable[[str], None]] = None
    ) -> Dict:
        """
        통합 데이터 수집 로직
        - 설정(collection_mode)에 따라 '전체' 또는 '무작위 N개' 수집
        - DB 상태에 따라 초기화(Full) 또는 이어하기(Incremental) 자동 처리
        """
        if base_date is None:
            now = datetime.now()
            # 새벽(0시~8시)에 실행되면, '오늘'이 아니라 '어제' 데이터를 수집하도록 처리
            if now.hour < 9:
                base_date = (now - timedelta(days=1)).date()
            else:
                base_date = now.date()
        
        base_date_str = base_date.strftime('%Y%m%d')
        
        result = {
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'base_date': base_date_str,
            'collect_source': collect_source,
            'items_collected': 0,
            'financial_collected': 0,
            'financial_skipped': 0,
            'kis_collected': 0,
            'errors': [],
            'logs': []
        }
        
        def log(message: str, level: str = "INFO"):
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_msg = f"[{timestamp}] {message}"
            result['logs'].append(log_msg)
            if level == "ERROR": logger.error(message)
            else: logger.info(message)
            if log_callback: log_callback(log_msg)

        try:
            settings = self.settings_manager.settings.collection
            mode_label = "무작위 N개" if settings.kr_collection_mode == "random_n" else "전체"
            log("=" * 50)
            log(f"데이터 수집 시작 (기준일: {base_date_str}, 모드: {mode_label})")
            
            # -------------------------------------------------------
            # 1단계: 종목 리스트 준비 (없으면 FDR로 수집)
            # -------------------------------------------------------
            if progress_callback: progress_callback(0, 100, "종목 목록 수집 중...")
                
            # 시장 선택
            market = "ALL"
            if settings.collect_kospi and not settings.collect_kosdaq: market = "KOSPI"
            elif not settings.collect_kospi and settings.collect_kosdaq: market = "KOSDAQ"
            
            # FDR 수집 (KRXCollector 내부에서 처리)
            all_items = self.krx_collector.collect_stock_list(
                market=market, base_date=base_date, 
                progress_callback=lambda msg: log(msg)
            )
            
            if not all_items:
                log("종목 목록 수집 실패 (데이터 없음)", "ERROR")
                return result
            
            # 설정에 따른 수집 대상 필터링 (DB 저장 단계에서 제한)
            if settings.kr_collection_mode == "random_n":
                total_count = len(all_items)
                target_count = min(settings.kr_random_n_stocks, len(all_items))
                all_items = random.sample(all_items, target_count)
                log(f"전체 {total_count}개 종목 중 설정에 따라 무작위 {target_count}개 종목만 선택하여 저장")
            
            saved = self._save_items_to_db(all_items, base_date_str)
            result['items_collected'] = saved

            # -------------------------------------------------------
            # 2단계: 미수집 종목(Pending) 대상 선정
            # -------------------------------------------------------
            target_items = []
            with get_session() as session:
                # collect_source가 NULL인 것만 조회 (완료된 것은 건너뜀)
                query = session.query(ItemMst).filter(
                    ItemMst.base_date == base_date_str,
                    ItemMst.collect_source.is_(None)
                ).all()
                for item in query:
                    target_items.append({
                        'item_cd': item.item_cd,
                        'itms_nm': item.itms_nm,
                        'mrkt_ctg': item.mrkt_ctg
                    })
            
            # 이미 DB에 전체 데이터가 있는데 '무작위 N개'로 모드를 변경한 경우 대비
            if settings.kr_collection_mode == "random_n":
                limit_n = settings.kr_random_n_stocks
                if len(target_items) > limit_n:
                    target_items = random.sample(target_items, limit_n)

            total_count = len(target_items)
            if total_count == 0:
                log("수집할 대상이 없습니다. (모든 종목 수집 완료됨)")
                result['end_time'] = datetime.now().isoformat()
                if progress_callback: progress_callback(100, 100, "완료")
                return result

            # -------------------------------------------------------
            # 3단계: 상세 데이터 수집 (시세/재무)
            # -------------------------------------------------------
            current_year = date.today().year - 1
            
            # KIS API 준비
            from data.kr.price_fetcher import StockDataCollector
            stock_data_collector = StockDataCollector()
            kis_configured = stock_data_collector.kis_api.is_configured()
            
            if kis_configured: log("KIS API 연결됨 (시세 수집 가능)")
            else: log("KIS API 미연결 (재무 정보만 수집)")

            for idx, item in enumerate(target_items):
                stock_code = item['item_cd']
                stock_name = item.get('itms_nm', stock_code)
                market_type = item.get('mrkt_ctg', '')
                
                if progress_callback:
                    progress = int((idx / total_count) * 100)
                    progress_callback(progress, 100, f"[{idx+1}/{total_count}] {stock_name}")

                # 작업 시작 표시
                self._update_item_collect_source(stock_code, base_date_str, collect_source or 'manual')
                
                try:
                    # [KIS] 시세/수급/PER/PBR 수집
                    if kis_configured:
                        kis_res = stock_data_collector.collect_stock_data(stock_code, base_date_str)
                        if kis_res.get('success'):
                            result['kis_collected'] += 1
                    
                    # [DART] 재무제표 수집
                    financial = self.dart_collector.collect_financial_ratios(stock_code, current_year)
                    
                    if financial and financial.get('has_data'):
                        self._save_financial_to_db(stock_code, financial, base_date_str)
                        result['financial_collected'] += 1
                        log(f"✓ [{market_type}] {stock_name} - 수집 성공")
                    else:
                        result['financial_skipped'] += 1
                        log(f"- [{market_type}] {stock_name} - 재무 없음")
                    
                    time.sleep(0.3) 
                    
                except Exception as e:
                    result['errors'].append(f"{stock_code}: {e}")
                    log(f"✗ {stock_name} 오류: {e}", "ERROR")

            log("데이터 수집 작업이 완료되었습니다.")
            result['end_time'] = datetime.now().isoformat()
            if progress_callback: progress_callback(100, 100, "수집 완료")
            
        except Exception as e:
            log(f"kr collector 프로세스 치명적 오류: {e}", "ERROR")
            result['errors'].append(str(e))
            
        return result

    # --- Helper Methods ---
    
    def _save_items_to_db(self, items: List[Dict], base_date_str: str) -> int:
        count = 0
        with get_session() as session:
            for item in items:
                try:
                    # [참고] 여기서는 Insert만 수행 (기존 데이터 유지)
                    # 전체 갱신(Update)이 필요하다면 session.merge() 사용 권장
                    existing = session.query(ItemMst).filter(
                        ItemMst.item_cd == item['item_cd'],
                        ItemMst.base_date == base_date_str
                    ).first()
                    
                    if not existing:
                        new_item = ItemMst(
                            item_cd=item['item_cd'],
                            base_date=base_date_str,
                            itms_nm=item.get('itms_nm', ''),
                            corp_nm=item.get('corp_nm', ''),
                            mrkt_ctg=item.get('mrkt_ctg', ''),
                            sector=item.get('sector', ''), # [해결] 섹터 정보 저장
                            collect_source=None,
                            created_date=datetime.now()
                        )
                        session.add(new_item)
                        count += 1
                except Exception as e:
                    logger.error(f"ItemMst 저장 실패: {e}")
            
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"DB 저장 실패: {e}")
                
        return count

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

    def _save_financial_to_db(self, stock_code: str, data: Dict, base_date_str: str):
        try:
            with get_session() as session:
                year = data.get('year')
                stac = f"{year}12"
                
                sheet = session.query(FinancialSheet).filter(
                    FinancialSheet.item_cd == stock_code,
                    FinancialSheet.base_date == base_date_str,
                    FinancialSheet.stac_yymm == stac
                ).first()
                
                if not sheet:
                    sheet = FinancialSheet(
                        item_cd=stock_code,
                        base_date=base_date_str,
                        sheet_cl='0',
                        stac_yymm=stac
                    )
                    session.add(sheet)
                
                # 모든 필드 할당 시 safe_cast 적용
                # 값이 없거나 NaN이면 None(NULL)으로 저장되거나 0으로 저장 (로직에 따라 선택)
                
                # 비율(Rate) 데이터 - 없는 경우 0.0 처리? or None 유지?
                # 여기서는 값이 없으면 None(NULL)이 들어가도록 safe_cast만 사용합니다.
                # 만약 DB 컬럼이 Not Null이고 Default 0이라면 'or 0'을 붙이세요.
                sheet.grs = safe_cast(data.get('grs'))
                sheet.bsop_prfi_inrt = safe_cast(data.get('bsop_prfi_inrt'))
                sheet.rsrv_rate = safe_cast(data.get('rsrv_rate'))
                sheet.lblt_rate = safe_cast(data.get('lblt_rate'))
                
                # 금액 데이터 - 보통 정수형이나 소수점 가능성 있음
                sheet.thtr_ntin = safe_cast(data.get('thtr_ntin'))
                sheet.roe_val = safe_cast(data.get('roe'))
                
                sheet.revenue = safe_cast(data.get('revenue'))
                sheet.total_assets = safe_cast(data.get('total_assets'))
                sheet.total_equity = safe_cast(data.get('total_equity'))
                
                sheet.cf_oa = safe_cast(data.get('cf_oa'))
                sheet.cf_ia = safe_cast(data.get('cf_ia'))
                
                session.commit()
        except Exception as e:
            if session:
                session.rollback()

    # 기존 호환성 유지를 위한 Alias
    def run_full_collection(self, **kwargs):
        return self.run_collection(**kwargs)

    def run_incremental_collection(self, **kwargs):
        return self.run_collection(**kwargs)