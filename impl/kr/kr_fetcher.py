import logging
import requests
import time
from typing import Optional, Dict, List
from datetime import datetime, date, timedelta

from config.settings import get_settings_manager
from config.database import get_session, ItemPrice, ItemEquity
from data.kr.fnguide_fetcher import FnGuideFetcher 
from utils.token_manager import get_token_manager
from pathlib import Path

logger = logging.getLogger(__name__)

class KrFetcher():
    """KIS API 데이터 수집 및 주문"""
    
    def __init__(self, mode: str = None):
        self.settings_manager = get_settings_manager()
        self.token_manager = get_token_manager()
        self._mode = mode
        self._load_api_config()
        self.fnguide = FnGuideFetcher()
    
    def _load_api_config(self):
        api_settings = self.settings_manager.settings.api
        if self._mode is None: 
            self._mode = getattr(api_settings, 'kis_api_mode_kr', 'mock') # 안전한 접근

        logger.info(f"[KrFetcher] API 초기화 모드: {self._mode}")

        # HTS User ID 로드
        self.hts_user_id = api_settings.hts_user_id
        
        if self._mode == "real":
            self.app_key = api_settings.kis_real_app_key_kr
            self.app_secret = api_settings.kis_real_app_secret_kr
            self.base_url = "https://openapi.koreainvestment.com:9443"
            self.is_mock = False
        else:
            self.app_key = api_settings.kis_mock_app_key_kr
            self.app_secret = api_settings.kis_mock_app_secret_kr
            self.base_url = "https://openapivts.koreainvestment.com:29443"
            self.is_mock = True
    
    def is_configured(self) -> bool:
        return bool(self.app_key and self.app_secret)
    
    def get_access_token(self) -> Optional[str]:
        if not self.is_configured(): return None
        return self.token_manager.get_token("KR", self._mode, self.app_key, self.app_secret, self.base_url)
    
    def _get_headers(self, tr_id: str) -> Dict:
        token = self.get_access_token()
        if not token: return {}
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    def _call_api(self, tr_id: str, url: str, params: dict = None, body: dict = None, method: str = 'GET') -> Optional[Dict]:
        """
        API 호출 공통 메서드
        - 토큰 만료 시 자동 갱신 후 재시도
        - 통신 오류 또는 비정상 응답 시 3회 재시도
        """
        if not self.is_configured(): return None

        time.sleep(0.5)

        for attempt in range(2):  # 최대 2회 시도 (1회 실패 → 토큰 갱신 → 재시도)
            try:
                headers = self._get_headers(tr_id)
                if not headers: 
                    logger.error("헤더 생성 실패 (토큰 없음)")
                    return None
                
                # 통신/데이터 수신 재시도 (3회)
                # 요청하신대로 실패 시 0.5초 대기 후 재시도
                for retry_count in range(3):
                    try:
                        if method == 'GET':
                            response = requests.get(url, headers=headers, params=params, timeout=10)
                        else:
                            response = requests.post(url, headers=headers, json=body, timeout=10)
                        
                        # 1. 응답 파싱 시도
                        data = None
                        try:
                            data = response.json()
                        except:
                            pass

                        # 2. 토큰 만료 체크 (EGW00123, EGW00121)
                        # 이 경우는 재시도(wait)가 아니라 즉시 Break 후 토큰 갱신해야 함
                        if data and isinstance(data, dict):
                            msg_cd = data.get('msg_cd', '')

                            # 초당 전송 횟수 초과 (EGW00201) -> 단순 대기 후 재시도
                            if msg_cd == 'EGW00201':
                                logger.warning(f"API 호출 제한. 0.5초 대기 후 재시도합니다.")
                                time.sleep(0.5)
                                continue

                            # 토큰 만료 -> Inner Loop 탈출 -> Outer Loop에서 토큰 갱신
                            if msg_cd in ['EGW00123', 'EGW00121']:
                                logger.warning(f"API 호출 실패 ({msg_cd}): 토큰 만료 (상태코드 {response.status_code}). 재발급 시도.")
                                self.token_manager.clear_token("KR", self._mode)
                                break # Inner Loop break -> Outer Loop continue로 이어짐

                        # 3. HTTP 상태 및 데이터 성공 확인
                        if response.status_code == 200 and data:
                            return data
                        
                        # --- 실패 시 재시도 로직 ---
                        if retry_count < 2:
                            # 아직 재시도 기회가 남음 (0, 1번째 시도)
                            # logger.debug(f"API 호출 실패/무응답 (시도 {retry_count+1}/3). 0.5초 후 재시도...") 
                            time.sleep(0.5)
                            continue
                        else:
                            # 3번(0,1,2) 모두 실패했을 때만 에러 로그 출력
                            msg = data.get('msg1', 'No Message') if data else response.text
                            logger.error(f"HTTP 오류 또는 데이터 없음 (3회 시도 실패): Status={response.status_code}, Msg={msg}, TR_ID={tr_id}")
                            return None

                    except Exception as req_e:
                        # 요청 중 예외 발생 (ConnectionError 등)
                        if retry_count < 2:
                            time.sleep(0.5)
                            continue
                        else:
                            logger.error(f"API 요청 중 예외 발생 (3회 시도 실패): {req_e}")
                            return None

                # Inner Loop가 'break'로 끝났는지 확인 (토큰 갱신 필요)
                # 토큰이 클리어 되었다면 Outer Loop가 다시 돌면서 새 토큰을 받음
                if not self.token_manager.get_existing_token("KR", self._mode): 
                     continue
                
                # 토큰 문제가 아니었는데 Inner Loop가 끝났다면 3회 실패한 것이므로 종료
                return None

            except Exception as e:
                logger.error(f"API 요청 프로세스 중 치명적 오류: {e}")
                return None
        
        return None
    
    def get_current_price(self, code: str) -> Optional[Dict]:
        # KIS API를 이용한 현재가 조회 구현
        info = self.get_stock_price_info(code)
        if info:
            return {
                'price': info.get('stck_clpr', 0),
                'open': info.get('stck_oprc', 0),
                'high': info.get('stck_hgpr', 0),
                'low': info.get('stck_lwpr', 0),
                'volume': info.get('acml_vol', 0),
                'change': 0 # 변동률 등은 필요 시 계산
            }
        return None
    
    def get_stock_price_info(self, stock_code: str) -> Optional[Dict]:
        """종목 상세정보 조회 (KIS API)"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code}
        
        data = self._call_api("FHKST01010100", url, params=params)
        
        if data and data.get('rt_cd') == '0':
            o = data.get('output', {})
            # 1. KIS API에서 배당수익률 추출
            dividend_yield = self._float(o.get('dvid_dfct_rt'))

            return {
                'bstp_kor_isnm': o.get('bstp_kor_isnm'),
                # --- [기본 시세 및 거래량] ---
                'stck_clpr': self._int(o.get('stck_prpr')),       # 현재가
                'stck_oprc': self._int(o.get('stck_oprc')),       # 시가
                'stck_hgpr': self._int(o.get('stck_hgpr')),       # 고가
                'stck_lwpr': self._int(o.get('stck_lwpr')),       # 저가
                'acml_vol': self._int(o.get('acml_vol')),         # 누적거래량
                
                # --- [밸류에이션] ---
                'per': self._float(o.get('per')),
                'pbr': self._float(o.get('pbr')),
                'eps': self._float(o.get('eps')),
                'bps': self._float(o.get('bps')),
                'hts_avls': self._int(o.get('hts_avls')),         # 시가총액
                'lstn_stcn': self._int(o.get('lstn_stcn')),       # 상장주식수
                
                # --- [52주 및 연중 최고/최저] ---
                'w52_hgpr': self._int(o.get('w52_hgpr')),
                'w52_hgpr_date': o.get('w52_hgpr_date', ''),
                'w52_lwpr': self._int(o.get('w52_lwpr')),
                'w52_lwpr_date': o.get('w52_lwpr_date', ''),
                'stck_dryy_hgpr': self._int(o.get('stck_dryy_hgpr')),
                'stck_dryy_lwpr': self._int(o.get('stck_dryy_lwpr')),
                'dryy_hgpr_vrss_prpr_rate': self._float(o.get('dryy_hgpr_vrss_prpr_rate')),
                'dryy_lwpr_vrss_prpr_rate': self._float(o.get('dryy_lwpr_vrss_prpr_rate')),
                
                # --- [수급 정보] ---
                'hts_frgn_ehrt': self._float(o.get('hts_frgn_ehrt')), # 외국인소진율
                'frgn_hldn_qty': self._int(o.get('frgn_hldn_qty')),   # 외국인보유수량
                'dividend_yield': dividend_yield,
                
                # --- [추가 적용] 리스크 관리 및 보조 지표 ---
                # 1. 신용 잔고율 (Risk: 높으면 위험)
                'loan_rate': self._float(o.get('whol_loan_rmnd_rate')),
                
                # 2. 종목 상태 (Risk: 51=관리, 58=정지 등. 00=정상)
                'stat_code': o.get('iscd_stat_cls_code', '00'),
                
                # 3. 단기 과열 여부 (Y/N)
                'is_short_over': o.get('short_over_yn', 'N'),
                
                # 4. 거래량 회전율 (Activity: 수급 활성도)
                'vol_turnover': self._float(o.get('vol_tnrt')),
                
                # 5. 피벗 지지/저항 (기술적 분석 보조)
                'pvt_res': self._int(o.get('dmrs_val')), # 저항
                'pvt_res1': self._int(o.get('pvt_frst_dmrs_prc')), # 1차 저항
                'pvt_res2': self._int(o.get('pvt_scnd_dmrs_prc')), # 2차 저항 (목표가)
                'pvt_sup': self._int(o.get('dmsp_val')), # 지지
                'pvt_sup1': self._int(o.get('pvt_frst_dmsp_prc')), # 1차 지지 (손절/매수)
                'pvt_sup2': self._int(o.get('pvt_scnd_dmsp_prc')), # 2차 지지
                'pvt': self._int(o.get('pvt_pont_val')), # 피벗
            }
        return None
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        """종목 상세정보 조회 (KIS API + FnGuide 보완)"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code}
        
        data = self._call_api("FHKST01010100", url, params=params)
        
        if data and data.get('rt_cd') == '0':
            o = data.get('output', {})
            
            # 1. KIS API에서 배당수익률 추출
            dividend_yield = self._float(o.get('dvid_dfct_rt'))
            
            # 2. [보완] KIS 데이터가 없으면(0이면) FnGuide 크롤링 시도
            if dividend_yield == 0:
                try:
                    # 안전망 데이터 수집 메서드 재활용 (배당수익률 포함됨)
                    # 주의: 웹 크롤링이 포함되므로 대량 조회 시 속도가 느려질 수 있음
                    fn_data = self.fnguide.get_financial_safety_data(stock_code)
                    if fn_data:
                        dividend_yield = fn_data.get('dividend_yield', 0.0)
                except Exception:
                    pass  # 크롤링 실패 시 0으로 유지

            return {
                'bstp_kor_isnm': o.get('bstp_kor_isnm'),
                # --- [기본 시세 및 거래량] ---
                'stck_clpr': self._int(o.get('stck_prpr')),       # 현재가
                'stck_oprc': self._int(o.get('stck_oprc')),       # 시가
                'stck_hgpr': self._int(o.get('stck_hgpr')),       # 고가
                'stck_lwpr': self._int(o.get('stck_lwpr')),       # 저가
                'acml_vol': self._int(o.get('acml_vol')),         # 누적거래량
                
                # --- [밸류에이션] ---
                'per': self._float(o.get('per')),
                'pbr': self._float(o.get('pbr')),
                'eps': self._float(o.get('eps')),
                'bps': self._float(o.get('bps')),
                'hts_avls': self._int(o.get('hts_avls')),         # 시가총액
                'lstn_stcn': self._int(o.get('lstn_stcn')),       # 상장주식수
                
                # --- [52주 및 연중 최고/최저] ---
                'w52_hgpr': self._int(o.get('w52_hgpr')),
                'w52_hgpr_date': o.get('w52_hgpr_date', ''),
                'w52_lwpr': self._int(o.get('w52_lwpr')),
                'w52_lwpr_date': o.get('w52_lwpr_date', ''),
                'stck_dryy_hgpr': self._int(o.get('stck_dryy_hgpr')),
                'stck_dryy_lwpr': self._int(o.get('stck_dryy_lwpr')),
                'dryy_hgpr_vrss_prpr_rate': self._float(o.get('dryy_hgpr_vrss_prpr_rate')),
                'dryy_lwpr_vrss_prpr_rate': self._float(o.get('dryy_lwpr_vrss_prpr_rate')),
                
                # --- [수급 정보] ---
                'hts_frgn_ehrt': self._float(o.get('hts_frgn_ehrt')), # 외국인소진율
                'frgn_hldn_qty': self._int(o.get('frgn_hldn_qty')),   # 외국인보유수량
                'dividend_yield': dividend_yield,
                
                # --- [추가 적용] 리스크 관리 및 보조 지표 ---
                # 1. 신용 잔고율 (Risk: 높으면 위험)
                'loan_rate': self._float(o.get('whol_loan_rmnd_rate')),
                
                # 2. 종목 상태 (Risk: 51=관리, 58=정지 등. 00=정상)
                'stat_code': o.get('iscd_stat_cls_code', '00'),
                
                # 3. 단기 과열 여부 (Y/N)
                'is_short_over': o.get('short_over_yn', 'N'),
                
                # 4. 거래량 회전율 (Activity: 수급 활성도)
                'vol_turnover': self._float(o.get('vol_tnrt')),
                
                # 5. 피벗 지지/저항 (기술적 분석 보조)
                'pvt_res': self._int(o.get('dmrs_val')), # 저항
                'pvt_res1': self._int(o.get('pvt_frst_dmrs_prc')), # 1차 저항
                'pvt_res2': self._int(o.get('pvt_scnd_dmrs_prc')), # 2차 저항 (목표가)
                'pvt_sup': self._int(o.get('dmsp_val')), # 지지
                'pvt_sup1': self._int(o.get('pvt_frst_dmsp_prc')), # 1차 지지 (손절/매수)
                'pvt_sup2': self._int(o.get('pvt_scnd_dmsp_prc')), # 2차 지지
                'pvt': self._int(o.get('pvt_pont_val')), # 피벗
            }
        return None

    def get_period_prices(self, stock_code: str, start_date: str, end_date: str) -> Optional[List[Dict]]:
        """기간별 시세 조회"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": start_date, "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"
        }
        
        data = self._call_api("FHKST03010100", url, params=params)
        
        if data and data.get('rt_cd') == '0':
            result = []
            items = data.get('output2', [])
            for item in items:
                if not item.get('stck_bsop_date'): continue
                result.append({
                    'stck_bsop_date': item.get('stck_bsop_date'),
                    'stck_clpr': self._int(item.get('stck_clpr')),
                    'stck_oprc': self._int(item.get('stck_oprc')),
                    'stck_hgpr': self._int(item.get('stck_hgpr')),
                    'stck_lwpr': self._int(item.get('stck_lwpr')),
                    'acml_vol': self._int(item.get('acml_vol')),
                    'prdy_vrss': self._int(item.get('prdy_vrss')),
                })
            return result
        return None

    def get_investor_trading(self, stock_code: str) -> Optional[Dict]:
        """투자자 매매동향"""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-investor"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code}
        
        data = self._call_api("FHKST01010900", url, params=params)
        
        if data and data.get('rt_cd') == '0':
            output_list = data.get('output', [])
            if output_list and isinstance(output_list, list) and len(output_list) > 0:
                latest_data = output_list[0]
                return {
                    'frgn_ntby_qty': self._int(latest_data.get('frgn_ntby_qty')),
                    'orgn_ntby_qty': self._int(latest_data.get('orgn_ntby_qty')),
                    'prsn_ntby_qty': self._int(latest_data.get('prsn_ntby_qty')),
                }
        return None
    
    def get_safe_deposit(self, summary):
        """
        미수 방지를 위한 안전한 예수금 계산 (D+2 기준)
        """
        # 1. API에서 값 가져오기 (문자열 -> 정수 변환)
        d2_deposit = int(summary.get('prvs_rcdl_excc_amt', '0')) # D+2 (가수도/추정)
        d1_deposit = int(summary.get('nxdy_excc_amt', '0'))      # D+1 (익일)
        d0_deposit = int(summary.get('dnca_tot_amt', '0'))       # D+0 (현재)

        # 2. [최우선] 주문가능금액 필드가 있다면 이걸 쓰는게 무조건 맞습니다.
        # (한국투자증권 기준: ord_psbl_amt / nrcvb_buy_amt 등)
        if 'ord_psbl_amt' in summary:
            return int(summary['ord_psbl_amt'])

        # 3. [차선] D+2 예수금 사용 (가장 안전)
        # D+2 데이터가 존재한다면(API 오류가 아니라면), 0원이든 아니든 이 값이 내 진짜 돈입니다.
        # 단, API에 따라 데이터가 아예 누락된 경우를 대비해 None 체크 정도만 합니다.
        if summary.get('prvs_rcdl_excc_amt') is not None:
            return d2_deposit
            
        return d0_deposit

    def get_account_balance(self, account_no: str, account_cd: str) -> Optional[Dict]:
        """주식 잔고 및 예수금 조회"""
        tr_id = "TTTC8434R" if not self.is_mock else "VTTC8434R"
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
        params = {
            "CANO": account_no, "ACNT_PRDT_CD": account_cd, "AFHR_FLPR_YN": "N",
            "OFL_YN": "", "INQR_DVSN": "01", "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""
        }
        
        data = self._call_api(tr_id, url, params=params)
        
        if data:
            if data.get('rt_cd') == '0':
                output2 = data.get('output2', [])
                output1 = data.get('output1', [])
                if output2:
                    summary = output2[0]
                    deposit = self.get_safe_deposit(summary)
                    total_eval = int(summary.get('tot_evlu_amt', '0'))
                    profit = int(summary.get('evlu_pfls_smtl_amt', '0'))
                    buy_amt = int(summary.get('pchs_amt_smtl_amt', '0'))
                    profit_rate = (profit / buy_amt) * 100 if buy_amt > 0 else 0.0
                    return {
                        'deposit': deposit, 'total_eval': total_eval, 'profit': profit,
                        'profit_rate': profit_rate, 'holdings_count': len(output1),
                        'holdings': output1
                    }
            else:
                logger.error(f"잔고조회 실패: {data.get('msg1')}")
        return None

    # 관심종목 그룹 조회 (국내주식-204)
    def get_kis_favorite_groups(self) -> Optional[List[Dict]]:
        """
        KIS 관심종목 그룹 조회 API
        - TR_ID: HHKCM113004C7 (실전투자 API 전용).xlsx - 관심종목 그룹조회.csv]
        """
        # 계좌가 아닌 'API 모드(도메인)'가 Mock인지 체크
        if self.is_mock:
            logger.warning("관심종목 그룹조회는 실전투자 API(도메인) 환경에서만 지원됩니다.")
            return []

        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/intstock-grouplist"
        tr_id = "HHKCM113004C7"
        
        # 문서 기준 파라미터 (FID_ETC_CLS_CODE: "00")
        params = {
            "TYPE": "1", 
            "FID_ETC_CLS_CODE": "00", 
            "USER_ID" : self.hts_user_id,
        }
        
        data = self._call_api(tr_id, url, params=params)
        
        if data and data.get('rt_cd') == '0':
            # 응답 필드: inter_grp_code(그룹코드), inter_grp_name(그룹명)
            return data.get('output2', [])
        return None

    # 관심종목 그룹별 종목 조회 (국내주식-203)
    def get_kis_group_stocks(self, group_code: str) -> Optional[List[Dict]]:
        """
        KIS 관심종목 그룹별 종목 리스트 조회 API
        - TR_ID: HHKCM113004C6 (실전투자 API 전용).xlsx - 관심종목 그룹별 종목조회.csv]
        """
        if self.is_mock:
            return []

        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/intstock-stocklist-by-group"
        tr_id = "HHKCM113004C6"
        
        # 문서 기준 파라미터 (FID_INTER_GRP_CODE)
        params = {
            "TYPE" : "1",
            "USER_ID" : self.hts_user_id,
            "DATA_RANK" : "",
            "INTER_GRP_CODE" : group_code,
            "INTER_GRP_NAME" : "",
            "HTS_KOR_ISNM" : "",
            "CNTG_CLS_CODE" : "",
            "FID_ETC_CLS_CODE" : "4"
        }
        
        data = self._call_api(tr_id, url, params=params)
        
        if data and data.get('rt_cd') == '0':
            # 응답 필드: jong_code(종목코드), hts_kor_isnm(종목명)
            return data.get('output2', [])
        return None
    
    def send_order(self, order_type: str, stock_code: str, qty: int, price: int, account_no: str, account_cd: str) -> Dict:
        """주식 주문 (매수/매도)"""
        # TR ID 결정
        if self.is_mock:
            tr_id = "VTTC0012U" if order_type == 'buy' else "VTTC0011U"
        else:
            tr_id = "TTTC0012U" if order_type == 'buy' else "TTTC0011U"
            
        # 주문 구분 (00: 지정가, 01: 시장가)
        ord_dvsn = "01" if price == 0 else "00"
        ord_unpr = "0" if price == 0 else str(price)
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash"
        
        body = {
            "CANO": account_no,
            "ACNT_PRDT_CD": account_cd,
            "PDNO": stock_code,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(qty),
            "ORD_UNPR": ord_unpr
        }
        
        data = self._call_api(tr_id, url, body=body, method='POST')
        
        if data:
            if data.get('rt_cd') == '0':
                return {
                    'success': True,
                    'message': data.get('msg1', '주문 완료'),
                    'order_no': data.get('output', {}).get('ODNO')
                }
            else:
                msg = data.get('msg1', '알 수 없는 오류')
                logger.error(f"주문 실패: {msg}")
                return {'success': False, 'message': msg}
        else:
            return {'success': False, 'message': 'API 호출 오류 (토큰/통신)'}

    def _int(self, v) -> int:
        if v is None: return 0
        try: return int(str(v).replace(',', ''))
        except: return 0
    
    def _float(self, v) -> float:
        if v is None: return 0.0
        try: return float(str(v).replace(',', ''))
        except: return 0.0

    def check_buy_limit(self, account_no: str, account_cd: str, stock_code: str, use_margin: bool = False) -> Optional[Dict]:
        """
        주식 매수 가능 조회 (inquire-psbl-order)
        - 미수 사용 여부에 따라 조회 필드가 달라짐
        - 증거금률 반영을 위해 시장가(01) 기준으로 조회
        """
        # 1. TR ID 및 URL 설정 (실전/모의 구분)
        tr_id = "TTTC8908R" if not self.is_mock else "VTTC8908R"
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
        
        # 2. 파라미터 구성
        # ORD_DVSN: '01'(시장가)로 해야 증거금률이 반영된 정확한 수량이 나옴
        params = {
            "CANO": account_no,
            "ACNT_PRDT_CD": account_cd,
            "PDNO": stock_code,
            "ORD_UNPR": "",       # 시장가 조회 시 단가 공란
            "ORD_DVSN": "01",     # 01: 시장가 (필수)
            "CMA_EVLU_AMT_ICLD_YN": "N",
            "OVRS_ICLD_YN": "N"
        }
        
        # 3. API 호출 (_call_api 메서드 활용)
        data = self._call_api(tr_id, url, params=params)
        
        # 4. 응답 처리
        if data:
            if data.get('rt_cd') == '0':
                output = data.get('output', {})
                
                if output:
                    # 미수 사용 여부에 따라 참조할 필드 결정
                    if not use_margin:
                        # [안전] 미수 미사용 (nrcvb: Non-receivable)
                        max_qty = int(output.get('nrcvb_buy_qty', '0'))
                        possible_amt = int(output.get('nrcvb_buy_amt', '0'))
                    else:
                        # [위험] 미수 사용 (max: 증거금 100% 활용)
                        max_qty = int(output.get('max_buy_qty', '0'))
                        possible_amt = int(output.get('max_buy_amt', '0'))
                    
                    return {
                        'max_qty': max_qty,         # 주문 가능 수량
                        'possible_amt': possible_amt, # 주문 가능 금액
                        'raw_output': output        # (필요시 참조용) 원본 데이터
                    }
            else:
                # 실패 시 메시지 로깅
                logger.error(f"매수가능조회 실패: {data.get('msg1')} (Code: {data.get('msg_cd')})")
                
        return None
