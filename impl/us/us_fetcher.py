import logging
import requests
from typing import Optional, Dict, List
import time
from config.settings import get_settings_manager
from utils.token_manager import get_token_manager
from config.database import get_session, ItemMst
import yfinance as yf

logger = logging.getLogger(__name__)

class UsFetcher():
    """
    미국 주식 전용 Fetcher (KIS 해외주식 API)
    """
    def __init__(self, mode: str = None):
        self.settings_manager = get_settings_manager()
        self.token_manager = get_token_manager()
        self._mode = mode
        self._load_api_config()

    def _load_api_config(self):
        """API 설정 로드 (국내와 동일한 Key 사용 가정)"""
        api_settings = self.settings_manager.settings.api
        if self._mode is None: 
            self._mode = getattr(api_settings, 'kis_api_mode_us', 'real') # 안전한 접근
            
        logger.info(f"[UsFetcher] API 초기화 모드: {self._mode}")

        self.hts_user_id = api_settings.hts_user_id

        if self._mode == "real":
            self.app_key = api_settings.kis_real_app_key_us
            self.app_secret = api_settings.kis_real_app_secret_us
            self.base_url = "https://openapi.koreainvestment.com:9443"
            self.is_mock = False
        else:
            self.app_key = api_settings.kis_mock_app_key_us
            self.app_secret = api_settings.kis_mock_app_secret_us
            self.base_url = "https://openapivts.koreainvestment.com:29443"
            self.is_mock = True

    def is_configured(self) -> bool:
        return bool(self.app_key and self.app_secret)

    def get_access_token(self) -> Optional[str]:
        if not self.is_configured(): return None
        return self.token_manager.get_token("US", self._mode, self.app_key, self.app_secret, self.base_url)

    def _get_headers(self, tr_id: str) -> Dict:
        token = self.get_access_token()
        if not token: return {}
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id
        }

    def _call_api(self, tr_id: str, url: str, params: dict = None, body: dict = None, method: str = 'GET') -> Optional[Dict]:
        """
        [수정됨] API 호출 공통 메서드 
        1. 토큰 만료 시 자동 갱신 (Outer Loop)
        2. 통신 오류 또는 비정상 응답 시 3회 재시도 (Inner Loop)
        """
        if not self.is_configured(): return None

        time.sleep(0.5)

        # [Outer Loop] 최대 2회 시도 (1회 실패 -> 토큰 갱신 -> 2회 재시도)
        for attempt in range(2):
            try:
                headers = self._get_headers(tr_id)
                if not headers: 
                    logger.error("헤더 생성 실패 (토큰 없음)")
                    return None
                
                # [Inner Loop] 통신/데이터 수신 재시도 (3회)
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
                                self.token_manager.clear_token("US", self._mode)
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
                if not self.token_manager.get_existing_token("US", self._mode): 
                     continue
                
                # 토큰 문제가 아니었는데 Inner Loop가 끝났다면 3회 실패한 것이므로 종료
                return None

            except Exception as e:
                logger.error(f"API 요청 프로세스 중 치명적 오류: {e}")
                return None
        
        return None
    
    def format_ticker_for_yfinance(self, ticker):
        """
        yfinance용 티커 포맷 변환
        예: 'BRK/B' -> 'BRK-B', 'BF.B' -> 'BF-B'
        """
        if not ticker:
            return ticker
            
        # 특수문자를 하이픈(-)으로 변경
        # 보통 BRK.B 또는 BRK/B 형태로 들어오는 경우가 많음
        return ticker.replace("/", "-").replace(".", "-")

    # [Helper] 거래소 코드 변환, 가격 조회용
    def _get_exchange_code(self, symbol: str) -> str:
        """
        종목 코드로 거래소 코드 조회, 가격 조회용
        - DB(ItemMst)에서 mrkt_ctg 조회
        NAS : 나스닥
        NYS : 뉴욕
        AMS : 아멕스
        """
        try:
            with get_session() as session:
                # 1. DB에서 해당 종목의 시장 구분(mrkt_ctg) 조회
                result = session.query(ItemMst.mrkt_ctg).filter(
                    ItemMst.item_cd == symbol
                ).first()

                # 2. 데이터가 존재하는 경우 로직 처리
                if result and result.mrkt_ctg:
                    market = result.mrkt_ctg.upper() # 대문자 변환 안전장치
                    
                    if market == "NASDAQ":
                        return "NAS"
                    elif market == "NYSE":
                        return "NYS"
                    elif market == "AMEX":
                        return "AMS"
                    else:
                        return market
        except Exception as e:
            pass

        # 2. DB에 정보가 없는 경우 yfinance로 조회
        try:
            ticker = yf.Ticker(self.format_ticker_for_yfinance(symbol))
            # 네트워크 요청 발생 (속도가 느릴 수 있음)
            # fast_info를 사용하면 info보다 빠름 (최신 yfinance 버전 권장)
            try:
                exchange = ticker.fast_info.get('exchange')
            except:
                # fast_info 실패 시 일반 info 사용
                exchange = ticker.info.get('exchange')
                
            if exchange:
                exchange = exchange.upper()
                
                # Yahoo Finance 거래소 코드 -> API 거래소 코드 매핑
                if exchange in ['NMS', 'NGM', 'NCM', 'NASDAQ']:
                    return "NAS"
                elif exchange in ['NYQ', 'NYSE']:
                    return "NYS"
                elif exchange in ['ASE', 'AMEX', 'NCM', 'NGM']: # ASE: NYSE American (구 AMEX)
                    return "AMS"
                
                # 그 외 (OTC 등)는 기본적으로 NAS 처리하거나 필요시 추가 분기
                return "NAS"
                
        except Exception as e:
            # yfinance 조회조차 실패한 경우
            # logger.error(f"YFinance 조회 실패 ({symbol}): {e}")
            pass

        # 3. 모든 조회 실패 시 기본값
        return "NASD"
        
    # [Helper] 거래소 코드 변환, 주문용
    def _get_exchange_code2(self, symbol: str) -> str:
        """
        종목 코드로 거래소 코드 조회, 주문용
        - DB(ItemMst)에서 mrkt_ctg 조회
        - NASDAQ이면 'NASD' 리턴, 그 외에는 값 그대로 리턴
        NASD : 나스닥
        NYSE : 뉴욕
        AMEX : 아멕스
        """
        # 1. DB에서 조회
        try:
            with get_session() as session:
                result = session.query(ItemMst.mrkt_ctg).filter(
                    ItemMst.item_cd == symbol
                ).first()

                if result and result.mrkt_ctg:
                    market = result.mrkt_ctg.upper()
                    if market == "NASDAQ":
                        return "NASD"
                    else:
                        return market
                        
        except Exception as e:
            # DB 에러 시 로그만 남기고 다음 단계(yf)로 진행
            pass 
            # logger.warning(f"DB 조회 실패, YF로 시도 ({symbol}): {e}")

        # 2. DB에 정보가 없는 경우 yfinance로 조회
        try:
            ticker = yf.Ticker(self.format_ticker_for_yfinance(symbol))
            # 네트워크 요청 발생 (속도가 느릴 수 있음)
            # fast_info를 사용하면 info보다 빠름 (최신 yfinance 버전 권장)
            try:
                exchange = ticker.fast_info.get('exchange')
            except:
                # fast_info 실패 시 일반 info 사용
                exchange = ticker.info.get('exchange')
                
            if exchange:
                exchange = exchange.upper()
                
                # Yahoo Finance 거래소 코드 -> API 거래소 코드 매핑
                if exchange in ['NMS', 'NGM', 'NCM', 'NASDAQ']:
                    return "NASD"
                elif exchange in ['NYQ', 'NYSE']:
                    return "NYSE"
                elif exchange in ['ASE', 'AMEX', 'NCM', 'NGM']: # ASE: NYSE American (구 AMEX)
                    return "AMEX"
                
                # 그 외 (OTC 등)는 기본적으로 NASD 처리하거나 필요시 추가 분기
                return "NASD"
                
        except Exception as e:
            # yfinance 조회조차 실패한 경우
            # logger.error(f"YFinance 조회 실패 ({symbol}): {e}")
            pass

        # 3. 모든 조회 실패 시 기본값
        return "NASD"
    
    # [Helper] 안전한 변환 함수
    def _safe_float(self, val):
        """빈 문자열이나 None을 0.0으로 변환"""
        if not val or val == '': 
            return 0.0
        try:
            return float(val)
        except ValueError:
            return 0.0

    def _safe_int(self, val):
        """빈 문자열이나 None을 0으로 변환"""
        if not val or val == '': 
            return 0
        try:
            # "10.5" 같은 문자열이 올 경우를 대비해 float 변환 후 int
            return int(float(val)) 
        except ValueError:
            return 0

    def get_current_price_market(self, code: str, excd: str) -> Optional[Dict]:
        """미국 주식 현재가 조회"""
        # API: 해외주식 현재체결가
        tr_id = "HHDFS76200200" if self._mode == "real" else "HHDFS76200200" 
        
        if self._mode != "real":
            # 모의투자는 해외주식 시세 조회가 제한적일 수 있어 가상 데이터 반환 가능성 염두
            pass 

        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price-detail"
        
        params = {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": code
        }
        
        # 해외주식 현재가 TR: HHDFS76200200 (실전)
        data = self._call_api(tr_id, url, params=params)
        
        if data and data.get('rt_cd') == '0':
            output = data.get('output', {})
            # [수정] 헬퍼 함수 사용하여 안전하게 변환
            return {
                'price': self._safe_float(output.get('last')),  # USD
                'open': self._safe_float(output.get('open')),
                'high': self._safe_float(output.get('high')),
                'low': self._safe_float(output.get('low')),
                'volume': self._safe_int(output.get('tvol')),
                'change': self._safe_float(output.get('rate'))
            }
        return None

    def get_current_price(self, code: str) -> Optional[Dict]:
        """미국 주식 현재가 조회"""
        # API: 해외주식 현재체결가
        tr_id = "HHDFS76200200" if self._mode == "real" else "HHDFS76200200" 
        
        if self._mode != "real":
            # 모의투자는 해외주식 시세 조회가 제한적일 수 있어 가상 데이터 반환 가능성 염두
            pass 

        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price-detail"
        excd = self._get_exchange_code(code)
        
        params = {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": code
        }
        
        # 해외주식 현재가 TR: HHDFS76200200 (실전)
        data = self._call_api(tr_id, url, params=params)
        
        if data and data.get('rt_cd') == '0':
            output = data.get('output', {})
            # [수정] 헬퍼 함수 사용하여 안전하게 변환
            return {
                'price': self._safe_float(output.get('last')),  # USD
                'open': self._safe_float(output.get('open')),
                'high': self._safe_float(output.get('high')),
                'low': self._safe_float(output.get('low')),
                'volume': self._safe_int(output.get('tvol')),
                'change': self._safe_float(output.get('rate'))
            }
        return None

    def get_account_balance(self, account_no: str, account_cd: str) -> Optional[Dict]:
        """
        미국 주식 잔고 및 자산 현황 조회 (체결기준현재잔고 API 통합 사용)
        - TR_ID: CTRP6504R (실전) / VTRP6504R (모의)
        - 외화(USD) 기준으로 데이터 조회 및 파싱
        """
        
        tr_id = "CTRP6504R" if self._mode == "real" else "VTRP6504R"
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-present-balance"
        
        # WCRC_FRCR_DVSN_CD: 01(원화), 02(외화)
        params = {
            "CANO": account_no,
            "ACNT_PRDT_CD": account_cd,
            "WCRC_FRCR_DVSN_CD": "02",  # 외화(USD) 기준 요청
            "NATN_CD": "840",           # 미국
            "TR_MKET_CD": "00",         
            "INQR_DVSN_CD": "00"        
        }
        
        data = self._call_api(tr_id, url, params=params)
        
        deposit = 0.0
        total_eval_amt = 0.0
        total_profit_amt = 0.0
        total_profit_rate = 0.0
        merged_holdings = []
        
        # 합계 검증용 변수
        calc_total_buy = 0.0
        calc_total_eval = 0.0
        
        if data and data.get('rt_cd') == '0':
            # ---------------------------------------------------------
            # [Part 1] 보유 종목 리스트 (output1)
            # ---------------------------------------------------------
            output1 = data.get('output1', [])
            for item in output1:
                # ccld_qty_smtl1: 체결수량합계 (보유수량)
                qty = self._safe_int(item.get('ccld_qty_smtl1'))
                
                if qty > 0:
                    # [필드명 수정] API 명세에 맞춰 필드명 변경
                    # frcr_evlu_amt2: 외화평가금액
                    eval_amt = self._safe_float(item.get('frcr_evlu_amt2'))
                    
                    # frcr_pchs_amt: 외화매입금액 (기존 frcr_pchs_amt1 아님)
                    buy_amt = self._safe_float(item.get('frcr_pchs_amt'))
                    
                    # ovrs_now_pric1: 해외현재가 (기존 now_pric2 아님)
                    current_price = self._safe_float(item.get('ovrs_now_pric1'))
                    
                    # evlu_pfls_rt1: 평가수익률
                    profit_rate = self._safe_float(item.get('evlu_pfls_rt1'))
                    
                    # 평단가 계산 (매입금액 / 수량)
                    avg_price = buy_amt / qty if qty > 0 else 0.0
                    
                    merged_holdings.append({
                        'pdno': item.get('pdno'),               # 종목코드 (ex: AAPL)
                        'prdt_name': item.get('prdt_name'),     # 종목명
                        'hldg_qty': qty,                        # 보유수량
                        'evlu_pfls_rt': profit_rate,            # 수익률
                        'pchs_avg_pric': avg_price,             # 매입단가 (USD)
                        'prpr': current_price,                  # 현재가 (USD)
                        'evlu_amt': eval_amt,                   # 평가금액 (USD)
                        'buy_amt': buy_amt,                     # 매입금액 (USD)
                        'market_code': "US"                     
                    })

            # ---------------------------------------------------------
            # [Part 2] 예수금 (output2)
            # - 실전/모의 상관없이 USD 예수금을 찾아서 설정
            # ---------------------------------------------------------
            output2 = data.get('output2', [])
            if isinstance(output2, list):
                for currency_info in output2:
                    # USD 통화의 예수금(frcr_drwg_psbl_amt_1) 확인
                    if currency_info.get('crcy_cd') == 'USD':
                        deposit = self._safe_float(currency_info.get('frcr_drwg_psbl_amt_1'))
                        break
            
            # ---------------------------------------------------------
            # [Part 3] 총 평가 현황 (output3)
            # ---------------------------------------------------------
            output3 = data.get('output3', {})
            # output3가 리스트로 올 경우 처리
            if isinstance(output3, list) and len(output3) > 0:
                output3 = output3[0]
            
            if output3:
                # frcr_evlu_tota: 외화평가총액
                total_eval_amt = self._safe_float(output3.get('evlu_amt_smtl'))
                total_profit_amt = self._safe_float(output3.get('evlu_pfls_amt_smtl'))
                total_profit_rate = self._safe_float(output3.get('evlu_erng_rt1'))
                
        return {
            'deposit': deposit,            
            'total_eval': total_eval_amt,   
            'profit': total_profit_amt,  
            'profit_rate': total_profit_rate,   
            'holdings': merged_holdings    
        }
    
    # 관심종목 그룹 조회 (국내주식-204)
    def get_kis_favorite_groups(self) -> Optional[List[Dict]]:
        """
        KIS 관심종목 그룹 조회 API
        - TR_ID: HHKCM113004C7 (실전투자 API 전용).xlsx - 관심종목 그룹조회.csv]
        """
        # [수정] 계좌가 아닌 'API 모드(도메인)'가 Mock인지 체크
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

    def send_order(self, order_type: str, stock_code: str, qty: int, price: float, account_no: str, account_cd: str) -> Dict:
        """미국 주식 주문 (매수/매도)"""
        # 매수: TTTS1002U, 매도: TTTT1006U (실전)
        is_buy = (order_type == 'buy')
        
        if self._mode == "real":
            tr_id = "TTTT1002U" if is_buy else "TTTT1006U"
        else:
            tr_id = "VTTS1002U" if is_buy else "VTTS1001U"
            
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/order"
        
        # [주의] 미국 주식은 시장가(00) 주문이 제한될 수 있음. 
        # 보통 지정가(00)를 사용. 시장가는 '01' 등이지만 여기선 지정가 기본으로 구현.
        ord_dvsn = "00" 
        
        body = {
            "CANO": account_no,              # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": "01",                 # 계좌상품코드 (보통 01)
            "OVRS_EXCG_CD": self._get_exchange_code2(stock_code), # 거래소코드 (NASD, NYSE, AMEX 등)
            "PDNO": stock_code,                   # 종목코드 (티커)
            "ORD_DVSN": ord_dvsn,                 # 주문구분 (00: 지정가, 01: 시장가)
            "ORD_QTY": str(int(qty)),             # 주문수량 (정수형 문자열)
            "OVRS_ORD_UNPR": f"{float(price):.2f}", # [핵심수정] 주문단가 (Key명 변경 및 소수점 포맷팅)
            "ORD_SVR_DVSN_CD": "0"                # 주문서버구분
        }
        
        data = self._call_api(tr_id, url, body=body, method='POST')
        
        if data:
            if data.get('rt_cd') == '0':
                return {'success': True, 'message': 'US 주문 전송 완료', 'order_no': data.get('output', {}).get('ODNO')}
            else:
                return {'success': False, 'message': data.get('msg1')}
        return {'success': False, 'message': 'API 호출 오류'}

    # 상세 정보 조회는 현재가 조회로 대체하거나 별도 구현
    def get_stock_info(self, code: str) -> Optional[Dict]:
        return self.get_current_price(code)
    
    def check_buy_limit_us(self, account_no: str, account_cd: str, stock_code: str, price: float) -> Optional[Dict]:
        """
        해외주식 매수 가능 금액 및 수량 조회 (inquire-psamount)
        
        Args:
            account_no (str): 계좌번호 앞 8자리
            account_cd (str): 계좌번호 뒤 2자리
            stock_code (str): 종목코드 (예: AAPL)
            price (float): 주문 희망 단가 (현재가) *필수*
            exchange_cd (str): 거래소 코드 (NASD, NYSE, AMEX) *필수*
            
        Returns:
            dict: {
                'max_qty': int,        # 외화 기준 최대 주문 가능 수량
                'possible_amt': float, # 외화 기준 주문 가능 금액 (USD)
                'max_qty_integ': int,  # (참고) 통합증거금 기준 최대 수량
                'raw_output': dict     # 원본 데이터
            }
        """
        # 1. TR ID 및 URL 설정 (실전/모의 구분)
        tr_id = "TTTS3007R" if not self.is_mock else "VTTS3007R"
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-psamount"
        
        excd = self._get_exchange_code2(stock_code)
        # 2. 파라미터 구성
        # [주의] OVRS_EXCG_CD: NASD(나스닥), NYSE(뉴욕), AMEX(아멕스) 정확히 입력 필요
        # [주의] OVRS_ORD_UNPR: 해외주식은 단가 입력이 필수입니다. (시장가 개념이라도 예상 체결가를 넣어야 함)
        params = {
            "CANO": account_no,
            "ACNT_PRDT_CD": account_cd,
            "OVRS_EXCG_CD": excd, 
            "OVRS_ORD_UNPR": str(price), # 주문단가 (String 변환)
            "ITEM_CD": stock_code
        }
        
        # 3. API 호출 (_call_api 메서드 활용)
        data = self._call_api(tr_id, url, params=params)
        
        # 4. 응답 처리
        if data:
            if data.get('rt_cd') == '0':
                output = data.get('output', {})
                
                if output:
                    # -----------------------------------------------------------
                    # [중요] 외화(USD) 기준 vs 통합증거금(Won+USD) 기준
                    # 보통 '외화' 기준으로 계산하는 것이 안전합니다.
                    # -----------------------------------------------------------
                    
                    # 1) 순수 외화(USD) 기준 (기본)
                    # max_ord_psbl_qty: 최대주문가능수량 (외화)
                    # ovrs_ord_psbl_amt: 해외주문가능금액 (외화)
                    max_qty = int(output.get('max_ord_psbl_qty', '0'))
                    possible_amt = float(output.get('ovrs_ord_psbl_amt', '0.0'))
                    
                    # 2) 통합증거금(원화 예수금 포함) 기준 (참고용)
                    # ovrs_max_ord_psbl_qty: 해외최대주문가능수량 (통합)
                    # frcr_ord_psbl_amt1: 외화주문가능금액1 (통합)
                    max_qty_integ = int(output.get('ovrs_max_ord_psbl_qty', '0'))
                    possible_amt_integ = float(output.get('frcr_ord_psbl_amt1', '0.0'))

                    return {
                        'max_qty': max_qty,         
                        'possible_amt': possible_amt, 
                        'max_qty_integ': max_qty_integ, # 통합증거금 사용 시 이 값 참조
                        'possible_amt_integ': possible_amt_integ,
                        'raw_output': output
                    }
            else:
                # 실패 시 메시지 로깅
                # 해외주식 API는 거래소 코드나 가격 형식이 틀리면 에러가 자주 발생함
                logger.error(f"해외주식 매수가능조회 실패: {data.get('msg1')} (Code: {data.get('msg_cd')})")
                
        return None