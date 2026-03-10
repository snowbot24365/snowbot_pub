import pandas as pd
import requests
import logging
import io
from typing import Dict, Optional
import time
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)

class FnGuideFetcher:
    """
    FnGuide 웹 크롤링을 통한 재무 데이터 수집기
    1. get_financial_safety_data: 안전망(FCF, 활동성, 배당, ROE) 평가용 데이터
    2. fetch_financial_statement: DART 데이터 누락 시 특정 연도의 재무제표 보완용
    """
    
    def __init__(self):
        # 봇 탐지 방지용 헤더
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    def _parse_fnguide_number(self, value):
        """
        FnGuide 숫자 문자열 파싱 (정규표현식 사용)
        - 입력: "2.79%", "1,234.5", "N/A", "-", "2.79 %"
        - 출력: 2.79, 1234.5, 0.0, 0.0, 2.79
        """
        try:
            if value is None: return 0.0
            
            # 문자열로 변환 및 콤마 제거
            str_val = str(value).replace(',', '')
            
            # 정규식으로 숫자(음수 포함, 소수점 포함)만 추출
            # -?: 마이너스 부호가 있을 수도 있음
            # \d+: 숫자 1개 이상
            # (\.\d+)?: 소수점과 그 뒤의 숫자가 있을 수도 있음
            match = re.search(r'-?\d+(\.\d+)?', str_val)
            
            if match:
                return float(match.group())
            
            return 0.0
        except Exception:
            return 0.0

    def get_financial_safety_data(self, stock_code: str) -> Optional[Dict]:
        """
        [평가용] 안전망 체크 데이터 수집
        - 대상: ROE, 배당수익률, FCF, 활동성, 지배주주지분
        """
        data = {
            'roe_avg': 0.0,
            'fcf_pass': False,
            'activity_pass': False,
            'equity': 0,
            'roe': 0.0,
            'dividend_yield': 0.0,
            'has_data': False
        }
        
        try:
            url_fin = f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock_code}&NewMenuID=103"
            url_ratio = f"https://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A{stock_code}&NewMenuID=104"
            url_main = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{stock_code}&NewMenuID=101"

            # =========================================================
            # 인덱스 검색 헬퍼 함수 (내부 함수로 정의)
            # =========================================================
            def find_series(df, keyword):
                """
                DataFrame 인덱스에서 keyword가 포함된 행(Series)을 반환
                예: 'ROE' 검색 -> 'ROE계산에 참여한 계정 펼치기' 행 반환
                """
                for idx in df.index:
                    # 인덱스가 문자열이고 키워드를 포함하면 해당 행 반환
                    if isinstance(idx, str) and keyword in idx:
                        return df.loc[idx]
                return None

            # ---------------------------------------------------------
            # A. 재무제표 파싱 (자본, 현금흐름)
            # ---------------------------------------------------------
            resp_fin = requests.get(url_fin, headers=self.headers)
            dfs_fin = pd.read_html(io.StringIO(resp_fin.text))
            
            if len(dfs_fin) > 2:
                # 재무상태표
                df_bs = dfs_fin[2].set_index(dfs_fin[2].columns[0])
                # '지배주주지분' 찾기 (find_series 활용)
                equity_series = find_series(df_bs, '지배주주지분')
                if equity_series is not None:
                    # 최근 결산값 사용 (보통 끝에서 2번째)
                    data['equity'] = self._parse_fnguide_number(equity_series.iloc[-2]) * 100_000_000

            if len(dfs_fin) > 4:
                # 현금흐름표
                df_cf = dfs_fin[4].set_index(dfs_fin[4].columns[0])
                
                # '영업활동', '투자활동' 키워드로 검색
                cf_oa_series = find_series(df_cf, '영업활동')
                cf_ia_series = find_series(df_cf, '투자활동')
                
                cf_oa = 0; cf_ia = 0
                if cf_oa_series is not None:
                    cf_oa = self._parse_fnguide_number(cf_oa_series.iloc[-2])
                if cf_ia_series is not None:
                    cf_ia = self._parse_fnguide_number(cf_ia_series.iloc[-2])
                
                # FCF 판별
                if (cf_oa - abs(cf_ia)) > 0:
                    data['fcf_pass'] = True

            # ---------------------------------------------------------
            # B. 재무비율 파싱 (ROE, 활동성)
            # ---------------------------------------------------------
            resp_ratio = requests.get(url_ratio, headers=self.headers)
            dfs_ratio = pd.read_html(io.StringIO(resp_ratio.text))
            
            if len(dfs_ratio) > 0:
                df_ratio = dfs_ratio[0].set_index(dfs_ratio[0].columns[0])
                
                # 1. ROE (키워드 검색)
                roe_series = find_series(df_ratio, 'ROE')
                if roe_series is not None:
                    valid_roes = []
                    count = 0
                    # 최근 4개년도 데이터 중 유효한 값 3개 수집
                    for i in range(2, 6):
                        if len(roe_series) < i: break
                        val = self._parse_fnguide_number(roe_series.iloc[-i])
                        if val != 0: 
                            valid_roes.append(val)
                            count += 1
                        if count >= 3: break
                    
                    if valid_roes:
                        data['roe_avg'] = sum(valid_roes) / len(valid_roes)
                        data['roe'] = valid_roes[0] # 가장 최신 값

                # 2. 활동성 - 총자산회전율 (키워드 검색)
                turnover_series = find_series(df_ratio, '총자산회전율')
                if turnover_series is not None:
                    if len(turnover_series) >= 3:
                        cur = self._parse_fnguide_number(turnover_series.iloc[-2])
                        prev = self._parse_fnguide_number(turnover_series.iloc[-3])
                        if cur > prev:
                            data['activity_pass'] = True

            # ---------------------------------------------------------
            # C. 배당수익률 파싱 (Snapshot 페이지 + BeautifulSoup)
            # ---------------------------------------------------------
            try:
                resp_main = requests.get(url_main, headers=self.headers)
                soup = BeautifulSoup(resp_main.text, 'html.parser')
                
                div_yield_val = 0.0
                
                div_elem = soup.select_one('#corp_group2 > dl:nth-child(5) > dd')
                if div_elem:
                    div_yield_val = self._parse_fnguide_number(div_elem.get_text())

                data['dividend_yield'] = div_yield_val
                    
            except Exception:
                pass

            data['has_data'] = True
            return data

        except Exception as e:
            # logger.debug(f"FnGuide 파싱 오류: {e}")
            return None

    def fetch_financial_statement(self, stock_code: str, target_year: int) -> Optional[Dict]:
        """
        [수집용] 특정 연도의 재무제표 데이터 보완 (DART 누락 시 호출)
        - 매출, 영업이익, 순이익, 자산, 자본, 영업현금, 투자현금
        - 단위: 원 (FnGuide '억원' -> '원' 변환)
        """
        try:
            url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock_code}&NewMenuID=103"
            resp = requests.get(url, headers=self.headers)
            dfs = pd.read_html(io.StringIO(resp.text))
            
            # FnGuide 테이블 구조: 
            # 0: 포괄손익계산서, 2: 재무상태표, 4: 현금흐름표 (연간)
            if len(dfs) < 5: return None
            
            data = {}
            target_col_idx = -1
            
            # 1. 해당 연도(target_year)가 있는 컬럼 인덱스 찾기
            # 컬럼 예시: ['IFRS(연결)', '2022/12', '2023/12', ...]
            header_row = dfs[0].columns
            for i, col_name in enumerate(header_row):
                if str(target_year) in str(col_name):
                    target_col_idx = i
                    break
            
            if target_col_idx == -1:
                return None  # 해당 연도 데이터 없음

            def get_val(df, row_name):
                # 첫번째 컬럼(항목명)을 인덱스로 설정
                df = df.set_index(df.columns[0])
                if row_name in df.index:
                    # iloc를 사용하여 해당 연도 컬럼의 값 추출
                    # (df.columns[target_col_idx] 사용 시 멀티인덱스 이슈 가능성 있어 iloc 사용 권장)
                    # 하지만 read_html 결과는 보통 단일 헤더이므로 컬럼명 접근 가능
                    # 여기서는 안전하게 iloc의 열 위치(target_col_idx) 사용
                    # read_html은 인덱스가 리셋된 상태이므로, set_index 후에는 열 개수가 1 줄어듦에 주의
                    # -> set_index 하기 전 원본에서 찾는게 더 안전할 수 있음.
                    # 하지만 편의상 set_index 했으니, target_col_idx는 전체 컬럼 기준이었으므로 -1 보정 필요할 수 있음.
                    # FnGuide read_html 결과의 columns는 Index 객체임.
                    
                    # 더 안전한 방법: 컬럼명으로 접근
                    col_name = header_row[target_col_idx]
                    val = df.loc[row_name][col_name] # Series or scalar
                    
                    return self._parse_fnguide_number(val) * 100_000_000 # 억원 -> 원
                return None

            # (1) 손익계산서 (dfs[0])
            data['revenue'] = get_val(dfs[0], '매출액')
            data['operating_profit'] = get_val(dfs[0], '영업이익')
            data['net_income'] = get_val(dfs[0], '당기순이익')
            
            # (2) 재무상태표 (dfs[2])
            data['total_assets'] = get_val(dfs[2], '자산총계')
            data['total_equity'] = get_val(dfs[2], '자본총계')
            
            # (3) 현금흐름표 (dfs[4])
            data['cf_oa'] = get_val(dfs[4], '영업활동으로인한현금흐름')
            data['cf_ia'] = get_val(dfs[4], '투자활동으로인한현금흐름')
            
            return data
            
        except Exception as e:
            logger.debug(f"FnGuide 재무제표 보완 실패 ({stock_code}/{target_year}): {e}")
            return None