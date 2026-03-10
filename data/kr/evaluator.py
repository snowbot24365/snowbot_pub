"""
종목 평가 로직 모듈 (통합 버전)
- Evaluator: 단일 종목 점수 계산기 (순수 로직)
- EvaluationService: 전 종목 일괄 평가 실행기 (DB 작업, 크롤링, 저장 담당)
"""

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Callable, List
from sqlalchemy import func

from config.settings import get_settings_manager
from config.database import (
    get_session, ItemMst, ItemPrice, FinancialSheet, ItemEquity, EvaluationResult
)
from data.kr.price_fetcher import BondYieldFetcher
from data.kr.fnguide_fetcher import FnGuideFetcher

logger = logging.getLogger(__name__)

# =========================================================
# 1. 데이터 구조 (DTO)
# =========================================================

@dataclass
class SwingData:
    """평가에 필요한 데이터 집합 (점수 계산용)"""
    item_cd: str
    item_nm: str = ""
    
    # 시세
    stck_clpr: int = 0
    ma5: int = 0
    ma20: int = 0
    ma60: int = 0
    ma120: int = 0
    
    # 재무
    grs: float = 0.0
    bsop_prfi_inrt: float = 0.0
    roe_val: float = 0.0
    lblt_rate: float = 999.0
    thtr_ntin: int = 0
    rsrv_rate: float = 0.0
    
    # 수급/기타
    acml_vol: int = 0       # 누적 거래량
    lstn_stcn: int = 1      # 상장 주식 수 (0 방지용 1)
    frgn_hldn_qty: int = 0  # 외국인 보유 수량
    frgn_ntby_qty: int = 0
    pgtr_ntby_qty: int = 0
    hts_avls: int = 0
    per: float = 0.0
    pbr: float = 0.0
    
    # 추세
    high_rate: float = -99.0
    low_rate: float = -99.0
    frgn_rate: float = 0.0

    # KPI 지표 (RSI, OBV)
    rsi_14: float = 50.0  # 기본값 50 (중립)
    obv_trend: int = 0    # 1:상승, -1:하락, 0:보합

@dataclass
class EvaluationScore:
    """평가 결과 점수"""
    item_cd: str
    item_nm: str
    total_score: int = 0
    
    sheet_score: int = 0
    trend_score: int = 0
    price_score: int = 0
    kpi_score: int = 0
    buy_score: int = 0
    avls_score: int = 0
    per_score: int = 0
    pbr_score: int = 0
    
    is_buy_candidate: bool = False

    # 안전망 결과
    srim_price: int = 0
    srim_pass: bool = False
    cashflow_pass: bool = False
    activity_pass: bool = False
    dividend_pass: bool = False
    roe_pass: bool = False


# =========================================================
# 2. 단일 종목 점수 계산기 (Evaluator) - 순수 로직
# =========================================================

class Evaluator:
    
    def evaluate(self, data: SwingData) -> EvaluationScore:
        score = EvaluationScore(item_cd=data.item_cd, item_nm=data.item_nm)

        settings = get_settings_manager().settings.evaluation.kr
        
        score.sheet_score = self._cal_sheet_score(data)
        score.trend_score = self._cal_trend_score(data)
        score.price_score = self._cal_price_score(data)
        score.kpi_score = self._cal_kpi_score(data)
        score.buy_score = self._cal_buy_score(data)
        score.avls_score = self._cal_avls_score(data)
        score.per_score = self._cal_per_score(data)
        score.pbr_score = self._cal_pbr_score(data)
        
        # 가중 총점
        weighted_sum = (
            score.sheet_score * settings.weight_sheet +
            score.trend_score * settings.weight_trend +
            score.price_score * settings.weight_price +
            score.kpi_score * settings.weight_kpi +
            score.buy_score * settings.weight_buy +
            score.avls_score * settings.weight_avls +
            score.per_score * settings.weight_per +
            score.pbr_score * settings.weight_pbr
        )

        # 가중치 합계
        total_weight = (
            settings.weight_sheet + settings.weight_trend + settings.weight_price +
            settings.weight_kpi + settings.weight_buy + settings.weight_avls +
            settings.weight_per + settings.weight_pbr
        )
        
        # 점수 정규화: (가중평균) * (항목 수 8)
        # 이렇게 하면 가중치를 3.0으로 높여도 만점은 항상 40점으로 유지됨
        if total_weight > 0:
            final_score = (weighted_sum / total_weight) * 8
        else:
            final_score = 0
            
        # 반올림하여 정수형 저장
        score.total_score = int(round(final_score))
        
        return score

    def _cal_sheet_score(self, d: SwingData) -> int:
        """재무 점수 계산 (설정값 기반 동적 적용)"""
        # 설정 가져오기
        settings = get_settings_manager().settings.evaluation.kr
        point = 0
        # 1. 매출액 증가율 > 설정값 (기본 10%)
        if d.grs > settings.threshold_grs: 
            point += 1
        # 2. 영업이익 증가율 > 설정값 (기본 10%)
        if d.bsop_prfi_inrt > settings.threshold_bsop_prfi_inrt: 
            point += 1
        # 3. 유보율 > 설정값 (기본 500%)
        if d.rsrv_rate > settings.threshold_rsrv_rate: 
            point += 1
        # 4. 부채비율 < 설정값 (기본 200%)
        if d.lblt_rate < settings.threshold_lblt_rate: 
            point += 1
        # 5. 당기순이익 흑자
        if d.thtr_ntin > 0: 
            point += 1
        return point

    def _cal_price_score(self, d: SwingData) -> int:
        """
        고가/저가 괴리율 점수 (기준값 + 간격 방식)
        - High Rate: 하락폭이 클수록(저평가) 높은 점수
        - Low Rate: 바닥 대비 상승폭이 클수록(고평가) 감점 (Penalty)
        """
        settings = get_settings_manager().settings.evaluation.kr
        
        # -----------------------------------------------------
        # 1. 연중 최고가 대비 현재가 비율 (High Price Score)
        # -----------------------------------------------------
        score = 0
        h = d.high_rate
        h_base = settings.high_rate_benchmark # 예: -30.0
        h_step = settings.high_rate_step      # 예: 7.0
        
        # 기준값(-30)보다 작으면(더 많이 떨어졌으면) 5점
        if h < h_base: score = 5
        # 기준 + 1간격(-23)보다 작으면 4점
        elif h < h_base + h_step: score = 4
        # 기준 + 2간격(-16)보다 작으면 3점
        elif h < h_base + (h_step * 2): score = 3
        # 기준 + 3간격(-9)보다 작으면 2점
        elif h < h_base + (h_step * 3): score = 2
        # 0보다 작으면(하락 상태) 1점
        elif h < 0: score = 1
        else: score = 0
        
        # -----------------------------------------------------
        # 2. 연중 최저가 대비 현재가 비율 (Low Price Penalty)
        # -----------------------------------------------------
        penalty = 0
        l = d.low_rate
        l_base = settings.low_rate_benchmark # 예: 30.0
        l_step = settings.low_rate_step      # 예: 5.0
        
        # 기준값(30)보다 크면(많이 올랐으면) 3점 감점
        if l > l_base: penalty = 3
        # 기준 - 1간격(25)보다 크면 2점 감점
        elif l > l_base - l_step: penalty = 2
        # 기준 - 2간격(20)보다 크면 1점 감점
        elif l > l_base - (l_step * 2): penalty = 1
        else: penalty = 0
        
        # 최종 점수 (음수 방지 여부는 정책에 따름, 여기선 0점 하한선 적용)
        return max(0, score - penalty)

    def _cal_trend_score(self, d: SwingData) -> int:
        """
        이평선 분석 점수 (매매 설정에 따라 정배열/역배열 기준 적용)
        """
        # 1. 설정 가져오기
        settings = get_settings_manager().settings.evaluation.kr
        mode = settings.trend_alignment.upper() # "REGULAR" or "REVERSE"
        
        point = 0
        p = d.stck_clpr
        
        # 필수 데이터 검증
        if d.ma5 == 0 or d.ma20 == 0 or d.ma60 == 0: 
            return 0

        # ---------------------------------------------------------
        # CASE A: 정배열 전략 (상승 추세 추종)
        # -> 이미 상승세를 탄 종목을 선호
        # ---------------------------------------------------------
        if mode == "REGULAR":
            if p >= d.ma5: point += 1        # 현재가 > 5일선
            if p >= d.ma20: point += 1       # 현재가 > 20일선
            if d.ma5 >= d.ma20: point += 1   # 5일선 > 20일선 (골든크로스 구간)
            if d.ma20 >= d.ma60: point += 1  # 20일선 > 60일선
            if d.ma60 >= d.ma120: point += 1 # 60일선 > 120일선
            
            return point # 최대 5점

        # ---------------------------------------------------------
        # CASE B: 역배열 전략 (바닥권/눌림목 공략)
        # -> 중장기 하락 후 반등하는 종목을 선호
        # ---------------------------------------------------------
        else: # "REVERSE"
            # 1. [저평가 위치] 60일선 > 20일선 (2점)
            # 장기 이평선이 위에 있다는 것은 중장기 조정/하락 국면임을 의미
            if d.ma60 > d.ma20:
                point += 2
                
            # 2. [바닥 다지기] 현재가 >= 20일선 (2점)
            # 하락 추세 속에서 생명선(20일선)을 회복했다면 강력한 매수 신호
            if p >= d.ma20:
                point += 2
                
            # 3. [단기 탄력] 현재가 >= 5일선 (1점)
            # 단기 수급 유입 확인
            if p >= d.ma5:
                point += 1
                
            return min(5, point) # 최대 5점 제한

    def _cal_kpi_score(self, d: SwingData) -> int:
        """
        RSI 및 OBV 보조지표 점수
        - RSI 14일 기준: 과매도(<30) +2점, 과매수(>70) -2점
        - OBV 14일 추세: 상승 +2점, 하락 -2점
        - 합계가 4점(강력 매수)이면 보너스 +1점
        """
        rsi_score = 0
        if d.rsi_14 > 70: rsi_score = -2
        elif d.rsi_14 < 30: rsi_score = 2
        
        obv_score = 0
        if d.obv_trend > 0: obv_score = 2
        elif d.obv_trend < 0: obv_score = -2
        
        total = rsi_score + obv_score
        
        if total == 4:
            total += 1
            
        return min(5, max(0, total))

    def _cal_buy_score(self, d: SwingData) -> int:
        """
        수급 점수 계산
        - 거래량 대비 수급 유입 강도(volumeRate)
        - 외국인 지분율(volumeRate2)
        """
        # 비율 계산 헬퍼 함수 (Java calculateRate 대응)
        def calculate_rate(a, b):
            if b == 0: return 0.0
            return (a / b) * 100

        # 1. Volume Rate: 거래량 대비 (외국인 순매수 OR 프로그램 순매수) 비율 중 큰 값
        rate_frgn = calculate_rate(d.frgn_ntby_qty, d.acml_vol)
        rate_pgtr = calculate_rate(d.pgtr_ntby_qty, d.acml_vol)
        
        volume_rate = max(rate_frgn, rate_pgtr)

        # 2. Volume Rate 2: 외국인 보유 비중 (외국인보유수량 / 상장주식수)
        volume_rate2 = calculate_rate(d.frgn_hldn_qty, d.lstn_stcn)

        # 3. 점수 판별
        # - volume_rate: 당일 수급 강도
        # - volume_rate2: 메이저(외국인) 지분 안정성
        if volume_rate > 10 and volume_rate2 > 10: return 5
        if volume_rate > 10 or volume_rate2 > 10: return 4
        if volume_rate > 5 and volume_rate2 > 5: return 3
        
        if volume_rate > 5 or volume_rate2 > 5: return 2
        
        return 1

    def _cal_avls_score(self, d: SwingData) -> int:
        """
        시가총액 점수 계산 (기준값 + 간격 방식)
        - 시가총액이 클수록 안정성 점수 부여
        - 단위: 억원 (HTS 기준)
        """
        mkt_cap = d.hts_avls or 0
        
        settings = get_settings_manager().settings.evaluation.kr
        base = settings.avls_benchmark  # 예: 100 (억원)
        step = settings.avls_step       # 예: 1200 (억원)
        
        # 1. 기준값 미만 (초소형주) -> 1점
        if mkt_cap < base: return 1
        
        # 2. 기준 + 1간격 미만 -> 2점
        if mkt_cap < base + step: return 2
        
        # 3. 기준 + 2간격 미만 -> 3점
        if mkt_cap < base + (step * 2): return 3
        
        # 4. 기준 + 3간격 미만 -> 4점
        if mkt_cap < base + (step * 3): return 4
        
        # 5. 그 이상 (중대형주) -> 5점
        return 5

    def _cal_per_score(self, d: SwingData) -> int:
        """
        PER 점수 계산 (기준값 + 간격 방식)
        - PER가 낮을수록 고득점
        """
        if d.per <= 0: return 0  # 적자 기업 0점 처리
        
        settings = get_settings_manager().settings.evaluation.kr
        base = settings.per_benchmark
        step = settings.per_step
        
        # 기준값(5.0)보다 작으면 5점
        if d.per < base: return 5
        # 기준 + 1간격(10.0)보다 작으면 4점
        if d.per < base + step: return 4
        # 기준 + 2간격(15.0)보다 작으면 3점
        if d.per < base + (step * 2): return 3
        # 기준 + 3간격(20.0)보다 작으면 2점
        if d.per < base + (step * 3): return 2
        
        return 1

    def _cal_pbr_score(self, d: SwingData) -> int:
        """
        PBR 점수 계산 (기준값 + 간격 방식)
        - PBR이 낮을수록 고득점
        """
        if d.pbr <= 0: return 0 # 데이터 오류 시 0점
        
        settings = get_settings_manager().settings.evaluation.kr
        base = settings.pbr_benchmark
        step = settings.pbr_step
        
        # 기준값(1.0)보다 작으면 5점
        if d.pbr < base: return 5
        # 기준 + 1간격(2.0)보다 작으면 4점
        if d.pbr < base + step: return 4
        # 기준 + 2간격(3.0)보다 작으면 3점
        if d.pbr < base + (step * 2): return 3
        # 기준 + 3간격(4.0)보다 작으면 2점
        if d.pbr < base + (step * 3): return 2
        
        return 1

    def calculate_srim(self, equity: int, roe: float, required_yield: float = 8.0) -> int:
        """SRIM 적정 주가 계산"""
        if not equity or equity <= 0: return 0
        if not roe: roe = 0.0
        
        k = required_yield / 100.0
        roe_val = roe / 100.0
        
        # 초과이익 (Excess Earnings)
        excess_earnings = equity * (roe_val - k)
        
        # 잔여가치 (w = 0.9, 0.8) 가중 평균
        value_w9 = equity + (excess_earnings * (0.9 / (1 + k - 0.9)))
        value_w8 = equity + (excess_earnings * (0.8 / (1 + k - 0.8)))
        
        target_cap = (value_w9 + value_w8) / 2
        return int(target_cap)

    def check_safety_nets(
        self, 
        item_cd, 
        current_price, 
        shares, 
        financial_data, 
        equity_data, 
        roe_avg_3yr,
        required_yield: float = 8.0,
        fnguide_data: Optional[Dict] = None
    ) -> Dict:
        """안전망 체크 (FnGuide + DB)"""
        results = {
            'srim_pass': False, 'cashflow_pass': False, 'activity_pass': False,
            'dividend_pass': False, 'roe_pass': False, 'srim_price': 0
        }
        
        # 데이터 우선순위 (FnGuide > DB)
        fn_equity = fnguide_data.get('equity') if fnguide_data else 0
        fn_roe = fnguide_data.get('roe') if fnguide_data else 0
        db_equity = financial_data.total_equity if financial_data else 0
        db_roe = financial_data.roe_val if financial_data else 0
        
        equity = fn_equity if fn_equity > 0 else db_equity
        roe = fn_roe if fn_roe != 0 else db_roe
        
        # 1. SRIM
        if equity > 0:
            srim_mkt_cap = self.calculate_srim(equity, roe, required_yield)
            srim_price = int(srim_mkt_cap / shares) if shares > 0 else 0
            results['srim_price'] = srim_price
            if srim_price > 0 and current_price < srim_price:
                results['srim_pass'] = True
            
        # 2. 현금흐름
        if fnguide_data and fnguide_data.get('fcf_pass'):
            results['cashflow_pass'] = True
        elif financial_data:
            cf = (financial_data.cf_oa or 0) - abs(financial_data.cf_ia or 0)
            if cf > 0: results['cashflow_pass'] = True
            
        # 3. 활동성
        if fnguide_data and fnguide_data.get('activity_pass'):
            results['activity_pass'] = True
        elif financial_data:
            rev = financial_data.revenue or 0
            assets = financial_data.total_assets or 1
            if (rev / assets) > 0.3: results['activity_pass'] = True
            
        # 4. 배당
        fn_div = fnguide_data.get('dividend_yield', 0) if fnguide_data else 0
        db_div = equity_data.dividend_yield if equity_data else 0
        if fn_div > 0 or db_div > 0: results['dividend_pass'] = True
            
        # 5. ROE 3년
        if roe_avg_3yr >= 8.0: results['roe_pass'] = True
            
        return results


# =========================================================
# 3. 통합 실행 서비스 (EvaluationService)
# =========================================================

class EvaluationService:
    """전 종목 평가 실행기 (UI & Scheduler 공용)"""
    
    def __init__(self):
        self.evaluator = Evaluator()
        self.bond_fetcher = BondYieldFetcher()
        self.fnguide = FnGuideFetcher()
        self.settings_trading = get_settings_manager().settings.trading.kr
        self.settings_evaluation = get_settings_manager().settings.evaluation.kr
    
    # -------------------------------------------------------------
    # RSI 및 OBV 계산 헬퍼 함수
    # -------------------------------------------------------------
    def _calculate_rsi(self, prices: List[ItemPrice], period: int = 14) -> float:
        """RSI 계산 (Java: Wilder's Smoothing 방식 반영)"""
        if len(prices) < period:
            return 50.0

        # 가격순서: DB는 최신순(DESC) -> 계산은 과거순(ASC) 필요
        # prices[0]이 최신, prices[-1]이 가장 과거
        # 따라서 역순으로 정렬하여 계산
        sorted_prices = prices[::-1] # ASC 정렬
        
        gains = []
        losses = []
        
        for i in range(1, len(sorted_prices)):
            change = sorted_prices[i].stck_clpr - sorted_prices[i-1].stck_clpr
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(change))
                
        # First Average
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        # Smoothing (Wilder's)
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            
        if avg_loss == 0:
            return 100.0
            
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _calculate_obv_trend(self, prices: List[ItemPrice], period: int = 14) -> int:
        """OBV 추세 계산 (1:상승, -1:하락, 0:보합)"""
        if len(prices) < period + 1:
            return 0
            
        sorted_prices = prices[::-1] # ASC 정렬
        obv_values = [0.0]
        current_obv = 0.0
        
        for i in range(1, len(sorted_prices)):
            curr = sorted_prices[i].stck_clpr
            prev = sorted_prices[i-1].stck_clpr
            vol = sorted_prices[i].acml_vol
            
            if curr > prev:
                current_obv += vol
            elif curr < prev:
                current_obv -= vol
            
            obv_values.append(current_obv)
            
        if len(obv_values) < period:
            return 0
            
        start_obv = obv_values[len(obv_values) - period] # 14일 전
        end_obv = obv_values[-1]                         # 현재
        
        if end_obv > start_obv: return 1
        elif end_obv < start_obv: return -1
        else: return 0

    def run_evaluation(
        self, 
        base_date: date, 
        target_data_date: Optional[str] = None, 
        auto_detect_data_date: bool = False,  # [추가된 파라미터]
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        log_callback: Optional[Callable[[str], None]] = None
    ) -> Dict:
        """
        평가 프로세스 통합 실행
        :param base_date: 평가 결과가 저장될 날짜 (보통 오늘)
        :param target_data_date: 평가에 사용할 데이터의 날짜 (YYYYMMDD)
        :param auto_detect_data_date: True일 경우 DB에서 가장 최신 데이터 날짜를 자동으로 찾음
        """
        def log(msg, level="INFO"):
            if log_callback: log_callback(msg)
            else: 
                if level == "ERROR": logger.error(msg)
                else: logger.info(msg)
        
        eval_date_str = base_date.strftime('%Y%m%d')
        data_date_str = target_data_date

        result = {'total_evaluated': 0, 'buy_candidates': 0, 'errors': []}

        # [단계 1] 평가 대상 데이터 날짜 결정
        with get_session() as session:
            # 자동 감지 모드이거나 날짜가 지정되지 않은 경우
            if auto_detect_data_date or not data_date_str:
                # [수정 1] 최신 날짜 감지 시에도 'KR' 데이터 중에서만 찾도록 필터 추가
                latest_date = session.query(func.max(ItemMst.base_date))\
                    .filter(ItemMst.market_type == 'KR').scalar()
                
                if not latest_date:
                    log("평가할 기초 데이터(ItemMst - KR)가 DB에 없습니다.", "ERROR")
                    return result
                
                data_date_str = latest_date
                log(f"최신 데이터 자동 감지 (KR): {data_date_str}")
            
            # [단계 2] 평가 대상 종목 조회
            # [수정 2] 해당 날짜의 종목 중 'KR' 시장 종목만 가져오도록 필터 추가
            items = session.query(ItemMst).filter(
                ItemMst.base_date == data_date_str,
                ItemMst.market_type == 'KR'
            ).all()
            total_count = len(items)
            
            if total_count == 0:
                log(f"해당 날짜({data_date_str})의 데이터가 없습니다.", "ERROR")
                return result

            # BBB- 5년 금리 조회 (SRIM용)
            required_yield = self.bond_fetcher.get_bbb_5y_yield()
            log(f"평가 시작 (총 {total_count}개, 금리 {required_yield}%)")

            # [단계 3] 종목별 순회 평가
            processed_count = 0
            
            for idx, item in enumerate(items):
                processed_count += 1
                if progress_callback:
                    progress_callback(processed_count, total_count, f"{item.item_cd} 평가 중...")
                
                try:
                    # 3-1. FnGuide 데이터 수집 (지연 처리)
                    time.sleep(0.05) 
                    fn_data = self.fnguide.get_financial_safety_data(item.item_cd)
                    
                    # 3-2. DB 데이터 조회 (가격, 재무, 지분)
                    prices = session.query(ItemPrice).filter(
                        ItemPrice.item_cd == item.item_cd,
                        ItemPrice.trade_date <= data_date_str,
                        ItemPrice.market_type == 'KR'
                    ).order_by(ItemPrice.trade_date.desc()).limit(30).all()
                    
                    if not prices: continue # 가격 없으면 평가 불가
                    price = prices[0] # 최신 가격

                    # RSI & OBV 계산
                    rsi_val = self._calculate_rsi(prices, 14)
                    obv_trend = self._calculate_obv_trend(prices, 14)
                    
                    fs = session.query(FinancialSheet).filter(
                        FinancialSheet.item_cd == item.item_cd,
                        FinancialSheet.base_date <= data_date_str
                    ).order_by(FinancialSheet.base_date.desc()).first()
                    
                    eq = session.query(ItemEquity).filter(
                        ItemEquity.item_cd == item.item_cd,
                        ItemEquity.market_type == 'KR'
                    ).first()
                    
                    # 3-3. ROE 3년 평균 계산 (FnGuide -> DB)
                    roe_avg = 0.0
                    if fn_data and fn_data.get('roe_avg', 0) != 0:
                        roe_avg = fn_data['roe_avg']
                    elif fs:
                        # 과거 데이터 조회
                        roe_history = session.query(FinancialSheet.roe_val).filter(
                            FinancialSheet.item_cd == item.item_cd,
                            FinancialSheet.base_date <= data_date_str
                        ).order_by(FinancialSheet.base_date.desc()).limit(3).all()
                        
                        valid_roes = [r[0] for r in roe_history if r[0] is not None]
                        if valid_roes:
                            roe_avg = sum(valid_roes) / len(valid_roes)
                        else:
                            roe_avg = fs.roe_val or 0.0
                    
                    # 3-4. 안전망 체크
                    shares_val = eq.lstn_stcn if eq else 0
                    if shares_val is None: shares_val = 0  # 이중 체크

                    safety = self.evaluator.check_safety_nets(
                        item_cd=item.item_cd,
                        current_price=price.stck_clpr,
                        shares=shares_val,
                        financial_data=fs,
                        equity_data=eq,
                        roe_avg_3yr=roe_avg,
                        required_yield=required_yield,
                        fnguide_data=fn_data
                    )
                    
                    # 3-5. 점수 계산
                    swing_data = self._convert_to_swing_data(item, price, fs, eq, rsi_val, obv_trend)
                    score = self.evaluator.evaluate(swing_data)
                    
                    # 안전망 결과 매핑
                    score.srim_price = safety['srim_price']
                    score.srim_pass = safety['srim_pass']
                    score.cashflow_pass = safety['cashflow_pass']
                    score.activity_pass = safety['activity_pass']
                    score.dividend_pass = safety['dividend_pass']
                    score.roe_pass = safety['roe_pass']
                    
                    # 3-6. 최종 매수 후보 판별 (설정값 기반)
                    is_safe = True
                    s = self.settings_trading
                    if s.use_srim_filter and not safety['srim_pass']: is_safe = False
                    if s.use_cashflow_filter and not safety['cashflow_pass']: is_safe = False
                    if s.use_activity_filter and not safety['activity_pass']: is_safe = False
                    if s.use_dividend_filter and not safety['dividend_pass']: is_safe = False
                    if s.use_roe_filter and not safety['roe_pass']: is_safe = False
                    
                    # 총점 과락 체크
                    if score.total_score < self.settings_evaluation.min_total_score: is_safe = False
                    
                    score.is_buy_candidate = is_safe
                    
                    # 3-7. DB 저장
                    self._save_result(session, eval_date_str, score, price.stck_clpr)
                    
                    result['total_evaluated'] += 1
                    if is_safe: result['buy_candidates'] += 1
                    
                    # 배치 커밋 (50건마다)
                    if processed_count % 50 == 0:
                        session.commit()
                        
                except Exception as e:
                    session.rollback()
                    log(f"evaluator.run_evaluation 개별 오류 ({item.item_cd}): {e}", "ERROR") 
                    result['errors'].append(str(e))
            
            # 최종 커밋
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                log(f"최종 커밋 실패: {e}", "ERROR")
            
            log(f"평가 완료: {result['total_evaluated']}건, 매수후보 {result['buy_candidates']}건")
            return result
    
    def analyze_stock(self, item_cd: str, base_date: date = None) -> Dict:
        """단일 종목 상세 분석 (UI 상세화면용)"""
        if base_date is None:
            base_date = date.today()
        
        with get_session() as session:
            # ItemMst 조회 시 market_type='KR' 추가
            mst = session.query(ItemMst).filter(
                ItemMst.item_cd == item_cd,
                ItemMst.market_type == 'KR'
            ).order_by(ItemMst.base_date.desc()).first()
            if not mst: return {}
            
            # ItemPrice 조회 시 market_type='KR' 추가
            price = session.query(ItemPrice).filter(
                ItemPrice.item_cd == item_cd,
                ItemPrice.market_type == 'KR'
            ).order_by(ItemPrice.trade_date.desc()).first()
            
            # EvaluationResult 조회 시 market_type='KR' 추가
            eval_res = session.query(EvaluationResult).filter(
                EvaluationResult.item_cd == item_cd,
                EvaluationResult.market_type == 'KR'
            ).order_by(EvaluationResult.base_date.desc()).first()
            
            result = {
                'code': item_cd,
                'name': mst.itms_nm,
                'price': price.stck_clpr if price else 0,
                'score': eval_res.total_score if eval_res else 0,
                'is_candidate': eval_res.is_buy_candidate if eval_res else False,
                'per': eval_res.per_score if eval_res else 0, # 점수로 매핑됨
                'pbr': eval_res.pbr_score if eval_res else 0,
                'srim_price': eval_res.srim_price if eval_res else 0
            }
            return result

    def _convert_to_swing_data(self, item, price, fs, eq, rsi_val=50.0, obv_trend=0) -> SwingData:
        """DB 객체를 SwingData로 변환 (None 안전 처리 및 0값 보존)"""
        d = SwingData(item_cd=item.item_cd, item_nm=item.itms_nm)
        
        # [헬퍼 함수] None일 경우에만 기본값 사용 (0은 값으로 인정)
        def get_val(val, default):
            return val if val is not None else default

        # RSI, OBV 설정
        d.rsi_14 = rsi_val
        d.obv_trend = obv_trend

        if price:
            d.stck_clpr = get_val(price.stck_clpr, 0)
            d.acml_vol = get_val(price.acml_vol, 0)
            d.ma5 = get_val(price.ma5, 0)
            d.ma20 = get_val(price.ma20, 0)
            d.ma60 = get_val(price.ma60, 0)
            d.ma120 = get_val(price.ma120, 0)
            
        if fs:
            d.grs = get_val(fs.grs, 0.0)
            d.bsop_prfi_inrt = get_val(fs.bsop_prfi_inrt, 0.0)
            d.roe_val = get_val(fs.roe_val, 0.0)
            
            # 부채비율은 None일 때만 999로 설정 (0%는 우량한 것이므로 유지)
            d.lblt_rate = get_val(fs.lblt_rate, 999.0) 
            d.thtr_ntin = get_val(fs.thtr_ntin, 0)
            d.rsrv_rate = get_val(fs.rsrv_rate, 0.0)
            
        if eq:
            d.lstn_stcn = get_val(eq.lstn_stcn, 1)        # 상장주식수
            d.frgn_hldn_qty = get_val(eq.frgn_hldn_qty, 0) # 외국인보유수량
            d.frgn_ntby_qty = get_val(eq.frgn_ntby_qty, 0)
            d.pgtr_ntby_qty = get_val(eq.pgtr_ntby_qty, 0)
            d.hts_avls = get_val(eq.hts_avls, 0)
            d.per = get_val(eq.per, 0.0)
            d.pbr = get_val(eq.pbr, 0.0)
            d.high_rate = get_val(eq.dryy_hgpr_vrss_prpr_rate, -99.0)
            d.low_rate = get_val(eq.dryy_lwpr_vrss_prpr_rate, -99.0)
            d.frgn_rate = get_val(eq.hts_frgn_ehrt, 0.0)
            
        return d

    def _save_result(self, session, base_date, score: EvaluationScore, current_price: int):
        """DB 저장/업데이트 로직"""
        existing = session.query(EvaluationResult).filter(
            EvaluationResult.item_cd == score.item_cd,
            EvaluationResult.base_date == base_date,
            EvaluationResult.market_type == 'KR'
        ).first()
        
        if existing:
            existing.item_nm = score.item_nm
            existing.total_score = score.total_score
            existing.sheet_score = score.sheet_score
            existing.trend_score = score.trend_score
            existing.price_score = score.price_score
            existing.kpi_score = score.kpi_score
            existing.buy_score = score.buy_score
            existing.avls_score = score.avls_score
            existing.per_score = score.per_score
            existing.pbr_score = score.pbr_score
            
            existing.is_buy_candidate = score.is_buy_candidate
            existing.current_price = current_price
            
            existing.srim_price = score.srim_price
            existing.srim_pass = score.srim_pass
            existing.cashflow_pass = score.cashflow_pass
            existing.activity_pass = score.activity_pass
            existing.dividend_pass = score.dividend_pass
            existing.roe_pass = score.roe_pass
            
            existing.updated_at = datetime.now()
        else:
            new_res = EvaluationResult(
                item_cd=score.item_cd,
                base_date=base_date,
                market_type='KR',
                item_nm=score.item_nm,
                total_score=score.total_score,
                sheet_score=score.sheet_score,
                trend_score=score.trend_score,
                price_score=score.price_score,
                kpi_score=score.kpi_score,
                buy_score=score.buy_score,
                avls_score=score.avls_score,
                per_score=score.per_score,
                pbr_score=score.pbr_score,
                is_buy_candidate=score.is_buy_candidate,
                current_price=current_price,
                
                srim_price=score.srim_price,
                srim_pass=score.srim_pass,
                cashflow_pass=score.cashflow_pass,
                activity_pass=score.activity_pass,
                dividend_pass=score.dividend_pass,
                roe_pass=score.roe_pass,
                
                created_at=datetime.now()
            )
            session.add(new_res)