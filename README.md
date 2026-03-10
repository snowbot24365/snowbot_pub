# 📈 SnowBot (Python)

**SnowBot**은 Python과 Streamlit을 기반으로 구축된 **한국(KR) 및 미국(US) 주식 자동매매 및 데이터 분석 시스템**입니다.
한국투자증권(KIS) API를 활용하여 국내/해외 실전 및 모의 투자를 지원하며, `yfinance`와 `OpenDart`를 통해 다양한 시장 데이터를 수집합니다. 또한 Oracle Cloud Database(ATP) 또는 SQLite와 연동하여 안정적인 데이터 관리를 제공합니다.

## ✨ 주요 기능

### 1. 🌏 멀티 마켓 지원 (Multi-Market)

* **한국 주식 (KR)**: KOSPI, KOSDAQ 전 종목 지원 (OpenDart 재무 데이터 + KIS 시세)
* **미국 주식 (US)**: NASDAQ, NYSE, AMEX 등 주요 종목 지원 (`yfinance` 데이터 + KIS 해외주식 거래)

### 2. 📥 데이터 수집 (Data Collection)

* **KR Data**: OpenDart(재무제표, 재무비율), KRX 정보, KIS 실시간 시세
* **US Data**: `yfinance`를 활용한 주가 데이터 및 기술적 지표 수집
* **자동 관리**: 오래된 데이터 자동 정리 및 스케줄링

### 3. 📊 종목 평가 (Evaluation)

재무적 건전성과 기술적 추세를 결합한 독자적 스코어링 시스템:

* **재무 점수**: 매출성장률, 영업이익률, ROE, 부채비율 등 펀더멘털 분석
* **기술 점수**: 이동평균선 배열, 이격도, 신고가/신저가 위치 분석
* **수급/심리**: (KR 한정) 기관/외국인 수급 추이 분석

### 4. ⚡ 트레이딩 (Trading)

세 가지 실행 모드를 시장별로 개별 설정 가능합니다:

1. **시뮬레이션 (Simulation)**: 과거 데이터를 기반으로 한 전략 백테스팅
2. **모의투자 (Mock)**: 한국투자증권 모의투자 시스템 연동
3. **실전투자 (Real)**: 한국투자증권 실제 계좌 연동 (자금 관리 기능 포함)

### 5. 🔒 보안 및 편의성

* **로그인 시스템**: `Streamlit-Authenticator` 기반의 보안 접속 (설정 가능, Linux 환경에서 강제 적용)
* **스케줄러**: 장 마감 후 수집, 장 중 매매 등을 백그라운드에서 자동 수행 (`APScheduler`)

---

## 🛠 기술 스택

* **Language**: Python 3.10+
* **Web Framework**: Streamlit
* **Authentication**: Streamlit Authenticator
* **Database**:
  * **Local**: SQLite (`stock_data.db`)
  * **Cloud**: Oracle Autonomous Database (ATP)
* **Data & API**:
  * **Korea**: OpenDartReader, FinanceDataReader, KIS API (Domestic)
  * **USA**: yfinance, KIS API (Overseas)
* **ORM**: SQLAlchemy
* **Scheduler**: APScheduler

---

## 📂 프로젝트 구조

```
snowbot_pub/
├── main.py                 # 앱 진입점
├── requirements.txt        # 의존성 패키지 목록
├── config/                 # 설정 관리
│   ├── settings.py         # 환경 설정 로더
│   └── database.py         # DB 모델 및 세션 관리
├── core/                   # 핵심 추상 클래스 (Trader, Fetcher)
├── data/                   # 데이터 수집 로직
│   ├── kr/                 # 한국 주식 데이터 수집
│   └── us/                 # 미국 주식 데이터 수집
├── impl/                   # 시장별 구현체
│   ├── kr/                 # 한국 시장용 Fetcher/Trader
│   └── us/                 # 미국 시장용 Fetcher/Trader
├── trading/                # 트레이딩 엔진 (Simulation, AutoTrader)
├── scheduler/              # 작업 스케줄 관리
├── ui/                     # Streamlit 페이지
│   ├── dashboard.py        # 통합 대시보드
│   ├── auto_trading_page.py
│   ├── manual_trading_page.py
│   ├── evaluation_page.py
│   ├── data_collection_page.py
│   ├── favorite_page.py
│   ├── settings_page.py
│   └── main_impl.py        # 앱 레이아웃 및 라우팅
└── utils/                  # 로거, 토큰 관리 등 유틸리티
```

---

## 🚀 설치 및 실행

### 1. 가상환경 생성 및 패키지 설치

```bash
# 가상환경 생성
python -m venv venv

# 가상환경 활성화
# Windows:
.\venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 패키지 설치
pip install -r requirements.txt
```

### 2. 실행

```bash
streamlit run main.py
```

브라우저에서 `http://localhost:8501` 으로 접속합니다.

---

## ⚙️ 설정 가이드

### 1. `config_data/settings.json`

최초 실행 시 `config_data/` 폴더에 설정 파일이 자동 생성됩니다. UI의 **설정 페이지**에서도 수정할 수 있습니다.

* **API Keys**:
  * **OpenDart**: 한국 기업 재무 데이터 조회용
  * **KIS API**: 한국투자증권 (국내/해외 계좌 별도 설정 필요)

* **Execution Mode**:
  * 한국(KR)과 미국(US) 각각 `Simulation`, `Mock`, `Real` 모드를 독립적으로 설정할 수 있습니다.

### 2. `config_data/auth.yaml` (선택 사항)

로그인 기능을 사용하려면 인증 설정 파일을 구성해야 합니다. Linux 환경에서는 강제 적용됩니다.

```yaml
enabled: true
credentials:
  usernames:
    admin:
      name: Admin
      password: <bcrypt-hashed-password>
cookie:
  name: snowbot_cookie
  key: <random-secret-key>
  expiry_days: 30
```

---

## ⚠️ 주의사항

1. **투자 책임**: 본 시스템은 알고리즘 학습 및 연구용으로 개발되었습니다. 실제 투자로 인한 손실의 책임은 전적으로 사용자에게 있습니다.
2. **해외 주식 시세**: 미국 주식 실시간 시세 이용 시 한국투자증권의 **실시간 시세 신청**이 필요할 수 있습니다.
3. **보안**: API Key, Oracle Wallet 등 민감 정보가 포함된 `config_data/`, `wallet/` 폴더는 절대 외부에 노출되지 않도록 주의하십시오. (`.gitignore`에 의해 git 추적 제외됨)

---

## License

MIT License
