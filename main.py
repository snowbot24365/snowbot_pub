"""
SnowBot - 메인 앱
"""
import sys
import os
import streamlit as st

# ==========================================
# [핵심] 프로그램 버전 기록
# ==========================================
APP_VERSION = "1.1.3"  # 이곳에서 버전을 관리하세요.
# 환경 변수에 등록하여 다른 파이썬 파일들(main.py 등)에서 참조할 수 있게 합니다.
os.environ["SNOWBOT_VERSION"] = APP_VERSION
# ==========================================

st.markdown("""
    <style>
        /* Deploy 버튼만 숨김 (햄버거 메뉴, Running 아이콘은 유지) */
        .stAppDeployButton {
            display: none;
            visibility: hidden;
        }
    </style>
""", unsafe_allow_html=True)

# ui 폴더(또는 컴파일된 pyd가 있는 위치)를 경로에 추가
sys.path.append(os.path.join(os.path.dirname(__file__), 'ui'))

try:
    # 1. 컴파일된 바이너리 모듈 임포트
    # ui/main_impl.pyd 파일이 있어야 함
    from ui import main_impl
except ImportError as e:
    # 개발 환경(소스코드)에서는 .py를 임포트
    try:
        from ui import main_impl
    except ImportError:
        import streamlit as st
        st.error(f"실행 모듈을 찾을 수 없습니다: {e}")
        st.stop()

if __name__ == "__main__":
    # 2. 실제 로직 실행
    main_impl.run_snowbot()