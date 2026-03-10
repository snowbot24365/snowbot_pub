"""
SnowBot - 메인 앱
"""
import streamlit as st
from ui import main_impl

st.markdown("""
    <style>
        /* Deploy 버튼 숨김 */
        .stAppDeployButton {
            display: none;
            visibility: hidden;
        }
    </style>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main_impl.run_snowbot()