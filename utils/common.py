# utils/common.py

import math
import numpy as np
import streamlit as st

def safe_cast(val):
    """
    값을 DB에 저장 가능한 Python 기본 타입(float, int, None)으로 변환합니다.
    
    기능:
    1. Numpy 데이터 타입(np.int64, np.float64 등)을 Python Native 타입으로 변환
    2. NaN(Not a Number) 및 Inf(무한대) 값을 None으로 변환하여 DB 오류(DPY-4004) 방지
    3. 변환 불가능한 값은 None 반환
    """
    if val is None:
        return None
    
    # 1. Numpy 타입인 경우 Python Native 타입으로 변환 (.item() 사용)
    if hasattr(val, 'item'): 
        val = val.item()
        
    # 2. 실수형(float) 변환 시도 및 NaN/Inf 체크
    try:
        # 문자열이나 다른 타입이 들어올 수 있으므로 float 변환 시도
        f_val = float(val)
        
        # NaN(결측치)이거나 Inf(무한대)이면 None 반환
        if math.isnan(f_val) or math.isinf(f_val):
            return None
            
        return f_val
        
    except (ValueError, TypeError):
        # 변환할 수 없는 값(예: 문자열 "abc" 등)은 None 처리
        return None
    
def custom_metric(label, value, value_color="inherit"):
    st.markdown(f"""
    <div style="display: flex; flex-direction: column; margin-bottom: 10px;">
        <span style="font-size: 0.8rem; color: gray;">{label}</span>
        <span style="font-size: 1.2rem; font-weight: bold; color: {value_color};">{value}</span>
    </div>
    """, unsafe_allow_html=True)    