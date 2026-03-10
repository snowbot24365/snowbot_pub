"""
UI 패키지
- 설정 (스케줄 관리 포함)
- 데이터수집
- 종목평가
- 수동매매
- 자동매매
- 대시보드
"""

from ui.settings_page import render_settings
from ui.data_collection_page import render_data_collection
from ui.evaluation_page import render_evaluation
from ui.manual_trading_page import render_manual_trading
from ui.auto_trading_page import render_auto_trading
from ui.dashboard import render_dashboard

__all__ = [
    'render_settings',
    'render_data_collection',
    'render_evaluation',
    'render_manual_trading',
    'render_auto_trading',
    'render_dashboard'
]
