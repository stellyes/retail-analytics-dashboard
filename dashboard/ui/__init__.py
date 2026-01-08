"""
UI components for the Retail Analytics Dashboard.
Provides visualization functions and Streamlit components.
"""

from .charts import (
    plot_sales_trend,
    plot_category_breakdown,
    plot_brand_performance,
    plot_store_comparison,
)
from .auth import check_password
from .loading import show_loading_overlay, hide_loading_overlay

__all__ = [
    # Charts
    'plot_sales_trend',
    'plot_category_breakdown',
    'plot_brand_performance',
    'plot_store_comparison',
    # Auth
    'check_password',
    # Loading
    'show_loading_overlay',
    'hide_loading_overlay',
]
