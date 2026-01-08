"""
Core utilities for the Retail Analytics Dashboard.
Provides caching, configuration, and shared utilities.
"""

from .cache import (
    compute_data_hash,
    CacheManager,
    clear_all_caches
)
from .config import (
    STORE_MAPPING,
    STORE_DISPLAY_NAMES,
    SAMPLE_PREFIXES,
    AppConfig
)
from .utils import make_json_serializable, safe_json_dumps

__all__ = [
    # Cache
    'compute_data_hash',
    'CacheManager',
    'clear_all_caches',
    # Config
    'STORE_MAPPING',
    'STORE_DISPLAY_NAMES',
    'SAMPLE_PREFIXES',
    'AppConfig',
    # Utils
    'make_json_serializable',
    'safe_json_dumps',
]
