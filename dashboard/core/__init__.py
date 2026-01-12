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

# Optimized data loading
from .data_loader import (
    OptimizedDataLoader,
    HashTracker,
    get_s3_client,
    get_s3_resource,
    get_data_loader,
    load_and_cache_dataframe,
    quick_load_json,
    cached_s3_load,
)

# Unified cache management
from .cache_manager import (
    CacheManager as UnifiedCacheManager,
    SessionCache,
    FileCache,
    CacheLevel,
    CacheConfig,
    CacheStats,
    get_cache_manager,
    clear_all_caches as clear_unified_caches,
    cached,
    render_cache_stats,
)

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
    # Optimized data loading
    'OptimizedDataLoader',
    'HashTracker',
    'get_s3_client',
    'get_s3_resource',
    'get_data_loader',
    'load_and_cache_dataframe',
    'quick_load_json',
    'cached_s3_load',
    # Unified cache management
    'UnifiedCacheManager',
    'SessionCache',
    'FileCache',
    'CacheLevel',
    'CacheConfig',
    'CacheStats',
    'get_cache_manager',
    'clear_unified_caches',
    'cached',
    'render_cache_stats',
]
