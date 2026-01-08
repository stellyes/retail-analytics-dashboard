"""
Caching utilities for the Retail Analytics Dashboard.
Provides hash-based cache invalidation and localStorage persistence.
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Optional


def compute_data_hash(data: Any) -> str:
    """
    Compute a hash of the data for cache invalidation.

    Args:
        data: Any JSON-serializable data structure

    Returns:
        16-character MD5 hash string
    """
    json_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(json_str.encode()).hexdigest()[:16]


class CacheManager:
    """
    Manages application caching with hash-based invalidation.
    """

    def __init__(self, cache_key_prefix: str = "retail"):
        self.cache_key_prefix = cache_key_prefix
        self._hash_cache: Dict[str, str] = {}

    def get_cache_key(self, name: str) -> str:
        """Generate a prefixed cache key."""
        return f"{self.cache_key_prefix}_{name}"

    def compute_hash(self, data: Any) -> str:
        """Compute hash for cache invalidation."""
        return compute_data_hash(data)

    def needs_refresh(self, name: str, current_hash: str) -> bool:
        """
        Check if cached data needs refresh based on hash comparison.

        Args:
            name: Cache entry name
            current_hash: Hash of current data

        Returns:
            True if cache needs refresh, False otherwise
        """
        last_hash = self._hash_cache.get(name)
        return current_hash != last_hash

    def update_hash(self, name: str, current_hash: str) -> None:
        """Update the stored hash for a cache entry."""
        self._hash_cache[name] = current_hash

    def get_localstorage_script(self, key: str, data: dict) -> str:
        """
        Generate JavaScript to save data to browser localStorage.

        Args:
            key: Storage key
            data: Data to store

        Returns:
            HTML/JS snippet for Streamlit
        """
        json_data = json.dumps(data, default=str)
        data_hash = self.compute_hash(data)
        timestamp = datetime.now().isoformat()

        return f"""
        <script>
            try {{
                localStorage.setItem('{self.cache_key_prefix}_cache_{key}', '{json_data}');
                localStorage.setItem('{self.cache_key_prefix}_cache_{key}_hash', '{data_hash}');
                localStorage.setItem('{self.cache_key_prefix}_cache_{key}_timestamp', '{timestamp}');
            }} catch(e) {{
                console.log('localStorage save failed:', e);
            }}
        </script>
        """


def clear_all_caches() -> None:
    """
    Clear all Streamlit data caches.
    Call this when user requests a manual refresh.
    """
    try:
        import streamlit as st
        st.cache_data.clear()
    except Exception:
        pass


# Streamlit cache decorators for common patterns
def cached_data_loader(ttl_seconds: int = 86400):
    """
    Decorator factory for cached data loading functions.

    Usage:
        @cached_data_loader(ttl_seconds=3600)
        def load_my_data(hash_key: str):
            return expensive_operation()
    """
    try:
        import streamlit as st

        def decorator(func):
            return st.cache_data(ttl=ttl_seconds, show_spinner=False)(func)

        return decorator
    except ImportError:
        # If Streamlit not available, return identity decorator
        def decorator(func):
            return func
        return decorator
