"""
Unified Cache Management Module for Retail Analytics Dashboard

This module provides a centralized caching system that:
- Manages multiple cache layers (session, file, S3)
- Provides TTL-based expiration
- Supports cache warming and invalidation
- Tracks cache statistics for monitoring
- Integrates seamlessly with Streamlit

Author: Generated for stellyes/retail-analytics-dashboard
"""

import streamlit as st
import hashlib
import json
import os
import pickle
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, TypeVar, Generic, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

T = TypeVar('T')


# =============================================================================
# Cache Configuration
# =============================================================================

class CacheLevel(Enum):
    """Cache storage levels with different characteristics."""
    SESSION = "session"      # Streamlit session state (fastest, per-session)
    FILE = "file"           # Local file system (persistent, single instance)
    S3 = "s3"               # S3 storage (persistent, shared across instances)


@dataclass
class CacheConfig:
    """Configuration for cache behavior."""
    default_ttl_seconds: int = 3600  # 1 hour
    max_entries: int = 1000
    enable_stats: bool = True
    cache_directory: str = "/tmp/streamlit_cache"
    s3_cache_prefix: str = "cache/"
    compression_enabled: bool = False


@dataclass
class CacheEntry(Generic[T]):
    """A single cache entry with metadata."""
    key: str
    value: T
    created_at: datetime
    expires_at: datetime
    level: CacheLevel
    hit_count: int = 0
    size_bytes: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        return datetime.now() > self.expires_at
    
    def time_to_live(self) -> timedelta:
        """Get remaining TTL."""
        return self.expires_at - datetime.now()


@dataclass
class CacheStats:
    """Statistics for cache performance monitoring."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_size_bytes: int = 0
    entry_count: int = 0
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            'hits': self.hits,
            'misses': self.misses,
            'evictions': self.evictions,
            'hit_rate': f"{self.hit_rate:.2%}",
            'total_size_mb': self.total_size_bytes / (1024 * 1024),
            'entry_count': self.entry_count
        }


# =============================================================================
# Session Cache (Streamlit Session State)
# =============================================================================

class SessionCache:
    """
    Cache layer using Streamlit session state.
    Fastest but limited to current session.
    """
    
    CACHE_KEY = "_cache_manager_session"
    STATS_KEY = "_cache_manager_stats"
    
    @classmethod
    def _get_store(cls) -> Dict[str, CacheEntry]:
        """Get or initialize the session cache store."""
        if cls.CACHE_KEY not in st.session_state:
            st.session_state[cls.CACHE_KEY] = {}
        return st.session_state[cls.CACHE_KEY]
    
    @classmethod
    def _get_stats(cls) -> CacheStats:
        """Get or initialize cache statistics."""
        if cls.STATS_KEY not in st.session_state:
            st.session_state[cls.STATS_KEY] = CacheStats()
        return st.session_state[cls.STATS_KEY]
    
    @classmethod
    def get(cls, key: str) -> Optional[Any]:
        """Get value from session cache."""
        store = cls._get_store()
        stats = cls._get_stats()
        
        if key in store:
            entry = store[key]
            if not entry.is_expired():
                entry.hit_count += 1
                stats.hits += 1
                return entry.value
            else:
                # Remove expired entry
                del store[key]
                stats.evictions += 1
        
        stats.misses += 1
        return None
    
    @classmethod
    def set(
        cls,
        key: str,
        value: Any,
        ttl_seconds: int = 3600,
        metadata: Optional[Dict] = None
    ):
        """Set value in session cache."""
        store = cls._get_store()
        stats = cls._get_stats()
        
        now = datetime.now()
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds),
            level=CacheLevel.SESSION,
            size_bytes=len(pickle.dumps(value)),
            metadata=metadata or {}
        )
        
        store[key] = entry
        stats.entry_count = len(store)
        stats.total_size_bytes = sum(e.size_bytes for e in store.values())
    
    @classmethod
    def delete(cls, key: str) -> bool:
        """Delete entry from session cache."""
        store = cls._get_store()
        if key in store:
            del store[key]
            return True
        return False
    
    @classmethod
    def clear(cls):
        """Clear all session cache entries."""
        st.session_state[cls.CACHE_KEY] = {}
    
    @classmethod
    def cleanup_expired(cls):
        """Remove all expired entries."""
        store = cls._get_store()
        stats = cls._get_stats()
        
        expired_keys = [k for k, v in store.items() if v.is_expired()]
        for key in expired_keys:
            del store[key]
            stats.evictions += 1
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")


# =============================================================================
# File Cache (Local File System)
# =============================================================================

class FileCache:
    """
    Cache layer using local file system.
    Persists across restarts but local to instance.
    """
    
    def __init__(self, cache_dir: str = "/tmp/streamlit_cache"):
        """Initialize file cache with directory."""
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
    
    def _get_path(self, key: str) -> Path:
        """Get file path for cache key."""
        # Use hash for safe filename
        safe_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{safe_key}.cache"
    
    def _get_meta_path(self, key: str) -> Path:
        """Get metadata file path."""
        safe_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{safe_key}.meta"
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from file cache."""
        path = self._get_path(key)
        meta_path = self._get_meta_path(key)
        
        if not path.exists() or not meta_path.exists():
            return None
        
        try:
            # Check metadata for expiration
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            
            expires_at = datetime.fromisoformat(meta['expires_at'])
            if datetime.now() > expires_at:
                # Expired - clean up
                path.unlink(missing_ok=True)
                meta_path.unlink(missing_ok=True)
                return None
            
            # Load cached value
            with open(path, 'rb') as f:
                return pickle.load(f)
                
        except Exception as e:
            logger.warning(f"Error reading file cache: {e}")
            return None
    
    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int = 3600
    ):
        """Set value in file cache."""
        path = self._get_path(key)
        meta_path = self._get_meta_path(key)
        
        try:
            with self._lock:
                # Write value
                with open(path, 'wb') as f:
                    pickle.dump(value, f)
                
                # Write metadata
                meta = {
                    'key': key,
                    'created_at': datetime.now().isoformat(),
                    'expires_at': (datetime.now() + timedelta(seconds=ttl_seconds)).isoformat()
                }
                with open(meta_path, 'w') as f:
                    json.dump(meta, f)
                    
        except Exception as e:
            logger.error(f"Error writing to file cache: {e}")
    
    def delete(self, key: str) -> bool:
        """Delete entry from file cache."""
        path = self._get_path(key)
        meta_path = self._get_meta_path(key)
        
        deleted = False
        if path.exists():
            path.unlink()
            deleted = True
        if meta_path.exists():
            meta_path.unlink()
        
        return deleted
    
    def clear(self):
        """Clear all file cache entries."""
        for path in self.cache_dir.glob("*.cache"):
            path.unlink()
        for path in self.cache_dir.glob("*.meta"):
            path.unlink()
    
    def get_size(self) -> int:
        """Get total cache size in bytes."""
        return sum(f.stat().st_size for f in self.cache_dir.glob("*.cache"))


# =============================================================================
# Unified Cache Manager
# =============================================================================

class CacheManager:
    """
    Unified cache manager that coordinates multiple cache layers.
    Provides a simple interface with automatic fallback between layers.
    """
    
    def __init__(self, config: Optional[CacheConfig] = None):
        """Initialize cache manager with configuration."""
        self.config = config or CacheConfig()
        self.file_cache = FileCache(self.config.cache_directory)
    
    # -------------------------------------------------------------------------
    # Core Operations
    # -------------------------------------------------------------------------
    
    def get(
        self,
        key: str,
        levels: Optional[List[CacheLevel]] = None
    ) -> Optional[Any]:
        """
        Get value from cache, checking levels in order.
        
        Args:
            key: Cache key
            levels: Cache levels to check (default: SESSION, FILE)
            
        Returns:
            Cached value or None
        """
        if levels is None:
            levels = [CacheLevel.SESSION, CacheLevel.FILE]
        
        for level in levels:
            value = self._get_from_level(key, level)
            if value is not None:
                # Promote to faster levels if found in slower level
                self._promote(key, value, level, levels)
                return value
        
        return None
    
    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        levels: Optional[List[CacheLevel]] = None,
        metadata: Optional[Dict] = None
    ):
        """
        Set value in cache at specified levels.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Time to live (default from config)
            levels: Cache levels to write to (default: SESSION)
            metadata: Additional metadata to store
        """
        if ttl_seconds is None:
            ttl_seconds = self.config.default_ttl_seconds
        
        if levels is None:
            levels = [CacheLevel.SESSION]
        
        for level in levels:
            self._set_to_level(key, value, ttl_seconds, level, metadata)
    
    def delete(self, key: str, levels: Optional[List[CacheLevel]] = None) -> bool:
        """Delete entry from specified cache levels."""
        if levels is None:
            levels = [CacheLevel.SESSION, CacheLevel.FILE]
        
        deleted = False
        for level in levels:
            if self._delete_from_level(key, level):
                deleted = True
        
        return deleted
    
    def clear(self, levels: Optional[List[CacheLevel]] = None):
        """Clear all entries from specified cache levels."""
        if levels is None:
            levels = [CacheLevel.SESSION, CacheLevel.FILE]
        
        for level in levels:
            if level == CacheLevel.SESSION:
                SessionCache.clear()
            elif level == CacheLevel.FILE:
                self.file_cache.clear()
    
    # -------------------------------------------------------------------------
    # Level-Specific Operations
    # -------------------------------------------------------------------------
    
    def _get_from_level(self, key: str, level: CacheLevel) -> Optional[Any]:
        """Get value from a specific cache level."""
        if level == CacheLevel.SESSION:
            return SessionCache.get(key)
        elif level == CacheLevel.FILE:
            return self.file_cache.get(key)
        return None
    
    def _set_to_level(
        self,
        key: str,
        value: Any,
        ttl_seconds: int,
        level: CacheLevel,
        metadata: Optional[Dict] = None
    ):
        """Set value to a specific cache level."""
        if level == CacheLevel.SESSION:
            SessionCache.set(key, value, ttl_seconds, metadata)
        elif level == CacheLevel.FILE:
            self.file_cache.set(key, value, ttl_seconds)
    
    def _delete_from_level(self, key: str, level: CacheLevel) -> bool:
        """Delete from a specific cache level."""
        if level == CacheLevel.SESSION:
            return SessionCache.delete(key)
        elif level == CacheLevel.FILE:
            return self.file_cache.delete(key)
        return False
    
    def _promote(
        self,
        key: str,
        value: Any,
        found_level: CacheLevel,
        all_levels: List[CacheLevel]
    ):
        """Promote value to faster cache levels."""
        found_index = all_levels.index(found_level)
        for level in all_levels[:found_index]:
            self._set_to_level(key, value, self.config.default_ttl_seconds, level)
    
    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------
    
    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], T],
        ttl_seconds: Optional[int] = None,
        levels: Optional[List[CacheLevel]] = None
    ) -> T:
        """
        Get from cache or compute and cache the result.
        
        Args:
            key: Cache key
            compute_fn: Function to compute value if not cached
            ttl_seconds: TTL for cached value
            levels: Cache levels to use
            
        Returns:
            Cached or computed value
        """
        # Try to get from cache
        cached = self.get(key, levels)
        if cached is not None:
            return cached
        
        # Compute value
        value = compute_fn()
        
        # Cache the result
        self.set(key, value, ttl_seconds, levels)
        
        return value
    
    def cache_dataframe(
        self,
        key: str,
        df_or_fn,
        ttl_seconds: int = 3600
    ):
        """
        Cache a DataFrame or DataFrame-returning function.
        
        Args:
            key: Cache key
            df_or_fn: DataFrame or callable returning DataFrame
            ttl_seconds: TTL
            
        Returns:
            Cached or computed DataFrame
        """
        import pandas as pd
        
        cached = self.get(key)
        if cached is not None and isinstance(cached, pd.DataFrame):
            return cached
        
        if callable(df_or_fn):
            df = df_or_fn()
        else:
            df = df_or_fn
        
        self.set(key, df, ttl_seconds, [CacheLevel.SESSION])
        return df
    
    def cache_api_response(
        self,
        prompt_hash: str,
        response_or_fn,
        ttl_hours: int = 24
    ) -> str:
        """
        Cache an API response (e.g., Claude API).
        
        Args:
            prompt_hash: Hash of the prompt for cache key
            response_or_fn: Response string or callable
            ttl_hours: TTL in hours
            
        Returns:
            Cached or fresh response
        """
        key = f"api_response_{prompt_hash}"
        ttl_seconds = ttl_hours * 3600
        
        cached = self.get(key)
        if cached is not None:
            logger.info(f"Using cached API response for {prompt_hash[:8]}...")
            return cached
        
        if callable(response_or_fn):
            response = response_or_fn()
        else:
            response = response_or_fn
        
        self.set(key, response, ttl_seconds, [CacheLevel.SESSION, CacheLevel.FILE])
        return response
    
    # -------------------------------------------------------------------------
    # Statistics and Monitoring
    # -------------------------------------------------------------------------
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        session_stats = SessionCache._get_stats()
        
        return {
            'session': session_stats.to_dict(),
            'file': {
                'size_mb': self.file_cache.get_size() / (1024 * 1024)
            }
        }
    
    def cleanup(self):
        """Run cleanup on all cache levels."""
        SessionCache.cleanup_expired()
        logger.info("Cache cleanup completed")


# =============================================================================
# Decorator for Easy Caching
# =============================================================================

def cached(
    ttl_seconds: int = 3600,
    levels: Optional[List[CacheLevel]] = None,
    key_prefix: str = ""
):
    """
    Decorator for caching function results.
    
    Args:
        ttl_seconds: Cache TTL
        levels: Cache levels to use
        key_prefix: Prefix for cache keys
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            # Generate cache key from function and arguments
            key_parts = [key_prefix, func.__name__, str(args), str(sorted(kwargs.items()))]
            cache_key = hashlib.md5("_".join(key_parts).encode()).hexdigest()
            
            manager = get_cache_manager()
            return manager.get_or_compute(
                cache_key,
                lambda: func(*args, **kwargs),
                ttl_seconds,
                levels
            )
        return wrapper
    return decorator


# =============================================================================
# Global Cache Manager Instance
# =============================================================================

_cache_manager: Optional[CacheManager] = None


def get_cache_manager(config: Optional[CacheConfig] = None) -> CacheManager:
    """Get or create the global cache manager instance."""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager(config)
    return _cache_manager


def clear_all_caches():
    """Clear all cache levels."""
    manager = get_cache_manager()
    manager.clear()
    logger.info("All caches cleared")


# =============================================================================
# Streamlit Component for Cache Monitoring
# =============================================================================

def render_cache_stats():
    """
    Render cache statistics in Streamlit UI.
    Call this in a sidebar or debug section.
    """
    manager = get_cache_manager()
    stats = manager.get_stats()
    
    st.subheader("📊 Cache Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Session Hit Rate", stats['session']['hit_rate'])
        st.metric("Session Entries", stats['session']['entry_count'])
    
    with col2:
        st.metric("Session Hits", stats['session']['hits'])
        st.metric("Session Misses", stats['session']['misses'])
    
    st.metric("File Cache Size", f"{stats['file']['size_mb']:.2f} MB")
    
    if st.button("🧹 Clear All Caches"):
        clear_all_caches()
        st.success("Caches cleared!")
        st.rerun()


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    # Example usage patterns
    
    # 1. Basic caching
    cache = get_cache_manager()
    cache.set("my_key", {"data": "value"}, ttl_seconds=3600)
    value = cache.get("my_key")
    print(f"Cached value: {value}")
    
    # 2. Get or compute pattern
    def expensive_computation():
        print("Computing...")
        return {"result": 42}
    
    result = cache.get_or_compute("computation_key", expensive_computation)
    print(f"Result: {result}")
    
    # Second call uses cache
    result2 = cache.get_or_compute("computation_key", expensive_computation)
    print(f"Cached result: {result2}")
    
    # 3. Using decorator
    @cached(ttl_seconds=1800, key_prefix="analysis")
    def analyze_data(data_id: str):
        return f"Analysis for {data_id}"
    
    analysis = analyze_data("dataset_1")
    print(f"Analysis: {analysis}")
    
    # 4. Get stats
    stats = cache.get_stats()
    print(f"Cache stats: {json.dumps(stats, indent=2)}")
