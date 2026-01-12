"""
Optimized Data Loading Module for Retail Analytics Dashboard

This module provides efficient data loading mechanisms including:
- Streamlit caching integration
- Incremental/delta loading from S3
- Streaming for large files
- Hash-based change detection
- Memory-efficient chunked processing

Author: Generated for stellyes/retail-analytics-dashboard
"""

import streamlit as st
import boto3
import pandas as pd
import hashlib
import json
import io
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Generator, List, Tuple
from functools import wraps
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# S3 Client Management (Cached Resource)
# =============================================================================

@st.cache_resource
def get_s3_client():
    """
    Get a cached S3 client instance.
    Using @st.cache_resource ensures the client is created once and reused.
    """
    return boto3.client('s3')


@st.cache_resource
def get_s3_resource():
    """Get a cached S3 resource instance for higher-level operations."""
    return boto3.resource('s3')


# =============================================================================
# Hash-Based Change Detection
# =============================================================================

class HashTracker:
    """
    Tracks content hashes to detect changes and enable delta loading.
    Stores hash metadata in session state for persistence across reruns.
    """
    
    HASH_KEY = "_data_loader_hashes"
    
    @classmethod
    def _get_hash_store(cls) -> Dict[str, Dict[str, Any]]:
        """Get or initialize the hash store in session state."""
        if cls.HASH_KEY not in st.session_state:
            st.session_state[cls.HASH_KEY] = {}
        return st.session_state[cls.HASH_KEY]
    
    @classmethod
    def compute_hash(cls, content: bytes) -> str:
        """Compute MD5 hash of content."""
        return hashlib.md5(content).hexdigest()
    
    @classmethod
    def get_s3_etag(cls, bucket: str, key: str) -> Optional[str]:
        """
        Get ETag (hash) from S3 object metadata without downloading.
        This is the most efficient way to check for changes.
        """
        try:
            s3 = get_s3_client()
            response = s3.head_object(Bucket=bucket, Key=key)
            return response.get('ETag', '').strip('"')
        except Exception as e:
            logger.warning(f"Failed to get ETag for {bucket}/{key}: {e}")
            return None
    
    @classmethod
    def has_changed(cls, key: str, new_hash: str) -> bool:
        """Check if content has changed based on hash comparison."""
        store = cls._get_hash_store()
        old_hash = store.get(key, {}).get('hash')
        return old_hash != new_hash
    
    @classmethod
    def update_hash(cls, key: str, hash_value: str, metadata: Optional[Dict] = None):
        """Update stored hash with optional metadata."""
        store = cls._get_hash_store()
        store[key] = {
            'hash': hash_value,
            'updated_at': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
    
    @classmethod
    def get_last_sync_time(cls, key: str) -> Optional[datetime]:
        """Get the last sync time for a given key."""
        store = cls._get_hash_store()
        if key in store and 'updated_at' in store[key]:
            return datetime.fromisoformat(store[key]['updated_at'])
        return None


# =============================================================================
# Optimized Data Loader
# =============================================================================

class OptimizedDataLoader:
    """
    High-performance data loader with multiple optimization strategies.
    
    Features:
    - Streamlit caching with configurable TTL
    - Delta loading based on timestamps or hashes
    - Streaming for large files
    - Chunked processing for memory efficiency
    """
    
    def __init__(self, bucket_name: str, cache_ttl: int = 3600):
        """
        Initialize the data loader.
        
        Args:
            bucket_name: S3 bucket name
            cache_ttl: Cache time-to-live in seconds (default: 1 hour)
        """
        self.bucket_name = bucket_name
        self.cache_ttl = cache_ttl
        self.s3_client = get_s3_client()
    
    # -------------------------------------------------------------------------
    # Core Loading Methods
    # -------------------------------------------------------------------------
    
    def load_json(self, key: str, force_refresh: bool = False) -> Optional[Dict]:
        """
        Load JSON file from S3 with caching and change detection.
        
        Args:
            key: S3 object key
            force_refresh: Bypass cache if True
            
        Returns:
            Parsed JSON as dictionary, or None if not found
        """
        cache_key = f"{self.bucket_name}/{key}"
        
        # Check for changes using ETag
        current_etag = HashTracker.get_s3_etag(self.bucket_name, key)
        
        if not force_refresh and current_etag:
            if not HashTracker.has_changed(cache_key, current_etag):
                # Try to get from session state cache
                cached = self._get_session_cache(cache_key)
                if cached is not None:
                    logger.info(f"Using cached data for {key}")
                    return cached
        
        # Load from S3
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read()
            data = json.loads(content.decode('utf-8'))
            
            # Update cache and hash
            self._set_session_cache(cache_key, data)
            if current_etag:
                HashTracker.update_hash(cache_key, current_etag)
            
            logger.info(f"Loaded fresh data for {key}")
            return data
            
        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"Key not found: {key}")
            return None
        except Exception as e:
            logger.error(f"Error loading {key}: {e}")
            raise
    
    def load_csv(self, key: str, **pandas_kwargs) -> Optional[pd.DataFrame]:
        """
        Load CSV file from S3 with caching.
        
        Args:
            key: S3 object key
            **pandas_kwargs: Additional arguments for pd.read_csv
            
        Returns:
            DataFrame or None if not found
        """
        cache_key = f"{self.bucket_name}/{key}_df"
        current_etag = HashTracker.get_s3_etag(self.bucket_name, key)
        
        if current_etag and not HashTracker.has_changed(cache_key, current_etag):
            cached = self._get_session_cache(cache_key)
            if cached is not None:
                logger.info(f"Using cached DataFrame for {key}")
                return cached
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            df = pd.read_csv(io.BytesIO(response['Body'].read()), **pandas_kwargs)
            
            self._set_session_cache(cache_key, df)
            if current_etag:
                HashTracker.update_hash(cache_key, current_etag)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV {key}: {e}")
            return None
    
    # -------------------------------------------------------------------------
    # Delta Loading Methods
    # -------------------------------------------------------------------------
    
    def load_delta(
        self,
        prefix: str,
        last_sync: Optional[datetime] = None,
        date_pattern: str = "%Y/%m/%d"
    ) -> List[Dict]:
        """
        Load only files modified since last sync (delta loading).
        
        Args:
            prefix: S3 prefix to scan
            last_sync: Datetime of last successful sync
            date_pattern: Date pattern for partition-aware loading
            
        Returns:
            List of loaded data items
        """
        if last_sync is None:
            last_sync = HashTracker.get_last_sync_time(prefix) or datetime.min
        
        results = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['LastModified'].replace(tzinfo=None) > last_sync:
                    try:
                        data = self.load_json(obj['Key'])
                        if data:
                            results.append({
                                'key': obj['Key'],
                                'data': data,
                                'modified': obj['LastModified']
                            })
                    except Exception as e:
                        logger.warning(f"Failed to load {obj['Key']}: {e}")
        
        # Update sync time
        HashTracker.update_hash(prefix, datetime.now().isoformat())
        
        logger.info(f"Delta load: {len(results)} new/modified files from {prefix}")
        return results
    
    def load_partitioned(
        self,
        prefix: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        date_format: str = "%Y/%m/%d"
    ) -> List[Dict]:
        """
        Load data from date-partitioned S3 paths.
        
        Assumes structure: prefix/YYYY/MM/DD/files
        
        Args:
            prefix: Base S3 prefix
            start_date: Start of date range
            end_date: End of date range (defaults to now)
            date_format: Date format for partition paths
            
        Returns:
            List of loaded data items
        """
        if end_date is None:
            end_date = datetime.now()
        
        results = []
        current_date = start_date
        
        while current_date <= end_date:
            partition_prefix = f"{prefix}/{current_date.strftime(date_format)}/"
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=partition_prefix
                )
                
                for obj in response.get('Contents', []):
                    data = self.load_json(obj['Key'])
                    if data:
                        results.append(data)
                        
            except Exception as e:
                logger.warning(f"Error loading partition {partition_prefix}: {e}")
            
            current_date += timedelta(days=1)
        
        return results
    
    # -------------------------------------------------------------------------
    # Streaming Methods
    # -------------------------------------------------------------------------
    
    def stream_lines(self, key: str, chunk_size: int = 8192) -> Generator[str, None, None]:
        """
        Stream a file line by line without loading into memory.
        
        Args:
            key: S3 object key
            chunk_size: Size of chunks to read
            
        Yields:
            Individual lines from the file
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            stream = response['Body']
            
            for line in stream.iter_lines(chunk_size=chunk_size):
                if line:
                    yield line.decode('utf-8')
                    
        except Exception as e:
            logger.error(f"Error streaming {key}: {e}")
            raise
    
    def stream_csv_chunks(
        self,
        key: str,
        chunksize: int = 10000,
        **pandas_kwargs
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Stream a CSV file in chunks for memory-efficient processing.
        
        Args:
            key: S3 object key
            chunksize: Number of rows per chunk
            **pandas_kwargs: Additional pandas arguments
            
        Yields:
            DataFrame chunks
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            
            for chunk in pd.read_csv(
                io.BytesIO(response['Body'].read()),
                chunksize=chunksize,
                **pandas_kwargs
            ):
                yield chunk
                
        except Exception as e:
            logger.error(f"Error streaming CSV {key}: {e}")
            raise
    
    # -------------------------------------------------------------------------
    # Research Findings Optimization
    # -------------------------------------------------------------------------
    
    def load_research_findings(
        self,
        prefix: str = "research-findings",
        days_back: int = 7,
        use_summary: bool = True
    ) -> Dict[str, Any]:
        """
        Optimized loading for research findings.
        
        Args:
            prefix: Research findings prefix
            days_back: Number of days of history to load
            use_summary: If True, prefer summary over individual files
            
        Returns:
            Consolidated research findings
        """
        result = {
            'findings': [],
            'summary': None,
            'historical_context': None
        }
        
        # Try to load summary first (most efficient)
        if use_summary:
            summary = self.load_json(f"{prefix}/summary/latest.json")
            if summary:
                result['summary'] = summary
        
        # Load historical context
        historical = self.load_json(f"{prefix}/archive/historical-context.json")
        if historical:
            result['historical_context'] = historical
        
        # Load recent daily findings using delta loading
        start_date = datetime.now() - timedelta(days=days_back)
        result['findings'] = self.load_partitioned(prefix, start_date)
        
        return result
    
    # -------------------------------------------------------------------------
    # Session Cache Helpers
    # -------------------------------------------------------------------------
    
    def _get_session_cache(self, key: str) -> Optional[Any]:
        """Get value from session state cache."""
        cache_key = f"_loader_cache_{key}"
        if cache_key in st.session_state:
            cached = st.session_state[cache_key]
            # Check TTL
            if datetime.now() - cached['timestamp'] < timedelta(seconds=self.cache_ttl):
                return cached['data']
        return None
    
    def _set_session_cache(self, key: str, data: Any):
        """Set value in session state cache."""
        cache_key = f"_loader_cache_{key}"
        st.session_state[cache_key] = {
            'data': data,
            'timestamp': datetime.now()
        }


# =============================================================================
# Cached Data Loading Functions (Decorator-based)
# =============================================================================

def cached_s3_load(ttl: int = 3600, hash_check: bool = True):
    """
    Decorator for caching S3 data loading functions.
    
    Args:
        ttl: Cache time-to-live in seconds
        hash_check: Enable hash-based change detection
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key from function and arguments
            cache_key = f"{func.__name__}_{hash(str(args) + str(kwargs))}"
            
            # Check existing cache
            if f"_cache_{cache_key}" in st.session_state:
                cached = st.session_state[f"_cache_{cache_key}"]
                if datetime.now() - cached['timestamp'] < timedelta(seconds=ttl):
                    return cached['data']
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Cache result
            st.session_state[f"_cache_{cache_key}"] = {
                'data': result,
                'timestamp': datetime.now()
            }
            
            return result
        return wrapper
    return decorator


# =============================================================================
# Convenience Functions
# =============================================================================

@st.cache_data(ttl=3600)
def load_and_cache_dataframe(bucket: str, key: str) -> pd.DataFrame:
    """
    Simple cached DataFrame loader using Streamlit's native caching.
    
    This is the most straightforward approach for most use cases.
    """
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()))


def quick_load_json(bucket: str, key: str) -> Optional[Dict]:
    """Quick JSON loader with basic caching."""
    loader = OptimizedDataLoader(bucket)
    return loader.load_json(key)


def get_data_loader(bucket: str, ttl: int = 3600) -> OptimizedDataLoader:
    """Factory function to get a configured data loader."""
    return OptimizedDataLoader(bucket, ttl)


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    # Example usage patterns
    
    # 1. Basic cached loading
    # loader = get_data_loader("my-bucket")
    # data = loader.load_json("config/settings.json")
    
    # 2. Delta loading for research findings
    # findings = loader.load_delta("research-findings/2024", 
    #                              last_sync=datetime(2024, 1, 1))
    
    # 3. Streaming for large files
    # for line in loader.stream_lines("logs/large-log.txt"):
    #     process_line(line)
    
    # 4. Chunked CSV processing
    # for chunk in loader.stream_csv_chunks("data/large-dataset.csv"):
    #     process_chunk(chunk)
    
    print("Data loader module loaded successfully!")
