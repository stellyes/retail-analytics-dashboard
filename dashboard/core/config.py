"""
Application configuration and constants.
"""

from dataclasses import dataclass
from typing import Optional
import os


# Store mappings based on data prefixes
STORE_MAPPING = {
    "Barbary Coast - SF Mission": "barbary_coast",
    "Grass Roots - SF": "grass_roots",
}

STORE_DISPLAY_NAMES = {
    "barbary_coast": "Barbary Coast",
    "grass_roots": "Grass Roots"
}

# Sample prefixes to filter out (not actual sales)
# [DS] = Display Samples, [SS] = Staff Samples
SAMPLE_PREFIXES = ["[DS]", "[SS]"]


@dataclass
class AppConfig:
    """Application configuration settings."""

    # AWS Settings
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    aws_region: str = "us-west-2"
    s3_bucket: Optional[str] = None

    # Cache Settings
    cache_ttl_seconds: int = 86400  # 24 hours

    # API Settings
    anthropic_api_key: Optional[str] = None

    @classmethod
    def from_environment(cls) -> 'AppConfig':
        """Load configuration from environment variables."""
        return cls(
            aws_access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_region=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
            s3_bucket=os.environ.get("S3_BUCKET_NAME"),
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY"),
        )

    @classmethod
    def from_streamlit_secrets(cls) -> 'AppConfig':
        """Load configuration from Streamlit secrets."""
        try:
            import streamlit as st

            aws_secrets = st.secrets.get("aws", {})
            return cls(
                aws_access_key=aws_secrets.get("access_key_id"),
                aws_secret_key=aws_secrets.get("secret_access_key"),
                aws_region=aws_secrets.get("region", "us-west-2"),
                s3_bucket=aws_secrets.get("bucket_name"),
                anthropic_api_key=(
                    st.secrets.get("ANTHROPIC_API_KEY") or
                    st.secrets.get("anthropic", {}).get("ANTHROPIC_API_KEY")
                ),
            )
        except Exception:
            return cls()

    @classmethod
    def load(cls) -> 'AppConfig':
        """Load configuration from environment first, then Streamlit secrets as fallback."""
        config = cls.from_environment()

        # Fill in missing values from Streamlit secrets
        st_config = cls.from_streamlit_secrets()

        if not config.aws_access_key:
            config.aws_access_key = st_config.aws_access_key
        if not config.aws_secret_key:
            config.aws_secret_key = st_config.aws_secret_key
        if not config.s3_bucket:
            config.s3_bucket = st_config.s3_bucket
        if not config.anthropic_api_key:
            config.anthropic_api_key = st_config.anthropic_api_key

        return config
