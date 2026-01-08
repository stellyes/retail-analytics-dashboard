"""
Retail Analytics Dashboard - Consolidated Import Package

This package provides a single import point for ALL dashboard functionality.
No loose .py files needed - everything is contained in this package.

Usage:
    from dashboard import (
        # Data Management
        S3DataManager,
        DataProcessor,
        AnalyticsEngine,
        load_invoice_data_from_dynamodb,

        # Configuration
        AppConfig,
        STORE_MAPPING,
        STORE_DISPLAY_NAMES,

        # UI Components
        plot_sales_trend,
        plot_brand_performance,
        plot_store_comparison,
        check_password,

        # Utilities
        compute_data_hash,
        make_json_serializable,
        clear_all_caches,

        # Services (with availability flags)
        ClaudeAnalytics,
        CLAUDE_AVAILABLE,
        InvoiceDataService,
        INVOICE_AVAILABLE,
        render_full_invoice_section,
        render_research_page,
        render_seo_page,
        render_qr_page,
    )
"""

# =============================================================================
# Core Configuration & Utilities
# =============================================================================
from .core.config import (
    STORE_MAPPING,
    STORE_DISPLAY_NAMES,
    SAMPLE_PREFIXES,
    AppConfig,
)
from .core.cache import (
    compute_data_hash,
    CacheManager,
    clear_all_caches,
    cached_data_loader,
)
from .core.utils import (
    make_json_serializable,
    safe_json_dumps,
)

# =============================================================================
# Data Management
# =============================================================================
from .data.s3_manager import S3DataManager
from .data.processor import DataProcessor
from .data.analytics import AnalyticsEngine
from .data.dynamodb import load_invoice_data_from_dynamodb

# =============================================================================
# UI Components
# =============================================================================
from .ui.charts import (
    plot_sales_trend,
    plot_category_breakdown,
    plot_brand_performance,
    plot_store_comparison,
    plot_margin_vs_sales,
)
from .ui.auth import (
    check_password,
    logout,
    get_current_user,
    is_admin,
)
from .ui.loading import (
    show_loading_overlay,
    hide_loading_overlay,
)

# =============================================================================
# External Services (with availability flags)
# =============================================================================
from .services import (
    # Availability flags
    CLAUDE_AVAILABLE,
    INVOICE_AVAILABLE,
    INVOICE_UPLOAD_AVAILABLE,
    RESEARCH_AVAILABLE,
    SEO_AVAILABLE,
    MANUAL_RESEARCH_AVAILABLE,
    QR_AVAILABLE,
    BUSINESS_CONTEXT_AVAILABLE,
    # Claude
    ClaudeAnalytics,
    # Invoice
    TreezInvoiceParser,
    InvoiceDataService,
    # Invoice Upload
    render_full_invoice_section,
    # Research
    render_research_page,
    ResearchFindingsViewer,
    # SEO
    render_seo_page,
    SEOFindingsViewer,
    # Manual Research
    MonthlyResearchSummarizer,
    DocumentStorage,
    S3_BUCKET,
    # QR
    render_qr_page,
    # Business Context
    BusinessContextService,
    get_business_context_service,
)

# =============================================================================
# Package Metadata
# =============================================================================
__version__ = "2.0.0"
__author__ = "Retail Analytics Team"

__all__ = [
    # Version info
    '__version__',
    '__author__',

    # Configuration
    'STORE_MAPPING',
    'STORE_DISPLAY_NAMES',
    'SAMPLE_PREFIXES',
    'AppConfig',

    # Cache utilities
    'compute_data_hash',
    'CacheManager',
    'clear_all_caches',
    'cached_data_loader',

    # Serialization utilities
    'make_json_serializable',
    'safe_json_dumps',

    # Data management
    'S3DataManager',
    'DataProcessor',
    'AnalyticsEngine',
    'load_invoice_data_from_dynamodb',

    # Visualization
    'plot_sales_trend',
    'plot_category_breakdown',
    'plot_brand_performance',
    'plot_store_comparison',
    'plot_margin_vs_sales',

    # Authentication
    'check_password',
    'logout',
    'get_current_user',
    'is_admin',

    # Loading UI
    'show_loading_overlay',
    'hide_loading_overlay',

    # Service availability flags
    'CLAUDE_AVAILABLE',
    'INVOICE_AVAILABLE',
    'INVOICE_UPLOAD_AVAILABLE',
    'RESEARCH_AVAILABLE',
    'SEO_AVAILABLE',
    'MANUAL_RESEARCH_AVAILABLE',
    'QR_AVAILABLE',
    'BUSINESS_CONTEXT_AVAILABLE',

    # Services (conditionally available)
    'ClaudeAnalytics',
    'TreezInvoiceParser',
    'InvoiceDataService',
    'render_full_invoice_section',
    'render_research_page',
    'ResearchFindingsViewer',
    'render_seo_page',
    'SEOFindingsViewer',
    'MonthlyResearchSummarizer',
    'DocumentStorage',
    'S3_BUCKET',
    'render_qr_page',
    'BusinessContextService',
    'get_business_context_service',
]
