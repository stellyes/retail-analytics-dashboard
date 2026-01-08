"""
External service integrations for the Retail Analytics Dashboard.
All service modules are now part of this package.
"""

# Claude AI integration
try:
    from .claude_integration import ClaudeAnalytics
    CLAUDE_AVAILABLE = True
except ImportError:
    CLAUDE_AVAILABLE = False
    ClaudeAnalytics = None

# Invoice extraction
try:
    from .invoice_extraction import TreezInvoiceParser, InvoiceDataService
    INVOICE_AVAILABLE = True
except ImportError:
    INVOICE_AVAILABLE = False
    TreezInvoiceParser = None
    InvoiceDataService = None

# Invoice upload UI
try:
    from .invoice_upload_ui import render_full_invoice_section
    INVOICE_UPLOAD_AVAILABLE = True
except ImportError:
    INVOICE_UPLOAD_AVAILABLE = False
    render_full_invoice_section = None

# Research integration
try:
    from .research_integration import render_research_page, ResearchFindingsViewer
    RESEARCH_AVAILABLE = True
except ImportError:
    RESEARCH_AVAILABLE = False
    render_research_page = None
    ResearchFindingsViewer = None

# SEO integration
try:
    from .seo_integration import render_seo_page, SEOFindingsViewer
    SEO_AVAILABLE = True
except ImportError:
    SEO_AVAILABLE = False
    render_seo_page = None
    SEOFindingsViewer = None

# Manual research integration
try:
    from .manual_research_integration import MonthlyResearchSummarizer, DocumentStorage, S3_BUCKET
    MANUAL_RESEARCH_AVAILABLE = True
except ImportError:
    MANUAL_RESEARCH_AVAILABLE = False
    MonthlyResearchSummarizer = None
    DocumentStorage = None
    S3_BUCKET = None

# QR integration
try:
    from .qr_integration import render_qr_page
    QR_AVAILABLE = True
except ImportError:
    QR_AVAILABLE = False
    render_qr_page = None

# Business context
try:
    from .business_context import BusinessContextService, get_business_context_service
    BUSINESS_CONTEXT_AVAILABLE = True
except ImportError:
    BUSINESS_CONTEXT_AVAILABLE = False
    BusinessContextService = None
    get_business_context_service = None


__all__ = [
    # Availability flags
    'CLAUDE_AVAILABLE',
    'INVOICE_AVAILABLE',
    'INVOICE_UPLOAD_AVAILABLE',
    'RESEARCH_AVAILABLE',
    'SEO_AVAILABLE',
    'MANUAL_RESEARCH_AVAILABLE',
    'QR_AVAILABLE',
    'BUSINESS_CONTEXT_AVAILABLE',
    # Claude
    'ClaudeAnalytics',
    # Invoice
    'TreezInvoiceParser',
    'InvoiceDataService',
    # Invoice Upload
    'render_full_invoice_section',
    # Research
    'render_research_page',
    'ResearchFindingsViewer',
    # SEO
    'render_seo_page',
    'SEOFindingsViewer',
    # Manual Research
    'MonthlyResearchSummarizer',
    'DocumentStorage',
    'S3_BUCKET',
    # QR
    'render_qr_page',
    # Business Context
    'BusinessContextService',
    'get_business_context_service',
]
