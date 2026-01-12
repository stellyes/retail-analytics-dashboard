"""
Prompt Optimization Module for Claude API Integration

This module provides utilities to optimize prompts for the Claude API:
- Context compression and summarization
- Relevance filtering for data inclusion
- Template abstraction for recurring prompts
- Token estimation and budgeting
- Tiered model selection (Haiku for scan, Sonnet for analysis)

Author: Generated for stellyes/retail-analytics-dashboard
"""

import streamlit as st
import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# Constants and Configuration
# =============================================================================

class ClaudeModel(Enum):
    """Available Claude models with cost information."""
    HAIKU = "claude-3-haiku-20240307"
    SONNET = "claude-3-5-sonnet-20241022"
    OPUS = "claude-3-opus-20240229"


# Approximate costs per 1M tokens (input/output)
MODEL_COSTS = {
    ClaudeModel.HAIKU: {"input": 0.25, "output": 1.25},
    ClaudeModel.SONNET: {"input": 3.00, "output": 15.00},
    ClaudeModel.OPUS: {"input": 15.00, "output": 75.00},
}

# Approximate tokens per character (varies by content)
CHARS_PER_TOKEN = 4


@dataclass
class PromptConfig:
    """Configuration for prompt optimization."""
    max_tokens: int = 100000
    target_compression_ratio: float = 0.5
    include_timestamps: bool = True
    use_summaries: bool = True
    relevance_threshold: float = 0.7


# =============================================================================
# Token Estimation
# =============================================================================

class TokenEstimator:
    """
    Estimates token counts for text content.
    Uses character-based heuristics for quick estimation.
    """
    
    @staticmethod
    def estimate_tokens(text: str) -> int:
        """
        Estimate token count for a given text.
        
        Args:
            text: Input text
            
        Returns:
            Estimated token count
        """
        if not text:
            return 0
        return len(text) // CHARS_PER_TOKEN
    
    @staticmethod
    def estimate_cost(
        input_tokens: int,
        output_tokens: int,
        model: ClaudeModel = ClaudeModel.SONNET
    ) -> float:
        """
        Estimate API cost for a request.
        
        Args:
            input_tokens: Number of input tokens
            output_tokens: Expected output tokens
            model: Claude model to use
            
        Returns:
            Estimated cost in USD
        """
        costs = MODEL_COSTS[model]
        input_cost = (input_tokens / 1_000_000) * costs["input"]
        output_cost = (output_tokens / 1_000_000) * costs["output"]
        return input_cost + output_cost
    
    @staticmethod
    def fits_context(text: str, max_tokens: int = 100000) -> bool:
        """Check if text fits within context window."""
        return TokenEstimator.estimate_tokens(text) <= max_tokens


# =============================================================================
# Context Compression
# =============================================================================

class ContextCompressor:
    """
    Compresses context data to reduce token usage while preserving key information.
    """
    
    @staticmethod
    def compress_dataframe_context(
        df_info: Dict[str, Any],
        max_rows: int = 10,
        include_stats: bool = True
    ) -> str:
        """
        Compress DataFrame information for prompt inclusion.
        
        Args:
            df_info: Dictionary containing DataFrame metadata
            max_rows: Maximum sample rows to include
            include_stats: Include statistical summaries
            
        Returns:
            Compressed context string
        """
        parts = []
        
        # Basic info
        if 'shape' in df_info:
            parts.append(f"Dataset: {df_info['shape'][0]} rows, {df_info['shape'][1]} columns")
        
        # Column info (compressed)
        if 'columns' in df_info:
            cols = df_info['columns']
            if len(cols) > 10:
                parts.append(f"Columns: {', '.join(cols[:5])} ... ({len(cols)} total)")
            else:
                parts.append(f"Columns: {', '.join(cols)}")
        
        # Key statistics only
        if include_stats and 'stats' in df_info:
            stats = df_info['stats']
            key_metrics = ['mean', 'sum', 'count']
            stat_parts = []
            for metric in key_metrics:
                if metric in stats:
                    stat_parts.append(f"{metric}: {stats[metric]}")
            if stat_parts:
                parts.append(f"Key metrics: {', '.join(stat_parts)}")
        
        return "\n".join(parts)
    
    @staticmethod
    def compress_findings(
        findings: List[Dict],
        max_findings: int = 5,
        max_content_length: int = 200
    ) -> str:
        """
        Compress research findings for prompt inclusion.
        
        Args:
            findings: List of finding dictionaries
            max_findings: Maximum number of findings to include
            max_content_length: Maximum length per finding
            
        Returns:
            Compressed findings string
        """
        if not findings:
            return "No recent findings available."
        
        compressed = []
        for finding in findings[:max_findings]:
            # Extract key information
            topic = finding.get('topic', 'General')
            date = finding.get('date', 'Unknown date')
            content = finding.get('content', finding.get('summary', ''))
            
            # Truncate content if needed
            if len(content) > max_content_length:
                content = content[:max_content_length] + "..."
            
            compressed.append(f"[{topic} - {date}]: {content}")
        
        if len(findings) > max_findings:
            compressed.append(f"... and {len(findings) - max_findings} more findings")
        
        return "\n".join(compressed)
    
    @staticmethod
    def create_summary_context(
        data: Dict[str, Any],
        key_fields: Optional[List[str]] = None
    ) -> str:
        """
        Create a summarized context from a data dictionary.
        
        Args:
            data: Input data dictionary
            key_fields: Fields to prioritize (others are summarized)
            
        Returns:
            Summarized context string
        """
        if not data:
            return ""
        
        if key_fields is None:
            key_fields = ['summary', 'key_points', 'recommendations', 'trends']
        
        parts = []
        
        # Include key fields in full
        for field in key_fields:
            if field in data:
                value = data[field]
                if isinstance(value, list):
                    parts.append(f"{field.title()}: {', '.join(str(v) for v in value[:5])}")
                elif isinstance(value, dict):
                    parts.append(f"{field.title()}: {json.dumps(value, default=str)[:200]}")
                else:
                    parts.append(f"{field.title()}: {str(value)[:300]}")
        
        # Summarize other fields
        other_fields = [k for k in data.keys() if k not in key_fields]
        if other_fields:
            parts.append(f"Additional data fields: {', '.join(other_fields[:10])}")
        
        return "\n".join(parts)


# =============================================================================
# Prompt Templates
# =============================================================================

class PromptTemplates:
    """
    Reusable prompt templates to reduce token overhead.
    Templates use placeholders for dynamic content.
    """
    
    RECOMMENDATION_SCAN = """
Analyze the following retail data summary and determine if there are actionable insights:

{context}

Respond with:
1. RELEVANT: Yes/No - Is there significant data warranting detailed analysis?
2. TOPICS: List key topics identified (comma-separated)
3. PRIORITY: High/Medium/Low
"""
    
    RECOMMENDATION_DETAILED = """
Based on the retail analytics data, provide strategic business recommendations:

Context:
{context}

Focus Areas:
{focus_areas}

Provide:
1. Top 3-5 actionable recommendations
2. Expected impact for each
3. Implementation timeline suggestions
4. Key metrics to track
"""
    
    RESEARCH_SUMMARY = """
Summarize the following research findings for business stakeholders:

{findings}

Create a brief executive summary covering:
1. Key market trends
2. Competitive insights  
3. Regulatory updates
4. Recommended actions
"""
    
    DATA_ANALYSIS = """
Analyze the following dataset characteristics:

{data_description}

Provide insights on:
1. Data quality observations
2. Notable patterns or anomalies
3. Suggested analyses to perform
"""
    
    @classmethod
    def get_template(cls, template_name: str) -> str:
        """Get a template by name."""
        templates = {
            'recommendation_scan': cls.RECOMMENDATION_SCAN,
            'recommendation_detailed': cls.RECOMMENDATION_DETAILED,
            'research_summary': cls.RESEARCH_SUMMARY,
            'data_analysis': cls.DATA_ANALYSIS,
        }
        return templates.get(template_name, "")
    
    @classmethod
    def fill_template(cls, template_name: str, **kwargs) -> str:
        """Fill a template with provided values."""
        template = cls.get_template(template_name)
        return template.format(**kwargs)


# =============================================================================
# Tiered Model Selector
# =============================================================================

class ModelSelector:
    """
    Implements tiered model selection strategy:
    - Use Haiku for quick scans and classification
    - Use Sonnet for detailed analysis when warranted
    """
    
    @staticmethod
    def select_model_for_task(
        task_type: str,
        content_length: int,
        priority: str = "medium"
    ) -> ClaudeModel:
        """
        Select appropriate model based on task requirements.
        
        Args:
            task_type: Type of task (scan, analysis, generation)
            content_length: Length of content to process
            priority: Priority level (low, medium, high)
            
        Returns:
            Recommended Claude model
        """
        # Quick scans always use Haiku
        if task_type in ['scan', 'classify', 'validate']:
            return ClaudeModel.HAIKU
        
        # Short content with low priority uses Haiku
        if content_length < 5000 and priority == 'low':
            return ClaudeModel.HAIKU
        
        # Complex analysis uses Sonnet
        if task_type in ['analysis', 'recommendation', 'synthesis']:
            return ClaudeModel.SONNET
        
        # Default to Sonnet for most tasks
        return ClaudeModel.SONNET
    
    @staticmethod
    def estimate_savings(
        original_model: ClaudeModel,
        optimized_model: ClaudeModel,
        input_tokens: int,
        output_tokens: int
    ) -> float:
        """Calculate cost savings from model optimization."""
        original_cost = TokenEstimator.estimate_cost(
            input_tokens, output_tokens, original_model
        )
        optimized_cost = TokenEstimator.estimate_cost(
            input_tokens, output_tokens, optimized_model
        )
        return original_cost - optimized_cost


# =============================================================================
# Prompt Optimizer (Main Class)
# =============================================================================

class PromptOptimizer:
    """
    Main class for prompt optimization.
    Combines compression, templating, and model selection.
    """
    
    def __init__(self, config: Optional[PromptConfig] = None):
        """Initialize with optional configuration."""
        self.config = config or PromptConfig()
        self.compressor = ContextCompressor()
        self.estimator = TokenEstimator()
        self.model_selector = ModelSelector()
    
    def optimize_recommendation_prompt(
        self,
        data_context: Dict[str, Any],
        research_findings: Optional[List[Dict]] = None,
        historical_context: Optional[str] = None
    ) -> Tuple[str, ClaudeModel, Dict[str, Any]]:
        """
        Create an optimized prompt for the recommendations page.
        
        Args:
            data_context: Current data metrics and summaries
            research_findings: Recent research findings
            historical_context: Historical analysis context
            
        Returns:
            Tuple of (optimized_prompt, recommended_model, metadata)
        """
        parts = []
        
        # Compress data context
        if data_context:
            compressed_data = self.compressor.create_summary_context(
                data_context,
                key_fields=['summary', 'key_metrics', 'trends', 'anomalies']
            )
            parts.append(f"Current Data:\n{compressed_data}")
        
        # Compress research findings
        if research_findings:
            compressed_findings = self.compressor.compress_findings(
                research_findings,
                max_findings=5,
                max_content_length=150
            )
            parts.append(f"\nRecent Research:\n{compressed_findings}")
        
        # Add historical context (truncated)
        if historical_context:
            max_history = 500
            if len(historical_context) > max_history:
                historical_context = historical_context[:max_history] + "..."
            parts.append(f"\nHistorical Context:\n{historical_context}")
        
        context = "\n".join(parts)
        
        # Estimate tokens
        token_count = self.estimator.estimate_tokens(context)
        
        # Select model based on complexity
        model = self.model_selector.select_model_for_task(
            'recommendation',
            len(context),
            'high'
        )
        
        # Build final prompt
        prompt = PromptTemplates.fill_template(
            'recommendation_detailed',
            context=context,
            focus_areas="Sales trends, Customer behavior, Market opportunities"
        )
        
        metadata = {
            'original_context_length': len(str(data_context)) + len(str(research_findings)),
            'compressed_context_length': len(context),
            'compression_ratio': len(context) / max(1, len(str(data_context)) + len(str(research_findings))),
            'estimated_tokens': token_count,
            'estimated_cost': self.estimator.estimate_cost(token_count, 1000, model),
            'selected_model': model.value
        }
        
        return prompt, model, metadata
    
    def create_scan_prompt(
        self,
        content: str,
        scan_type: str = "relevance"
    ) -> Tuple[str, ClaudeModel]:
        """
        Create a quick scan prompt using Haiku.
        
        Use this for initial classification before detailed analysis.
        """
        # Truncate content for scan
        max_scan_length = 2000
        if len(content) > max_scan_length:
            content = content[:max_scan_length] + "... [truncated for scan]"
        
        prompt = PromptTemplates.fill_template(
            'recommendation_scan',
            context=content
        )
        
        return prompt, ClaudeModel.HAIKU
    
    def should_proceed_with_analysis(self, scan_response: str) -> bool:
        """
        Determine if full analysis is warranted based on scan response.
        
        Args:
            scan_response: Response from Haiku scan
            
        Returns:
            True if detailed analysis should proceed
        """
        response_lower = scan_response.lower()
        
        # Check for relevance indicator
        if 'relevant: yes' in response_lower:
            return True
        if 'priority: high' in response_lower:
            return True
        
        return False


# =============================================================================
# Response Caching
# =============================================================================

class ResponseCache:
    """
    Caches Claude API responses to avoid redundant calls.
    Uses content hash for cache key generation.
    """
    
    CACHE_KEY = "_prompt_response_cache"
    
    @classmethod
    def _get_cache(cls) -> Dict[str, Dict]:
        """Get or initialize the response cache."""
        if cls.CACHE_KEY not in st.session_state:
            st.session_state[cls.CACHE_KEY] = {}
        return st.session_state[cls.CACHE_KEY]
    
    @classmethod
    def generate_key(cls, prompt: str, model: ClaudeModel) -> str:
        """Generate a cache key from prompt content and model."""
        content = f"{prompt}|{model.value}"
        return hashlib.md5(content.encode()).hexdigest()
    
    @classmethod
    def get(cls, prompt: str, model: ClaudeModel, ttl_hours: int = 24) -> Optional[str]:
        """
        Get cached response if available and not expired.
        
        Args:
            prompt: The prompt text
            model: Claude model used
            ttl_hours: Cache TTL in hours
            
        Returns:
            Cached response or None
        """
        cache = cls._get_cache()
        key = cls.generate_key(prompt, model)
        
        if key in cache:
            entry = cache[key]
            age = datetime.now() - entry['timestamp']
            if age < timedelta(hours=ttl_hours):
                logger.info("Using cached API response")
                return entry['response']
        
        return None
    
    @classmethod
    def set(cls, prompt: str, model: ClaudeModel, response: str):
        """Cache a response."""
        cache = cls._get_cache()
        key = cls.generate_key(prompt, model)
        
        cache[key] = {
            'response': response,
            'timestamp': datetime.now(),
            'model': model.value
        }
        
        logger.info(f"Cached API response (key: {key[:8]}...)")
    
    @classmethod
    def clear_expired(cls, ttl_hours: int = 24):
        """Remove expired cache entries."""
        cache = cls._get_cache()
        now = datetime.now()
        expired_keys = [
            k for k, v in cache.items()
            if now - v['timestamp'] > timedelta(hours=ttl_hours)
        ]
        for key in expired_keys:
            del cache[key]
        
        if expired_keys:
            logger.info(f"Cleared {len(expired_keys)} expired cache entries")


# =============================================================================
# Convenience Functions
# =============================================================================

def optimize_prompt(
    context: Dict[str, Any],
    findings: Optional[List[Dict]] = None
) -> Tuple[str, str, Dict]:
    """
    Convenience function for prompt optimization.
    
    Returns:
        Tuple of (prompt, model_name, metadata)
    """
    optimizer = PromptOptimizer()
    prompt, model, metadata = optimizer.optimize_recommendation_prompt(
        context,
        findings
    )
    return prompt, model.value, metadata


def get_cached_or_call(
    prompt: str,
    model: ClaudeModel,
    api_call_fn: Callable[[str, str], str],
    ttl_hours: int = 24
) -> str:
    """
    Get cached response or make API call.
    
    Args:
        prompt: The prompt text
        model: Claude model to use
        api_call_fn: Function to call API (takes prompt and model name)
        ttl_hours: Cache TTL
        
    Returns:
        API response (cached or fresh)
    """
    # Check cache first
    cached = ResponseCache.get(prompt, model, ttl_hours)
    if cached:
        return cached
    
    # Make API call
    response = api_call_fn(prompt, model.value)
    
    # Cache the response
    ResponseCache.set(prompt, model, response)
    
    return response


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    # Example: Optimize a recommendation prompt
    
    sample_context = {
        'summary': 'Q4 sales showed 15% YoY growth',
        'key_metrics': {
            'revenue': 1250000,
            'transactions': 45000,
            'avg_basket': 27.78
        },
        'trends': ['Increasing online orders', 'Weekend peak sales'],
        'anomalies': ['Unusual spike on Black Friday']
    }
    
    sample_findings = [
        {
            'topic': 'Market Trends',
            'date': '2024-01-15',
            'content': 'Industry analysts predict continued growth in the sector...'
        },
        {
            'topic': 'Regulatory',
            'date': '2024-01-14',
            'content': 'New compliance requirements effective Q2...'
        }
    ]
    
    optimizer = PromptOptimizer()
    prompt, model, metadata = optimizer.optimize_recommendation_prompt(
        sample_context,
        sample_findings
    )
    
    print("Optimized Prompt:")
    print("-" * 50)
    print(prompt[:500] + "...")
    print("-" * 50)
    print(f"Selected Model: {model.value}")
    print(f"Compression Ratio: {metadata['compression_ratio']:.2%}")
    print(f"Estimated Tokens: {metadata['estimated_tokens']}")
    print(f"Estimated Cost: ${metadata['estimated_cost']:.4f}")
