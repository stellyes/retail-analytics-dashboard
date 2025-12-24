"""
Claude AI Integration Module
Optional module for AI-powered analytics and recommendations using Anthropic's Claude API.
"""

import os
from typing import Optional, Any
import json
from datetime import datetime, date

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# Try to import numpy/pandas for type checking
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def make_json_serializable(obj: Any) -> Any:
    """
    Recursively convert an object to be JSON serializable.
    Handles numpy types, pandas types, datetime objects, etc.
    """
    # Handle None
    if obj is None:
        return None
    
    # Handle numpy types
    if NUMPY_AVAILABLE:
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)
    
    # Handle pandas types
    if PANDAS_AVAILABLE:
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, pd.Timedelta):
            return str(obj)
        # Check for pandas NA/NaN
        try:
            if pd.isna(obj):
                return None
        except (ValueError, TypeError):
            pass  # Not a type that pd.isna can check
    
    # Handle datetime types
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(obj, date):
        return obj.strftime('%Y-%m-%d')
    
    # Handle dictionaries recursively
    if isinstance(obj, dict):
        return {str(k): make_json_serializable(v) for k, v in obj.items()}
    
    # Handle lists/tuples recursively
    if isinstance(obj, (list, tuple)):
        return [make_json_serializable(item) for item in obj]
    
    # Handle sets
    if isinstance(obj, set):
        return [make_json_serializable(item) for item in obj]
    
    # Return as-is if it's a basic type
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    # Last resort: convert to string
    try:
        return str(obj)
    except Exception:
        return "<non-serializable>"


def safe_json_dumps(obj: Any, indent: int = 2) -> str:
    """
    Safely convert an object to a JSON string.
    Handles non-serializable types gracefully.
    """
    try:
        serializable_obj = make_json_serializable(obj)
        return json.dumps(serializable_obj, indent=indent)
    except Exception as e:
        return json.dumps({"error": f"Could not serialize data: {str(e)}"})


class ClaudeAnalytics:
    """
    Integrates Claude AI for advanced retail analytics and recommendations.
    
    Usage:
        analyzer = ClaudeAnalytics()
        if analyzer.is_available():
            insights = analyzer.analyze_sales_trends(sales_data)
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize Claude client."""
        self.client = None
        self.model = "claude-sonnet-4-20250514"  # Default to Sonnet 4
        self.init_error = None
        
        if not ANTHROPIC_AVAILABLE:
            self.init_error = "anthropic package not installed"
            return
            
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if key:
            try:
                self.client = anthropic.Anthropic(api_key=key)
            except Exception as e:
                self.init_error = f"Failed to initialize Anthropic client: {e}"
        else:
            self.init_error = "No API key provided"
    
    def is_available(self) -> bool:
        """Check if Claude API is available and configured."""
        return self.client is not None
    
    def get_error(self) -> Optional[str]:
        """Return initialization error if any."""
        return self.init_error
    
    def analyze_sales_trends(self, sales_summary: dict) -> str:
        """
        Analyze sales data and provide insights.
        
        Args:
            sales_summary: Dictionary containing aggregated sales metrics
        
        Returns:
            AI-generated analysis and recommendations
        """
        if not self.is_available():
            return f"Claude AI not configured: {self.init_error}"
        
        # Safely serialize the data
        data_str = safe_json_dumps(sales_summary)
        
        prompt = f"""You are a retail analytics expert for a cannabis dispensary operation.
        
Analyze the following sales data and provide actionable insights:

{data_str}

Please provide:
1. Key observations about performance trends
2. Comparison between stores (if multiple stores present)
3. Areas of concern that need attention
4. Specific recommendations for improving sales and margins
5. Suggested promotional strategies based on the data

Keep your response concise and actionable. Use bullet points for clarity."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing data: {str(e)}"
    
    def analyze_brand_performance(self, brand_data: list, brand_by_category: dict = None) -> str:
        """
        Analyze brand performance and suggest inventory decisions.

        Args:
            brand_data: List of brand performance dictionaries
            brand_by_category: Optional dict mapping categories to brand performance data

        Returns:
            AI-generated brand recommendations
        """
        if not self.is_available():
            return f"Claude AI not configured: {self.init_error}"

        # Limit to top 50 brands and safely serialize
        limited_data = brand_data[:50] if len(brand_data) > 50 else brand_data
        data_str = safe_json_dumps(limited_data)

        # Add category context if provided
        category_context = ""
        if brand_by_category:
            category_str = safe_json_dumps(brand_by_category)
            category_context = f"\n\nBrand performance by category:\n{category_str}\n"

        prompt = f"""You are a cannabis retail buyer and merchandising expert.

Analyze the following brand performance data:

{data_str}{category_context}

Please provide:
1. Which brands should we increase orders for and why?
2. Which brands should we consider discontinuing or reducing?
3. Any brands with unusual margin profiles that need investigation?
4. Recommendations for brand mix optimization
5. Suggested promotional candidates based on margin and velocity

Focus on actionable buying recommendations. Use specific brand names from the data."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing brands: {str(e)}"

    def analyze_category_performance(self, brand_by_category: dict, brand_summary: list = None) -> str:
        """
        Analyze category performance and provide insights.

        Args:
            brand_by_category: Dict mapping categories to brand performance data
            brand_summary: Optional list of overall brand performance data

        Returns:
            AI-generated category analysis
        """
        if not self.is_available():
            return f"Claude AI not configured: {self.init_error}"

        # Safely serialize the data
        category_str = safe_json_dumps(brand_by_category)

        # Add brand summary context if provided
        brand_context = ""
        if brand_summary:
            limited_brands = brand_summary[:30] if len(brand_summary) > 30 else brand_summary
            brand_str = safe_json_dumps(limited_brands)
            brand_context = f"\n\nOverall brand performance:\n{brand_str}\n"

        prompt = f"""You are a cannabis retail category manager and merchandising expert.

Analyze the following category performance data:

{category_str}{brand_context}

Please provide:
1. Which categories are performing best and why?
2. Which categories need attention or intervention?
3. Category-specific opportunities for growth
4. Recommendations for category mix optimization
5. Suggested category-level promotional strategies

Focus on actionable category management recommendations with specific insights."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing categories: {str(e)}"
    
    def generate_deal_recommendations(self, 
                                       slow_movers: list,
                                       high_margin_items: list,
                                       seasonal_context: str = None) -> str:
        """
        Generate promotional and deal recommendations.
        
        Args:
            slow_movers: List of slow-moving inventory items
            high_margin_items: List of high-margin products for bundling
            seasonal_context: Optional context about upcoming seasons/holidays
        
        Returns:
            AI-generated promotion recommendations
        """
        if not self.is_available():
            return f"Claude AI not configured: {self.init_error}"
        
        context = seasonal_context or "Consider current market conditions"
        
        # Safely serialize the data
        slow_movers_str = safe_json_dumps(slow_movers[:20] if len(slow_movers) > 20 else slow_movers)
        high_margin_str = safe_json_dumps(high_margin_items[:20] if len(high_margin_items) > 20 else high_margin_items)
        
        prompt = f"""You are a retail promotions strategist for cannabis dispensaries.

Context: {context}

Slow-moving inventory that needs movement:
{slow_movers_str}

High-margin products available for deals:
{high_margin_str}

Please recommend:
1. Specific deals/promotions to move slow inventory (e.g., BOGO, % off, bundles)
2. Bundle suggestions pairing slow movers with popular items
3. Suggested discount depths that protect margin
4. Timing recommendations for promotions
5. Marketing message ideas for each promotion

Be specific with numbers and product names from the data provided."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error generating recommendations: {str(e)}"
    
    def answer_business_question(self, question: str, context_data: dict) -> str:
        """
        Answer ad-hoc business questions using the data context.
        
        Args:
            question: User's business question
            context_data: Relevant data to inform the answer
        
        Returns:
            AI-generated answer
        """
        if not self.is_available():
            return f"Claude AI not configured: {self.init_error}"
        
        # Safely serialize the context data
        context_str = safe_json_dumps(context_data)
        
        prompt = f"""You are a retail analytics assistant for a cannabis dispensary.

Available data context:
{context_str}

User question: {question}

Please provide a helpful, data-informed answer. If the data doesn't fully answer 
the question, say so and explain what additional data would help.

Be specific and reference actual numbers from the data when possible."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error processing question: {str(e)}"


# =============================================================================
# STREAMLIT INTEGRATION HELPERS
# =============================================================================

def render_ai_analysis_section(claude: ClaudeAnalytics, data: dict):
    """
    Render the AI analysis section in Streamlit.
    
    Call this from your Streamlit app to add AI-powered insights.
    """
    import streamlit as st
    
    st.subheader("🤖 AI-Powered Analysis")
    
    if not claude.is_available():
        st.info(f"""
        **Enable AI Analysis**
        
        {claude.get_error() or "Claude AI not configured."}
        
        Add your Anthropic API key to unlock AI-powered insights:
        1. Get an API key at [console.anthropic.com](https://console.anthropic.com)
        2. Add to your `.streamlit/secrets.toml`:
        ```toml
        [anthropic]
        api_key = "your-api-key-here"
        ```
        """)
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Analyze Sales Trends"):
            with st.spinner("Analyzing..."):
                analysis = claude.analyze_sales_trends(data.get('sales_summary', {}))
                st.markdown(analysis)
    
    with col2:
        if st.button("🏷️ Brand Recommendations"):
            with st.spinner("Analyzing brands..."):
                analysis = claude.analyze_brand_performance(data.get('brand_data', []))
                st.markdown(analysis)
    
    # Q&A Section
    st.markdown("---")
    st.subheader("💬 Ask a Question")
    
    question = st.text_input("Ask anything about your business data:")
    if question:
        with st.spinner("Thinking..."):
            answer = claude.answer_business_question(question, data)
            st.markdown(answer)
