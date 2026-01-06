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
    
    def analyze_customer_segments(self, customer_summary: dict, sales_data: dict = None) -> str:
        """
        Analyze customer segmentation and demographics data.

        Args:
            customer_summary: Dictionary containing customer segment data
            sales_data: Optional sales data for cross-referencing

        Returns:
            AI-generated customer insights and recommendations
        """
        if not self.is_available():
            return f"Claude AI not configured: {self.init_error}"

        # Safely serialize the data
        customer_str = safe_json_dumps(customer_summary)

        # Add sales context if provided
        sales_context = ""
        if sales_data:
            sales_str = safe_json_dumps(sales_data)
            sales_context = f"\n\nRelated sales performance data:\n{sales_str}\n"

        prompt = f"""You are a customer analytics expert for a cannabis dispensary operation.

Analyze the following customer segmentation and demographic data:

{customer_str}{sales_context}

Please provide:
1. Key insights about customer segments (VIP, Whale, Regular, etc.)
2. Demographic patterns and their business implications
3. Customer retention risks and opportunities (based on recency segments)
4. Specific recommendations for:
   - High-value customer retention strategies
   - Re-engagement campaigns for at-risk customers
   - Growth opportunities in underserved segments
5. Suggested personalization or targeting strategies based on customer groups

Focus on actionable recommendations that can improve customer lifetime value and retention.
Use specific numbers from the data when possible."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing customer data: {str(e)}"

    def generate_integrated_insights(self, sales_data: dict, customer_data: dict, brand_data: list = None) -> str:
        """
        Generate integrated insights combining sales, customer, and brand data.

        Args:
            sales_data: Sales performance summary
            customer_data: Customer segmentation and demographic data
            brand_data: Optional brand performance data

        Returns:
            AI-generated comprehensive business insights
        """
        if not self.is_available():
            return f"Claude AI not configured: {self.init_error}"

        # Safely serialize all data
        sales_str = safe_json_dumps(sales_data)
        customer_str = safe_json_dumps(customer_data)

        brand_context = ""
        if brand_data:
            limited_brands = brand_data[:30] if len(brand_data) > 30 else brand_data
            brand_str = safe_json_dumps(limited_brands)
            brand_context = f"\n\nBrand Performance:\n{brand_str}\n"

        prompt = f"""You are a comprehensive retail analytics consultant for a cannabis dispensary.

Analyze the following integrated business data:

Sales Performance:
{sales_str}

Customer Analytics:
{customer_str}{brand_context}

Please provide:
1. Cross-functional insights linking customer behavior to sales performance
2. Which customer segments are driving the most value and how to grow them
3. Product/brand preferences by customer segment (if brand data available)
4. Opportunities to increase AOV or frequency in specific customer groups
5. Strategic recommendations that leverage both customer and sales insights
6. Specific action items prioritized by potential impact

Focus on insights that emerge from analyzing these data sources together, not separately.
Be specific and data-driven with your recommendations."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error generating integrated insights: {str(e)}"

    def answer_business_question(self, question: str, context_data: dict, use_deep_thinking: bool = True) -> str:
        """
        Answer ad-hoc business questions using the data context.
        Uses Claude Opus with extended thinking for deeper analysis.

        Args:
            question: User's business question
            context_data: Relevant data to inform the answer
            use_deep_thinking: If True, use Opus with extended thinking for deeper analysis

        Returns:
            AI-generated answer
        """
        if not self.is_available():
            return f"Claude AI not configured: {self.init_error}"

        # Safely serialize the context data
        context_str = safe_json_dumps(context_data)

        # Build a comprehensive data description
        data_description = []
        if context_data.get('sales_summary'):
            data_description.append("- **Sales Data**: Store metrics, transaction records, revenue, and performance trends")
        if context_data.get('top_brands'):
            data_description.append("- **Brand Data**: Brand performance, net sales, and gross margins")
        if context_data.get('product_mix'):
            data_description.append("- **Product Mix**: Sales breakdown by product category (Flower, Preroll, Cartridge, etc.)")
        if context_data.get('customer_summary'):
            data_description.append("- **Customer Data**: Customer segments, demographics, lifetime value, and recency")
        if context_data.get('invoice_summary'):
            data_description.append("- **Invoice Data**: Vendor invoices, total spend, and supplier relationships from DynamoDB")
        if context_data.get('purchase_data'):
            data_description.append("- **Purchase Data**: Line-item purchasing details, wholesale costs by brand and product type")
        if context_data.get('research_findings'):
            data_description.append("- **Industry Research**: Cannabis industry trends, regulatory updates, market analysis, and competitive insights from research documents")
        if context_data.get('seo_analysis'):
            data_description.append("- **SEO Analysis**: Website SEO scores, technical issues, content recommendations, and local SEO performance for store websites")
        if context_data.get('selected_research_documents'):
            doc_names = list(context_data['selected_research_documents'].keys())
            data_description.append(f"- **Selected Research Documents**: Full content from {len(doc_names)} specific document(s): {', '.join(doc_names)}")

        data_sources_text = "\n".join(data_description) if data_description else "Limited data available"

        prompt = f"""You are a comprehensive retail analytics assistant for a cannabis dispensary operation with two stores (Barbary Coast and Grass Roots) in San Francisco.

**DATA SOURCES AVAILABLE FOR ANALYSIS:**
{data_sources_text}

**COMPLETE DATA CONTEXT:**
{context_str}

**USER QUESTION:** {question}

**INSTRUCTIONS:**
1. Analyze ALL relevant data sources to provide a comprehensive answer
2. Cross-reference data when possible (e.g., compare sales performance to purchasing costs, relate industry trends to business strategy)
3. Be specific - use actual numbers, brand names, vendor names, and percentages from the data
4. If the question relates to:
   - Purchasing/vendors/invoices: Focus on invoice_summary and purchase_data
   - Sales/revenue: Focus on sales_summary and brand data
   - Customers: Focus on customer_summary
   - Product strategy: Combine product_mix, brand data, and purchase data
   - Industry trends/regulations: Focus on research_findings and selected_research_documents
   - Website/marketing/online presence: Focus on seo_analysis
   - Strategic planning: Synthesize across ALL available data sources
5. If selected_research_documents are provided, prioritize those documents for answering questions about their content - quote relevant passages and provide specific insights from the document text
6. If data is missing or incomplete for a full answer, clearly state what additional information would help
7. Provide actionable insights and specific recommendations when appropriate
8. When discussing SEO or research insights, relate them back to business impact and recommended actions
9. Think deeply about the interconnections between data sources and provide strategic insights that go beyond surface-level observations

Respond with a clear, data-driven answer that demonstrates you have access to and understand ALL the available business data."""

        try:
            if use_deep_thinking:
                # Use Claude Opus with extended thinking for deeper analysis
                response = self.client.messages.create(
                    model="claude-opus-4-20250514",
                    max_tokens=16000,
                    thinking={
                        "type": "enabled",
                        "budget_tokens": 10000  # Allow up to 10k tokens for thinking
                    },
                    messages=[{"role": "user", "content": prompt}]
                )

                # Extract the text response (skip thinking blocks)
                result_text = ""
                for block in response.content:
                    if block.type == "text":
                        result_text += block.text

                return result_text if result_text else "No response generated."
            else:
                # Standard mode without extended thinking
                message = self.client.messages.create(
                    model=self.model,
                    max_tokens=4000,
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

        **Option 1: Environment Variable (Recommended)**
        ```bash
        export ANTHROPIC_API_KEY="sk-ant-api03-..."
        ```

        **Option 2: Streamlit Secrets**
        Add to `.streamlit/secrets.toml`:
        ```toml
        ANTHROPIC_API_KEY = "sk-ant-api03-..."
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
