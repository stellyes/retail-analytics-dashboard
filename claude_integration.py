"""
Claude AI Integration Module
Optional module for AI-powered analytics and recommendations using Anthropic's Claude API.
"""

import os
from typing import Optional
import json

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


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
        
        if ANTHROPIC_AVAILABLE:
            key = api_key or os.environ.get("ANTHROPIC_API_KEY")
            if key:
                self.client = anthropic.Anthropic(api_key=key)
    
    def is_available(self) -> bool:
        """Check if Claude API is available and configured."""
        return self.client is not None
    
    def analyze_sales_trends(self, sales_summary: dict) -> str:
        """
        Analyze sales data and provide insights.
        
        Args:
            sales_summary: Dictionary containing aggregated sales metrics
        
        Returns:
            AI-generated analysis and recommendations
        """
        if not self.is_available():
            return "Claude AI not configured. Add ANTHROPIC_API_KEY to enable."
        
        prompt = f"""You are a retail analytics expert for a cannabis dispensary operation.
        
Analyze the following sales data and provide actionable insights:

{json.dumps(sales_summary, indent=2)}

Please provide:
1. Key observations about performance trends
2. Comparison between stores (if multiple stores present)
3. Areas of concern that need attention
4. Specific recommendations for improving sales and margins
5. Suggested promotional strategies based on the data

Keep your response concise and actionable."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing data: {str(e)}"
    
    def analyze_brand_performance(self, brand_data: list) -> str:
        """
        Analyze brand performance and suggest inventory decisions.
        
        Args:
            brand_data: List of brand performance dictionaries
        
        Returns:
            AI-generated brand recommendations
        """
        if not self.is_available():
            return "Claude AI not configured. Add ANTHROPIC_API_KEY to enable."
        
        prompt = f"""You are a cannabis retail buyer and merchandising expert.

Analyze the following brand performance data:

{json.dumps(brand_data[:50], indent=2)}  # Limit to top 50 brands

Please provide:
1. Which brands should we increase orders for and why?
2. Which brands should we consider discontinuing or reducing?
3. Any brands with unusual margin profiles that need investigation?
4. Recommendations for brand mix optimization
5. Suggested promotional candidates based on margin and velocity

Focus on actionable buying recommendations."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing brands: {str(e)}"
    
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
            return "Claude AI not configured. Add ANTHROPIC_API_KEY to enable."
        
        context = seasonal_context or "Consider current market conditions"
        
        prompt = f"""You are a retail promotions strategist for cannabis dispensaries.

Context: {context}

Slow-moving inventory that needs movement:
{json.dumps(slow_movers[:20], indent=2)}

High-margin products available for deals:
{json.dumps(high_margin_items[:20], indent=2)}

Please recommend:
1. Specific deals/promotions to move slow inventory (e.g., BOGO, % off, bundles)
2. Bundle suggestions pairing slow movers with popular items
3. Suggested discount depths that protect margin
4. Timing recommendations for promotions
5. Marketing message ideas for each promotion

Be specific with numbers and products."""

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
            return "Claude AI not configured. Add ANTHROPIC_API_KEY to enable."
        
        prompt = f"""You are a retail analytics assistant for a cannabis dispensary.

Available data context:
{json.dumps(context_data, indent=2)}

User question: {question}

Please provide a helpful, data-informed answer. If the data doesn't fully answer 
the question, say so and explain what additional data would help."""

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
        st.info("""
        **Enable AI Analysis**
        
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
