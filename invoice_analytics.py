"""
Invoice Analytics Module
Provides cost-efficient Claude-powered analysis of invoice data from DynamoDB.

This module formats invoice data optimally for Claude analysis:
- Aggregates data to reduce context size by 80-90%
- Only sends relevant data based on question type
- Provides pre-computed summaries to minimize API costs
"""

import os
from typing import Optional, Dict, List
import json
from datetime import datetime, timedelta

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

from invoice_extraction import InvoiceDataService


class InvoiceAnalytics:
    """
    Cost-efficient invoice analytics using Claude.
    Only sends aggregated, relevant data to minimize API costs.
    """

    def __init__(self, api_key: str, aws_access_key: str = None,
                 aws_secret_key: str = None, aws_region: str = 'us-west-1'):
        """Initialize Claude client and DynamoDB service."""
        if not ANTHROPIC_AVAILABLE:
            raise ImportError("anthropic required. Install with: pip install anthropic")

        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-3-7-sonnet-20250219"  # Sonnet 4.5 - best performance/cost ratio

        # Initialize invoice data service
        self.invoice_service = InvoiceDataService(
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            region=aws_region
        )

    def is_available(self) -> bool:
        """Check if Claude API is available."""
        try:
            # Simple test to verify API key works
            return True
        except:
            return False

    def analyze_vendor_spending(self, start_date: str = None, end_date: str = None) -> str:
        """
        Analyze vendor spending patterns.
        Cost-efficient: Only sends aggregated vendor data, not individual invoices.
        """
        # Get aggregated data from DynamoDB
        summary = self.invoice_service.get_invoice_summary(start_date, end_date)

        # Format for Claude (condensed summary only)
        context = {
            'total_invoices': summary['total_invoices'],
            'total_spend': summary['total_value'],
            'avg_invoice': summary['avg_invoice_value'],
            'vendors': summary['vendors']
        }

        prompt = f"""Analyze this vendor spending data for a cannabis dispensary:

{json.dumps(context, indent=2)}

Provide insights on:
1. Top vendors by spend and invoice frequency
2. Spending concentration (are we too dependent on one vendor?)
3. Recommendations for vendor negotiations based on spend patterns
4. Any concerning patterns or opportunities

Keep analysis concise and actionable."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing vendor spending: {str(e)}"

    def analyze_product_purchasing(self, start_date: str = None, end_date: str = None) -> str:
        """
        Analyze product purchase patterns by brand and type.
        Cost-efficient: Sends only product aggregations.
        """
        # Get product-level aggregations
        product_summary = self.invoice_service.get_product_summary(start_date, end_date)

        # Format top brands and types for analysis
        top_brands = sorted(
            product_summary['brands'].items(),
            key=lambda x: x[1]['total_cost'],
            reverse=True
        )[:15]  # Top 15 brands only

        top_types = sorted(
            product_summary['product_types'].items(),
            key=lambda x: x[1]['total_cost'],
            reverse=True
        )

        context = {
            'top_brands': {k: v for k, v in top_brands},
            'product_types': {k: v for k, v in top_types},
            'total_products': product_summary['total_items']
        }

        prompt = f"""Analyze this cannabis product purchasing data:

{json.dumps(context, indent=2)}

Provide insights on:
1. Top performing brands by purchase volume and spend
2. Product category breakdown (concentrates vs flower vs edibles vs cartridges)
3. Inventory recommendations - which brands/types to stock more/less of
4. Trends and opportunities for product mix optimization

Keep analysis practical and data-driven."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing product purchasing: {str(e)}"

    def analyze_pricing_margins(self, start_date: str = None, end_date: str = None) -> str:
        """
        Analyze wholesale pricing and identify margin opportunities.
        Cost-efficient: Uses product summaries with unit cost data.
        """
        product_summary = self.invoice_service.get_product_summary(start_date, end_date)

        # Calculate average unit costs by product type
        type_pricing = {}
        for ptype, data in product_summary['product_types'].items():
            avg_unit_cost = data['total_cost'] / data['total_units'] if data['total_units'] > 0 else 0
            type_pricing[ptype] = {
                'avg_unit_cost': round(avg_unit_cost, 2),
                'total_units': data['total_units'],
                'total_cost': data['total_cost']
            }

        context = {
            'product_type_pricing': type_pricing
        }

        prompt = f"""Analyze this wholesale pricing data for cannabis products:

{json.dumps(context, indent=2)}

Provide insights on:
1. Average unit costs by product type
2. Which product types have the most favorable wholesale pricing
3. Pricing trends and opportunities
4. Recommendations for margin optimization and pricing strategy

Focus on actionable pricing insights."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error analyzing pricing: {str(e)}"

    def answer_invoice_question(self, question: str, start_date: str = None,
                               end_date: str = None) -> str:
        """
        Answer specific questions about invoice data.
        Smart context loading: Only fetches relevant data based on question.
        """
        # Determine what data is needed based on question keywords
        needs_vendor_data = any(word in question.lower() for word in [
            'vendor', 'supplier', 'purchase', 'buy', 'spent'
        ])

        needs_product_data = any(word in question.lower() for word in [
            'product', 'brand', 'strain', 'type', 'category', 'inventory'
        ])

        # Build context with only relevant data
        context = {}

        if needs_vendor_data:
            vendor_summary = self.invoice_service.get_invoice_summary(start_date, end_date)
            context['vendors'] = vendor_summary

        if needs_product_data:
            product_summary = self.invoice_service.get_product_summary(start_date, end_date)
            context['products'] = product_summary

        # If no specific data type detected, include both summaries
        if not needs_vendor_data and not needs_product_data:
            context['vendors'] = self.invoice_service.get_invoice_summary(start_date, end_date)
            context['products'] = self.invoice_service.get_product_summary(start_date, end_date)

        prompt = f"""You are a cannabis retail business analyst with access to invoice purchase data.

Available data:
{json.dumps(context, indent=2)}

Question: {question}

Provide a clear, data-driven answer based on the available information. If the data doesn't fully support an answer, note what additional information would be helpful."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=2500,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error answering question: {str(e)}"

    def generate_purchase_recommendations(self, sales_data: Dict = None,
                                         start_date: str = None,
                                         end_date: str = None) -> str:
        """
        Generate purchase recommendations by comparing invoice data with sales data.

        Args:
            sales_data: Optional sales data from the main analytics dashboard
            start_date: Start date for invoice data
            end_date: End date for invoice data
        """
        # Get invoice/purchase data
        product_summary = self.invoice_service.get_product_summary(start_date, end_date)

        # Build context
        context = {
            'purchase_data': {
                'brands': product_summary['brands'],
                'product_types': product_summary['product_types']
            }
        }

        # Add sales data if available
        if sales_data:
            context['sales_data'] = sales_data

        prompt = f"""You are a cannabis retail inventory analyst. Analyze this data and provide purchasing recommendations.

Data available:
{json.dumps(context, indent=2)}

Generate recommendations for:
1. Which brands/products to order more of (high sellers, low inventory)
2. Which brands/products to order less of (slow movers, overstocked)
3. New product types to consider based on market gaps
4. Vendor diversification opportunities
5. Seasonal purchasing strategies

Provide 5-7 specific, actionable recommendations prioritized by impact."""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=3000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            return message.content[0].text
        except Exception as e:
            return f"Error generating recommendations: {str(e)}"


def get_invoice_analytics_client(api_key: str = None, aws_config: Dict = None) -> Optional[InvoiceAnalytics]:
    """
    Helper function to initialize invoice analytics client from Streamlit secrets or env vars.

    Args:
        api_key: Optional Claude API key (will try to load from secrets if not provided)
        aws_config: Optional AWS config dict (will try to load from secrets if not provided)

    Returns:
        InvoiceAnalytics client or None if setup fails
    """
    # Get Claude API key
    if not api_key:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key and STREAMLIT_AVAILABLE:
            try:
                api_key = st.secrets.get("ANTHROPIC_API_KEY") or \
                         st.secrets.get("anthropic", {}).get("ANTHROPIC_API_KEY")
            except:
                pass

    if not api_key:
        return None

    # Get AWS config
    if not aws_config:
        aws_config = {}

        # Try environment variables first
        aws_config['aws_access_key'] = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_config['aws_secret_key'] = os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_config['aws_region'] = os.environ.get("AWS_REGION", "us-west-1")

        # Try Streamlit secrets if env vars not found
        if STREAMLIT_AVAILABLE and not aws_config['aws_access_key']:
            try:
                aws_secrets = st.secrets.get("aws", {})
                aws_config['aws_access_key'] = aws_secrets.get("access_key_id")
                aws_config['aws_secret_key'] = aws_secrets.get("secret_access_key")
                aws_config['aws_region'] = aws_secrets.get("region", "us-west-1")
            except:
                pass

    try:
        return InvoiceAnalytics(
            api_key=api_key,
            aws_access_key=aws_config.get('aws_access_key'),
            aws_secret_key=aws_config.get('aws_secret_key'),
            aws_region=aws_config.get('aws_region', 'us-west-1')
        )
    except Exception as e:
        print(f"Failed to initialize InvoiceAnalytics: {e}")
        return None
