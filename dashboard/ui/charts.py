"""
Visualization components for the Retail Analytics Dashboard.
"""

from typing import Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Import store display names - use relative import within package
try:
    from ..core.config import STORE_DISPLAY_NAMES
except ImportError:
    STORE_DISPLAY_NAMES = {
        "barbary_coast": "Barbary Coast",
        "grass_roots": "Grass Roots"
    }


def plot_sales_trend(df: pd.DataFrame, store_filter: str = "All Stores") -> go.Figure:
    """
    Create sales trend visualization.

    Args:
        df: Sales DataFrame with Date, Net Sales, Store_ID columns
        store_filter: Store to filter by, or "All Stores"

    Returns:
        Plotly figure with sales and transaction trends
    """
    if store_filter != "All Stores":
        store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store_filter]
        if store_id:
            df = df[df['Store_ID'] == store_id[0]]

    # Filter out invalid data points
    df = df[
        (df['Net Sales'].notna()) &
        (df['Net Sales'] > 100) &
        (df['Tickets Count'].notna()) &
        (df['Tickets Count'] > 10)
    ].copy()

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        subplot_titles=('Net Sales by Day', 'Transaction Count'),
        vertical_spacing=0.1
    )

    for store_id in df['Store_ID'].unique():
        store_df = df[df['Store_ID'] == store_id]
        store_name = STORE_DISPLAY_NAMES.get(store_id, store_id)

        fig.add_trace(
            go.Scatter(
                x=store_df['Date'],
                y=store_df['Net Sales'],
                name=f'{store_name} Sales',
                mode='lines+markers'
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=store_df['Date'],
                y=store_df['Tickets Count'],
                name=f'{store_name} Transactions',
                mode='lines+markers'
            ),
            row=2, col=1
        )

    fig.update_layout(height=500, showlegend=True)
    return fig


def plot_category_breakdown(df: pd.DataFrame) -> go.Figure:
    """
    Create product category breakdown chart.

    Args:
        df: Product DataFrame with Net Sales and Product Type columns

    Returns:
        Plotly pie chart figure
    """
    fig = px.pie(
        df,
        values='Net Sales',
        names='Product Type',
        title='Sales by Product Category',
        hole=0.4
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def plot_brand_performance(df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    """
    Create brand performance visualization.

    Args:
        df: Brand DataFrame with Net Sales, Gross Margin %, Brand columns
        top_n: Number of top brands to display

    Returns:
        Plotly figure with bar chart and margin overlay
    """
    # Filter out invalid data
    df = df[
        (df['Net Sales'].notna()) &
        (df['Net Sales'] > 0) &
        (df['Gross Margin %'].notna())
    ]
    top_brands = df.nlargest(top_n, 'Net Sales').copy()

    # Handle margin percentage format
    max_margin = top_brands['Gross Margin %'].max() if len(top_brands) > 0 else 0
    if max_margin <= 1:
        top_brands['Margin_Pct'] = top_brands['Gross Margin %'] * 100
    else:
        top_brands['Margin_Pct'] = top_brands['Gross Margin %']

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Bar chart for Net Sales
    fig.add_trace(
        go.Bar(
            x=top_brands['Brand'],
            y=top_brands['Net Sales'],
            name='Net Sales',
            marker_color='steelblue',
            hovertemplate='<b>%{x}</b><br>Net Sales: $%{y:,.0f}<extra></extra>'
        ),
        secondary_y=False
    )

    # Scatter plot for Gross Margin
    fig.add_trace(
        go.Scatter(
            x=top_brands['Brand'],
            y=top_brands['Margin_Pct'],
            name='Gross Margin %',
            mode='markers',
            marker=dict(
                color='coral',
                size=12,
                symbol='diamond',
                line=dict(width=1, color='white')
            ),
            hovertemplate='<b>%{x}</b><br>Margin: %{y:.1f}%<extra></extra>'
        ),
        secondary_y=True
    )

    # Add reference line for target margin
    fig.add_hline(
        y=55,
        line_dash="dash",
        line_color="rgba(255,255,255,0.3)",
        secondary_y=True,
        annotation_text="55% Target",
        annotation_position="right"
    )

    fig.update_layout(
        title=f'Top {top_n} Brands by Net Sales with Margin Overlay',
        xaxis_tickangle=-45,
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode='x unified'
    )
    fig.update_yaxes(title_text="Net Sales ($)", secondary_y=False)
    fig.update_yaxes(title_text="Gross Margin (%)", range=[40, 90], secondary_y=True)

    return fig


def plot_store_comparison(metrics: Dict) -> go.Figure:
    """
    Create store comparison dashboard.

    Args:
        metrics: Dictionary of store metrics from AnalyticsEngine.calculate_store_metrics

    Returns:
        Plotly radar chart comparing stores
    """
    stores = list(metrics.keys())

    comparison_data = {
        'Metric': ['Net Sales', 'Transactions', 'Avg Order Value', 'Gross Margin %', 'Units Sold'],
    }

    for store in stores:
        comparison_data[store] = [
            metrics[store]['total_net_sales'],
            metrics[store]['total_transactions'],
            metrics[store]['avg_order_value'],
            metrics[store]['avg_margin'],
            metrics[store]['units_sold']
        ]

    fig = go.Figure()

    # Normalized comparison
    for store in stores:
        values = comparison_data[store]
        max_vals = [
            max(comparison_data[s][i] for s in stores)
            for i in range(len(values))
        ]
        normalized = [
            v / m * 100 if m > 0 else 0
            for v, m in zip(values, max_vals)
        ]
        normalized.append(normalized[0])  # Close the polygon

        categories = comparison_data['Metric'] + [comparison_data['Metric'][0]]

        fig.add_trace(go.Scatterpolar(
            r=normalized,
            theta=categories,
            fill='toself',
            name=store
        ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        title='Store Performance Comparison (Normalized)'
    )

    return fig


def plot_margin_vs_sales(df: pd.DataFrame, title: str = "Margin vs. Sales Analysis") -> go.Figure:
    """
    Create margin vs sales scatter plot for brand positioning analysis.

    Args:
        df: Brand DataFrame with Net Sales, Gross Margin %, Brand columns
        title: Chart title

    Returns:
        Plotly scatter chart
    """
    # Filter to significant brands with valid margin data
    significant_brands = df[
        (df['Net Sales'] > 1000) &
        (df['Gross Margin %'].notna()) &
        (df['Gross Margin %'] > 0)
    ].copy()

    if len(significant_brands) == 0:
        fig = go.Figure()
        fig.add_annotation(text="No data available", x=0.5, y=0.5, showarrow=False)
        return fig

    # Handle margin percentage format
    max_margin = significant_brands['Gross Margin %'].max()
    if max_margin <= 1:
        significant_brands['Margin_Pct'] = significant_brands['Gross Margin %'] * 100
    else:
        significant_brands['Margin_Pct'] = significant_brands['Gross Margin %']

    fig = px.scatter(
        significant_brands,
        x='Net Sales',
        y='Margin_Pct',
        hover_name='Brand',
        color='Margin_Pct',
        color_continuous_scale='RdYlGn',
        size='Net Sales',
        size_max=30,
        title=title,
        log_x=True,
        labels={'Margin_Pct': 'Gross Margin %', 'Net Sales': 'Net Sales ($)'}
    )

    # Add quadrant line
    fig.add_hline(
        y=55,
        line_dash="dash",
        line_color="rgba(255,255,255,0.5)",
        annotation_text="55% Target Margin",
        annotation_position="right"
    )

    fig.update_layout(
        height=500,
        coloraxis_colorbar=dict(title="Margin %")
    )

    return fig
