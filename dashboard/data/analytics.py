"""
Analytics engine for generating insights and recommendations.
"""

from typing import Dict, List
import pandas as pd

# Import store display names - use relative import within package
try:
    from ..core.config import STORE_DISPLAY_NAMES
except ImportError:
    STORE_DISPLAY_NAMES = {
        "barbary_coast": "Barbary Coast",
        "grass_roots": "Grass Roots"
    }


class AnalyticsEngine:
    """Generates insights and recommendations from data."""

    @staticmethod
    def calculate_store_metrics(df: pd.DataFrame) -> Dict:
        """Calculate key metrics by store."""
        metrics = {}

        for store_id in df['Store_ID'].unique():
            store_df = df[df['Store_ID'] == store_id]
            store_name = STORE_DISPLAY_NAMES.get(store_id, store_id)

            metrics[store_name] = {
                'total_net_sales': store_df['Net Sales'].sum(),
                'total_transactions': store_df['Tickets Count'].sum(),
                'total_customers': store_df['Customers Count'].sum(),
                'total_new_customers': store_df['New Customers'].sum(),
                'avg_order_value': store_df['Avg Order Value'].mean(),
                'avg_margin': store_df['Gross Margin %'].mean() * 100,
                'avg_discount_rate': store_df['Discount %'].mean() * 100,
                'units_sold': store_df['Units Sold'].sum(),
            }

        return metrics

    @staticmethod
    def identify_top_brands(
        df: pd.DataFrame,
        n: int = 10,
        store: str = None
    ) -> pd.DataFrame:
        """Identify top performing brands."""
        if store and store != 'All Stores':
            store_id = [k for k, v in STORE_DISPLAY_NAMES.items() if v == store]
            if store_id:
                if 'Store_ID' in df.columns:
                    df = df[df['Store_ID'] == store_id[0]]
                elif 'Upload_Store' in df.columns:
                    df = df[df['Upload_Store'] == store_id[0]]

        return df.nlargest(n, 'Net Sales')[
            ['Brand', 'Net Sales', 'Gross Margin %', '% of Total Net Sales']
        ]

    @staticmethod
    def identify_underperformers(
        df: pd.DataFrame,
        margin_threshold: float = 0.4
    ) -> pd.DataFrame:
        """Identify brands with low margins that might need attention."""
        low_margin = df[
            (df['Gross Margin %'] < margin_threshold) &
            (df['Net Sales'] > 1000)
        ].copy()

        return low_margin.nsmallest(10, 'Gross Margin %')[
            ['Brand', 'Net Sales', 'Gross Margin %']
        ]

    @staticmethod
    def generate_recommendations(
        store_metrics: Dict,
        brand_df: pd.DataFrame
    ) -> List[Dict]:
        """Generate actionable business recommendations."""
        recommendations = []

        # Compare store performance
        if len(store_metrics) == 2:
            stores = list(store_metrics.keys())
            s1, s2 = stores[0], stores[1]

            # AOV comparison
            aov_diff = abs(
                store_metrics[s1]['avg_order_value'] -
                store_metrics[s2]['avg_order_value']
            )
            if aov_diff > 5:
                higher = (
                    s1 if store_metrics[s1]['avg_order_value'] >
                    store_metrics[s2]['avg_order_value'] else s2
                )
                lower = s2 if higher == s1 else s1
                recommendations.append({
                    'type': 'opportunity',
                    'title': 'Average Order Value Gap',
                    'description': (
                        f"{higher} has ${aov_diff:.2f} higher AOV than {lower}. "
                        f"Consider cross-sell strategies at {lower}."
                    ),
                    'priority': 'medium'
                })

            # Margin comparison
            margin_diff = abs(
                store_metrics[s1]['avg_margin'] -
                store_metrics[s2]['avg_margin']
            )
            if margin_diff > 3:
                higher = (
                    s1 if store_metrics[s1]['avg_margin'] >
                    store_metrics[s2]['avg_margin'] else s2
                )
                lower = s2 if higher == s1 else s1
                recommendations.append({
                    'type': 'warning',
                    'title': 'Margin Disparity',
                    'description': (
                        f"{lower} margins are {margin_diff:.1f}% lower than {higher}. "
                        f"Review product mix and pricing."
                    ),
                    'priority': 'high'
                })

            # New customer comparison
            new_cust_rate_1 = (
                store_metrics[s1]['total_new_customers'] /
                max(store_metrics[s1]['total_customers'], 1)
            )
            new_cust_rate_2 = (
                store_metrics[s2]['total_new_customers'] /
                max(store_metrics[s2]['total_customers'], 1)
            )
            if abs(new_cust_rate_1 - new_cust_rate_2) > 0.05:
                higher = s1 if new_cust_rate_1 > new_cust_rate_2 else s2
                recommendations.append({
                    'type': 'insight',
                    'title': 'Customer Acquisition',
                    'description': (
                        f"{higher} is attracting more new customers proportionally. "
                        f"Analyze their local marketing."
                    ),
                    'priority': 'low'
                })

        # Brand recommendations
        if brand_df is not None and len(brand_df) > 0:
            high_margin_low_sales = brand_df[
                (brand_df['Gross Margin %'] > 0.65) &
                (brand_df['% of Total Net Sales'] < 0.01)
            ].head(5)

            if len(high_margin_low_sales) > 0:
                brands = ", ".join(high_margin_low_sales['Brand'].head(3).tolist())
                recommendations.append({
                    'type': 'opportunity',
                    'title': 'High-Margin Growth Potential',
                    'description': (
                        f"Consider promoting {brands} - they have strong margins "
                        f"(>65%) but low sales share."
                    ),
                    'priority': 'medium'
                })

        return recommendations

    @staticmethod
    def get_customer_summary(df: pd.DataFrame) -> Dict:
        """Generate customer analytics summary."""
        if df is None or df.empty:
            return {}

        summary = {
            'total_customers': len(df),
            'total_stores': df['Store_ID'].nunique() if 'Store_ID' in df.columns else 1,
        }

        # Segment distribution
        if 'Customer Segment' in df.columns:
            segment_counts = df['Customer Segment'].value_counts().to_dict()
            summary['segments'] = {str(k): v for k, v in segment_counts.items()}

        # Recency distribution
        if 'Recency Segment' in df.columns:
            recency_counts = df['Recency Segment'].value_counts().to_dict()
            summary['recency'] = {str(k): v for k, v in recency_counts.items()}

        # Lifetime value stats
        if 'Lifetime Net Sales' in df.columns:
            summary['ltv_stats'] = {
                'mean': float(df['Lifetime Net Sales'].mean()),
                'median': float(df['Lifetime Net Sales'].median()),
                'total': float(df['Lifetime Net Sales'].sum()),
            }

        # Age distribution
        if 'Age' in df.columns:
            age_bins = pd.cut(
                df['Age'].dropna(),
                bins=[0, 25, 35, 45, 55, 65, 100],
                labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
            )
            summary['age_distribution'] = age_bins.value_counts().to_dict()

        return summary
