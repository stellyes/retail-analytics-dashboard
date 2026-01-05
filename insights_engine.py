"""
Insights Engine - AI-Free Rule-Based Analytics
Generates comprehensive business insights without API costs using predefined rules.

Features:
- Rule-based analysis across all data sources
- S3-based caching to prevent repeated analysis
- Configurable thresholds and alert levels
- Incremental updates (only analyzes new data)
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False


class InsightPriority(Enum):
    """Priority levels for insights."""
    CRITICAL = "critical"  # Requires immediate attention
    HIGH = "high"          # Should address within a week
    MEDIUM = "medium"      # Address when convenient
    LOW = "low"            # Nice to know


class InsightCategory(Enum):
    """Categories of insights."""
    SALES = "sales"
    INVENTORY = "inventory"
    CUSTOMERS = "customers"
    PURCHASING = "purchasing"
    MARKETING = "marketing"
    OPERATIONS = "operations"
    COMPLIANCE = "compliance"


@dataclass
class Insight:
    """Represents a single business insight."""
    id: str
    title: str
    description: str
    priority: str
    category: str
    data_source: str
    metric_value: Any
    threshold: Any
    recommendation: str
    created_at: str
    expires_at: str = None

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'Insight':
        return cls(**data)


class InsightsCache:
    """S3-based caching for insights to prevent repeated analysis."""

    def __init__(self, bucket_name: str = None, aws_access_key: str = None,
                 aws_secret_key: str = None, region: str = 'us-west-1'):
        self.bucket_name = bucket_name or 'retail-analytics-dashboard'
        self.prefix = 'insights-cache/'
        self.s3_client = None

        if BOTO3_AVAILABLE:
            try:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=region
                )
            except Exception as e:
                print(f"Failed to initialize S3 client: {e}")

    def _get_cache_key(self, data_hash: str, rule_version: str) -> str:
        """Generate cache key from data hash and rule version."""
        return f"{self.prefix}{data_hash}_{rule_version}.json"

    def _compute_data_hash(self, data: Dict) -> str:
        """Compute hash of data to detect changes."""
        # Sort keys for consistent hashing
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()[:12]

    def get_cached_insights(self, data: Dict, rule_version: str = "v1") -> Optional[List[Dict]]:
        """Retrieve cached insights if data hasn't changed."""
        if not self.s3_client:
            return None

        try:
            data_hash = self._compute_data_hash(data)
            cache_key = self._get_cache_key(data_hash, rule_version)

            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=cache_key
            )
            cached = json.loads(response['Body'].read().decode('utf-8'))

            # Check if cache is still valid (24 hour TTL)
            cached_time = datetime.fromisoformat(cached.get('cached_at', '2000-01-01'))
            if datetime.now() - cached_time < timedelta(hours=24):
                return cached.get('insights', [])

            return None
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['NoSuchKey', 'NoSuchBucket', 'AccessDenied']:
                return None
            # For other errors, silently fail and regenerate
            return None
        except Exception:
            return None

    def save_insights(self, data: Dict, insights: List[Dict], rule_version: str = "v1") -> bool:
        """Save insights to cache."""
        if not self.s3_client:
            return False

        try:
            data_hash = self._compute_data_hash(data)
            cache_key = self._get_cache_key(data_hash, rule_version)

            cache_data = {
                'cached_at': datetime.now().isoformat(),
                'data_hash': data_hash,
                'rule_version': rule_version,
                'insights': insights
            }

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=cache_key,
                Body=json.dumps(cache_data, default=str),
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            # Silently fail for bucket/access issues
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ['NoSuchBucket', 'AccessDenied']:
                return False
            return False
        except Exception:
            # Silently fail - caching is optional
            return False

    def get_insights_history(self, limit: int = 10) -> List[Dict]:
        """Get recent insights from cache for historical view."""
        if not self.s3_client:
            return []

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix,
                MaxKeys=limit * 2  # Get more to filter
            )

            history = []
            for obj in response.get('Contents', []):
                try:
                    obj_response = self.s3_client.get_object(
                        Bucket=self.bucket_name,
                        Key=obj['Key']
                    )
                    data = json.loads(obj_response['Body'].read().decode('utf-8'))
                    history.append({
                        'date': data.get('cached_at'),
                        'insights_count': len(data.get('insights', [])),
                        'insights': data.get('insights', [])[:5]  # Preview only
                    })
                except:
                    continue

            # Sort by date descending
            history.sort(key=lambda x: x['date'], reverse=True)
            return history[:limit]
        except Exception:
            return []


class InsightsEngine:
    """
    Rule-based insights engine that analyzes all data sources
    without using AI to minimize costs.
    """

    RULE_VERSION = "v2"  # Increment when rules change significantly

    # Configurable thresholds
    THRESHOLDS = {
        # Sales thresholds
        'sales_decline_pct': -10,           # Alert if sales drop > 10%
        'sales_growth_pct': 15,             # Highlight if sales grow > 15%
        'aov_gap_pct': 15,                  # Alert if AOV gap between stores > 15%
        'margin_concern_pct': 30,           # Alert if margin drops below 30%

        # Customer thresholds
        'customer_churn_pct': 20,           # Alert if churn rate > 20%
        'new_customer_decline_pct': -15,    # Alert if new customer acquisition drops
        'customer_concentration_pct': 30,   # Alert if top 10% drive > 30% revenue

        # Inventory/Brand thresholds
        'brand_concentration_pct': 25,      # Alert if single brand > 25% of sales
        'underperforming_margin_pct': 20,   # Flag brands with margin < 20%
        'high_margin_low_sales_pct': 40,    # Opportunity: margin > 40% but low volume

        # Purchasing thresholds
        'vendor_concentration_pct': 40,     # Alert if single vendor > 40% of spend
        'spend_increase_pct': 20,           # Alert if spend increases > 20%
        'price_variance_pct': 15,           # Alert if unit cost variance > 15%

        # SEO thresholds
        'seo_score_critical': 50,           # Critical if score < 50
        'seo_score_warning': 70,            # Warning if score < 70
        'seo_improvement_target': 80,       # Target score to aim for
    }

    def __init__(self, aws_access_key: str = None, aws_secret_key: str = None,
                 region: str = 'us-west-1'):
        """Initialize insights engine with optional AWS credentials for caching."""
        self.cache = InsightsCache(
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            region=region
        )
        self.insights: List[Insight] = []

    def generate_all_insights(self, data: Dict) -> List[Dict]:
        """
        Generate insights from all available data sources.
        Uses caching to avoid re-analyzing unchanged data.

        Args:
            data: Dictionary containing all data sources:
                - sales_data: Sales metrics and trends
                - brand_data: Brand performance data
                - customer_data: Customer metrics
                - invoice_data: Invoice/purchasing summary
                - product_data: Product purchase summary
                - research_data: Research findings
                - seo_data: SEO analysis data

        Returns:
            List of insight dictionaries
        """
        # Check cache first
        cached = self.cache.get_cached_insights(data, self.RULE_VERSION)
        if cached:
            return cached

        self.insights = []

        # Run all rule-based analyzers
        if data.get('sales_data'):
            self._analyze_sales(data['sales_data'])

        if data.get('brand_data'):
            self._analyze_brands(data['brand_data'])

        if data.get('customer_data'):
            self._analyze_customers(data['customer_data'])

        if data.get('invoice_data'):
            self._analyze_invoices(data['invoice_data'])

        if data.get('product_data'):
            self._analyze_products(data['product_data'])

        if data.get('research_data'):
            self._analyze_research(data['research_data'])

        if data.get('seo_data'):
            self._analyze_seo(data['seo_data'])

        # Cross-data analysis
        self._analyze_cross_data(data)

        # Sort by priority
        priority_order = {
            InsightPriority.CRITICAL.value: 0,
            InsightPriority.HIGH.value: 1,
            InsightPriority.MEDIUM.value: 2,
            InsightPriority.LOW.value: 3
        }
        self.insights.sort(key=lambda x: priority_order.get(x.priority, 99))

        # Convert to dicts and cache
        insight_dicts = [i.to_dict() for i in self.insights]
        self.cache.save_insights(data, insight_dicts, self.RULE_VERSION)

        return insight_dicts

    def _create_insight(self, title: str, description: str, priority: InsightPriority,
                       category: InsightCategory, data_source: str, metric_value: Any,
                       threshold: Any, recommendation: str, ttl_hours: int = 24) -> None:
        """Helper to create and add an insight."""
        insight_id = hashlib.md5(f"{title}{metric_value}".encode()).hexdigest()[:8]

        insight = Insight(
            id=insight_id,
            title=title,
            description=description,
            priority=priority.value,
            category=category.value,
            data_source=data_source,
            metric_value=metric_value,
            threshold=threshold,
            recommendation=recommendation,
            created_at=datetime.now().isoformat(),
            expires_at=(datetime.now() + timedelta(hours=ttl_hours)).isoformat()
        )
        self.insights.append(insight)

    # =========================================================================
    # SALES ANALYSIS RULES
    # =========================================================================

    def _analyze_sales(self, sales_data: Dict) -> None:
        """Analyze sales data for insights."""

        # Rule 1: Store performance comparison
        if 'store_comparison' in sales_data:
            stores = sales_data['store_comparison']
            if len(stores) >= 2:
                store_sales = [(name, data.get('total_sales', 0)) for name, data in stores.items()]
                store_sales.sort(key=lambda x: x[1], reverse=True)

                if len(store_sales) >= 2 and store_sales[1][1] > 0:
                    gap_pct = ((store_sales[0][1] - store_sales[1][1]) / store_sales[1][1]) * 100

                    if gap_pct > 30:
                        self._create_insight(
                            title=f"Significant sales gap between stores",
                            description=f"{store_sales[0][0]} outperforms {store_sales[1][0]} by {gap_pct:.0f}%. "
                                       f"({store_sales[0][0]}: ${store_sales[0][1]:,.0f} vs {store_sales[1][0]}: ${store_sales[1][1]:,.0f})",
                            priority=InsightPriority.HIGH,
                            category=InsightCategory.SALES,
                            data_source="sales",
                            metric_value=gap_pct,
                            threshold=30,
                            recommendation=f"Investigate what's working at {store_sales[0][0]} (staffing, inventory, location factors) "
                                          f"and apply learnings to improve {store_sales[1][0]} performance."
                        )

        # Rule 2: Sales trend analysis
        if 'trend' in sales_data:
            trend = sales_data['trend']
            if trend.get('period_over_period_change'):
                change_pct = trend['period_over_period_change']

                if change_pct < self.THRESHOLDS['sales_decline_pct']:
                    self._create_insight(
                        title="Sales declining significantly",
                        description=f"Sales have dropped {abs(change_pct):.1f}% compared to the previous period.",
                        priority=InsightPriority.CRITICAL,
                        category=InsightCategory.SALES,
                        data_source="sales",
                        metric_value=change_pct,
                        threshold=self.THRESHOLDS['sales_decline_pct'],
                        recommendation="Immediate action needed: Review pricing strategy, marketing campaigns, "
                                      "inventory availability, and competitive landscape. Consider promotional offers."
                    )
                elif change_pct > self.THRESHOLDS['sales_growth_pct']:
                    self._create_insight(
                        title="Strong sales growth",
                        description=f"Sales have increased {change_pct:.1f}% compared to the previous period.",
                        priority=InsightPriority.LOW,
                        category=InsightCategory.SALES,
                        data_source="sales",
                        metric_value=change_pct,
                        threshold=self.THRESHOLDS['sales_growth_pct'],
                        recommendation="Analyze what's driving growth (products, promotions, seasonal factors) "
                                      "to sustain momentum. Ensure inventory can support increased demand."
                    )

        # Rule 3: Average Order Value (AOV) analysis
        if 'aov' in sales_data:
            aov_data = sales_data['aov']
            if isinstance(aov_data, dict) and len(aov_data) >= 2:
                aov_values = list(aov_data.values())
                if min(aov_values) > 0:
                    aov_gap_pct = ((max(aov_values) - min(aov_values)) / min(aov_values)) * 100

                    if aov_gap_pct > self.THRESHOLDS['aov_gap_pct']:
                        high_store = max(aov_data, key=aov_data.get)
                        low_store = min(aov_data, key=aov_data.get)

                        self._create_insight(
                            title="AOV disparity between stores",
                            description=f"{high_store} has ${aov_data[high_store]:.2f} AOV vs {low_store} "
                                       f"at ${aov_data[low_store]:.2f} ({aov_gap_pct:.0f}% gap).",
                            priority=InsightPriority.MEDIUM,
                            category=InsightCategory.SALES,
                            data_source="sales",
                            metric_value=aov_gap_pct,
                            threshold=self.THRESHOLDS['aov_gap_pct'],
                            recommendation=f"Train {low_store} staff on upselling and cross-selling techniques used at {high_store}. "
                                          "Review product placement and bundle offerings."
                        )

        # Rule 4: Margin analysis
        if 'margin' in sales_data:
            margin = sales_data['margin']
            if isinstance(margin, (int, float)):
                if margin < self.THRESHOLDS['margin_concern_pct']:
                    self._create_insight(
                        title="Profit margin below target",
                        description=f"Current margin is {margin:.1f}%, below the {self.THRESHOLDS['margin_concern_pct']}% target.",
                        priority=InsightPriority.HIGH,
                        category=InsightCategory.SALES,
                        data_source="sales",
                        metric_value=margin,
                        threshold=self.THRESHOLDS['margin_concern_pct'],
                        recommendation="Review pricing strategy, negotiate better vendor terms, "
                                      "reduce discounting, and focus on higher-margin product categories."
                    )

        # Rule 5: Day-of-week patterns
        if 'daily_pattern' in sales_data:
            daily = sales_data['daily_pattern']
            if daily:
                avg_daily = sum(daily.values()) / len(daily)
                weak_days = [day for day, sales in daily.items() if sales < avg_daily * 0.7]

                if weak_days:
                    self._create_insight(
                        title="Underperforming days identified",
                        description=f"Sales on {', '.join(weak_days)} are significantly below average.",
                        priority=InsightPriority.MEDIUM,
                        category=InsightCategory.SALES,
                        data_source="sales",
                        metric_value=weak_days,
                        threshold="<70% of average",
                        recommendation=f"Consider targeted promotions or events on {', '.join(weak_days)} "
                                      "to boost traffic. Review staffing levels on these days."
                    )

    # =========================================================================
    # BRAND ANALYSIS RULES
    # =========================================================================

    def _analyze_brands(self, brand_data: Dict) -> None:
        """Analyze brand performance data."""

        brands = brand_data.get('brands', {})
        if not brands:
            return

        total_sales = sum(b.get('sales', 0) for b in brands.values())
        total_units = sum(b.get('units', 0) for b in brands.values())

        if total_sales == 0:
            return

        # Rule 1: Brand concentration risk
        top_brand_sales = max(b.get('sales', 0) for b in brands.values())
        top_brand_pct = (top_brand_sales / total_sales) * 100
        top_brand_name = max(brands, key=lambda x: brands[x].get('sales', 0))

        if top_brand_pct > self.THRESHOLDS['brand_concentration_pct']:
            self._create_insight(
                title="High brand concentration risk",
                description=f"{top_brand_name} accounts for {top_brand_pct:.0f}% of total sales.",
                priority=InsightPriority.HIGH,
                category=InsightCategory.INVENTORY,
                data_source="brands",
                metric_value=top_brand_pct,
                threshold=self.THRESHOLDS['brand_concentration_pct'],
                recommendation=f"Diversify product mix to reduce dependency on {top_brand_name}. "
                              "Promote alternative brands and negotiate backup supplier agreements."
            )

        # Rule 2: High margin + low sales opportunities
        opportunities = []
        for name, data in brands.items():
            margin = data.get('margin', 0)
            sales_pct = (data.get('sales', 0) / total_sales) * 100 if total_sales > 0 else 0

            if margin > self.THRESHOLDS['high_margin_low_sales_pct'] and sales_pct < 5:
                opportunities.append({
                    'name': name,
                    'margin': margin,
                    'sales_pct': sales_pct
                })

        if opportunities:
            opp_names = [o['name'] for o in opportunities[:3]]
            self._create_insight(
                title="High-margin brands underperforming",
                description=f"{', '.join(opp_names)} have >40% margins but low sales volume.",
                priority=InsightPriority.MEDIUM,
                category=InsightCategory.INVENTORY,
                data_source="brands",
                metric_value=len(opportunities),
                threshold="margin >40%, sales <5%",
                recommendation="Increase visibility of these brands through better placement, "
                              "staff recommendations, and targeted promotions to boost profitable sales."
            )

        # Rule 3: Low margin brands taking shelf space
        low_margin_brands = []
        for name, data in brands.items():
            margin = data.get('margin', 0)
            sales_pct = (data.get('sales', 0) / total_sales) * 100

            if margin < self.THRESHOLDS['underperforming_margin_pct'] and sales_pct > 5:
                low_margin_brands.append({
                    'name': name,
                    'margin': margin,
                    'sales_pct': sales_pct
                })

        if low_margin_brands:
            brand_names = [b['name'] for b in low_margin_brands[:3]]
            self._create_insight(
                title="Low-margin brands consuming resources",
                description=f"{', '.join(brand_names)} have margins below {self.THRESHOLDS['underperforming_margin_pct']}% "
                           "but significant sales volume.",
                priority=InsightPriority.MEDIUM,
                category=InsightCategory.INVENTORY,
                data_source="brands",
                metric_value=len(low_margin_brands),
                threshold=f"margin <{self.THRESHOLDS['underperforming_margin_pct']}%",
                recommendation="Renegotiate pricing with these vendors or consider replacing with "
                              "higher-margin alternatives. Don't eliminate if they drive traffic."
            )

        # Rule 4: Declining brands
        if 'trends' in brand_data:
            declining = []
            for name, trend in brand_data['trends'].items():
                if trend.get('change_pct', 0) < -20:
                    declining.append({
                        'name': name,
                        'change': trend['change_pct']
                    })

            if declining:
                self._create_insight(
                    title="Multiple brands showing decline",
                    description=f"{len(declining)} brands have declined >20%: "
                               f"{', '.join([d['name'] for d in declining[:3]])}",
                    priority=InsightPriority.MEDIUM,
                    category=InsightCategory.INVENTORY,
                    data_source="brands",
                    metric_value=len(declining),
                    threshold="-20% change",
                    recommendation="Review inventory levels for declining brands. Consider clearance pricing "
                                  "and reallocate shelf space to growing brands."
                )

    # =========================================================================
    # CUSTOMER ANALYSIS RULES
    # =========================================================================

    def _analyze_customers(self, customer_data: Dict) -> None:
        """Analyze customer metrics for insights."""

        # Rule 1: Customer churn
        if 'churn_rate' in customer_data:
            churn = customer_data['churn_rate']
            if churn > self.THRESHOLDS['customer_churn_pct']:
                self._create_insight(
                    title="High customer churn rate",
                    description=f"Customer churn is at {churn:.1f}%, indicating retention issues.",
                    priority=InsightPriority.CRITICAL,
                    category=InsightCategory.CUSTOMERS,
                    data_source="customers",
                    metric_value=churn,
                    threshold=self.THRESHOLDS['customer_churn_pct'],
                    recommendation="Implement loyalty program enhancements, personalized outreach to at-risk customers, "
                                  "and investigate common reasons for customer departure."
                )

        # Rule 2: New customer acquisition
        if 'new_customer_change' in customer_data:
            change = customer_data['new_customer_change']
            if change < self.THRESHOLDS['new_customer_decline_pct']:
                self._create_insight(
                    title="New customer acquisition declining",
                    description=f"New customer sign-ups have dropped {abs(change):.1f}% vs previous period.",
                    priority=InsightPriority.HIGH,
                    category=InsightCategory.CUSTOMERS,
                    data_source="customers",
                    metric_value=change,
                    threshold=self.THRESHOLDS['new_customer_decline_pct'],
                    recommendation="Review marketing spend effectiveness, SEO/online presence, "
                                  "and consider referral incentives or new customer promotions."
                )

        # Rule 3: Customer concentration
        if 'top_customer_revenue_pct' in customer_data:
            concentration = customer_data['top_customer_revenue_pct']
            if concentration > self.THRESHOLDS['customer_concentration_pct']:
                self._create_insight(
                    title="Revenue concentrated in few customers",
                    description=f"Top 10% of customers generate {concentration:.0f}% of revenue.",
                    priority=InsightPriority.MEDIUM,
                    category=InsightCategory.CUSTOMERS,
                    data_source="customers",
                    metric_value=concentration,
                    threshold=self.THRESHOLDS['customer_concentration_pct'],
                    recommendation="Focus on growing the middle tier customer base. "
                                  "Implement tiered loyalty benefits to increase spending from average customers."
                )

        # Rule 4: Customer segment shifts
        if 'segment_changes' in customer_data:
            segments = customer_data['segment_changes']
            concerning = []
            for segment, change in segments.items():
                if change < -15:
                    concerning.append({'segment': segment, 'change': change})

            if concerning:
                self._create_insight(
                    title="Customer segment decline",
                    description=f"Significant decline in {', '.join([c['segment'] for c in concerning])} segments.",
                    priority=InsightPriority.MEDIUM,
                    category=InsightCategory.CUSTOMERS,
                    data_source="customers",
                    metric_value=concerning,
                    threshold="-15% segment change",
                    recommendation="Analyze what's causing segment decline. Consider targeted marketing "
                                  "and product adjustments for affected customer groups."
                )

        # Rule 5: LTV trends
        if 'ltv_trend' in customer_data:
            ltv_change = customer_data['ltv_trend']
            if ltv_change < -10:
                self._create_insight(
                    title="Customer lifetime value declining",
                    description=f"Average customer LTV has dropped {abs(ltv_change):.1f}%.",
                    priority=InsightPriority.HIGH,
                    category=InsightCategory.CUSTOMERS,
                    data_source="customers",
                    metric_value=ltv_change,
                    threshold="-10% LTV change",
                    recommendation="Focus on increasing purchase frequency through loyalty programs, "
                                  "personalized recommendations, and re-engagement campaigns."
                )

    # =========================================================================
    # INVOICE/PURCHASING ANALYSIS RULES
    # =========================================================================

    def _analyze_invoices(self, invoice_data: Dict) -> None:
        """Analyze invoice/purchasing data."""

        vendors = invoice_data.get('vendors', {})
        total_spend = invoice_data.get('total_value', 0)

        if not vendors or total_spend == 0:
            return

        # Rule 1: Vendor concentration
        top_vendor_spend = max(v.get('total_spend', 0) for v in vendors.values())
        top_vendor_pct = (top_vendor_spend / total_spend) * 100
        top_vendor_name = max(vendors, key=lambda x: vendors[x].get('total_spend', 0))

        if top_vendor_pct > self.THRESHOLDS['vendor_concentration_pct']:
            self._create_insight(
                title="High vendor concentration risk",
                description=f"{top_vendor_name} accounts for {top_vendor_pct:.0f}% of total purchasing spend.",
                priority=InsightPriority.HIGH,
                category=InsightCategory.PURCHASING,
                data_source="invoices",
                metric_value=top_vendor_pct,
                threshold=self.THRESHOLDS['vendor_concentration_pct'],
                recommendation=f"Diversify suppliers to reduce dependency on {top_vendor_name}. "
                              "Identify backup vendors for critical product categories."
            )

        # Rule 2: Spend increase alert
        if 'spend_change_pct' in invoice_data:
            change = invoice_data['spend_change_pct']
            if change > self.THRESHOLDS['spend_increase_pct']:
                self._create_insight(
                    title="Significant spend increase",
                    description=f"Purchasing spend has increased {change:.1f}% vs previous period.",
                    priority=InsightPriority.MEDIUM,
                    category=InsightCategory.PURCHASING,
                    data_source="invoices",
                    metric_value=change,
                    threshold=self.THRESHOLDS['spend_increase_pct'],
                    recommendation="Review if spend increase is justified by sales growth. "
                                  "Audit recent purchases for overstocking or price increases."
                )

        # Rule 3: Invoice frequency patterns
        if len(vendors) > 5:
            avg_invoices = sum(v.get('invoice_count', 0) for v in vendors.values()) / len(vendors)
            high_freq_vendors = [
                name for name, data in vendors.items()
                if data.get('invoice_count', 0) > avg_invoices * 2
            ]

            if high_freq_vendors:
                self._create_insight(
                    title="High-frequency vendor ordering",
                    description=f"{', '.join(high_freq_vendors[:3])} have significantly more invoices than average.",
                    priority=InsightPriority.LOW,
                    category=InsightCategory.PURCHASING,
                    data_source="invoices",
                    metric_value=len(high_freq_vendors),
                    threshold=">2x average frequency",
                    recommendation="Consider consolidating orders with these vendors to reduce "
                                  "processing overhead and potentially negotiate volume discounts."
                )

        # Rule 4: Small order detection
        small_orders = []
        for name, data in vendors.items():
            if data.get('invoice_count', 0) > 0:
                avg_order = data.get('total_spend', 0) / data['invoice_count']
                if avg_order < 500:  # Small orders under $500
                    small_orders.append({'name': name, 'avg_order': avg_order})

        if len(small_orders) > 3:
            self._create_insight(
                title="Many small orders detected",
                description=f"{len(small_orders)} vendors have average orders under $500.",
                priority=InsightPriority.LOW,
                category=InsightCategory.OPERATIONS,
                data_source="invoices",
                metric_value=len(small_orders),
                threshold="<$500 average order",
                recommendation="Consolidate small orders to reduce operational overhead. "
                              "Consider minimum order thresholds or scheduled ordering."
            )

    # =========================================================================
    # PRODUCT PURCHASING ANALYSIS RULES
    # =========================================================================

    def _analyze_products(self, product_data: Dict) -> None:
        """Analyze product purchasing patterns."""

        brands = product_data.get('brands', {})
        product_types = product_data.get('product_types', {})

        if not brands and not product_types:
            return

        # Rule 1: Product type concentration
        if product_types:
            total_cost = sum(pt.get('total_cost', 0) for pt in product_types.values())
            if total_cost > 0:
                for ptype, data in product_types.items():
                    pct = (data.get('total_cost', 0) / total_cost) * 100
                    if pct > 50:
                        self._create_insight(
                            title="Product category over-concentration",
                            description=f"{ptype} represents {pct:.0f}% of total product purchasing.",
                            priority=InsightPriority.MEDIUM,
                            category=InsightCategory.INVENTORY,
                            data_source="products",
                            metric_value=pct,
                            threshold="50%",
                            recommendation=f"Diversify product mix beyond {ptype}. "
                                          "Analyze sales data to identify growth categories."
                        )

        # Rule 2: Unit cost analysis by type
        if product_types:
            type_costs = []
            for ptype, data in product_types.items():
                units = data.get('total_units', 0)
                cost = data.get('total_cost', 0)
                if units > 0:
                    type_costs.append({
                        'type': ptype,
                        'unit_cost': cost / units,
                        'units': units
                    })

            if len(type_costs) >= 2:
                type_costs.sort(key=lambda x: x['unit_cost'], reverse=True)
                high_cost = type_costs[0]
                low_cost = type_costs[-1]

                if low_cost['unit_cost'] > 0:
                    ratio = high_cost['unit_cost'] / low_cost['unit_cost']
                    if ratio > 5:  # 5x difference
                        self._create_insight(
                            title="Wide unit cost variance across categories",
                            description=f"{high_cost['type']} costs ${high_cost['unit_cost']:.2f}/unit vs "
                                       f"{low_cost['type']} at ${low_cost['unit_cost']:.2f}/unit ({ratio:.1f}x difference).",
                            priority=InsightPriority.LOW,
                            category=InsightCategory.PURCHASING,
                            data_source="products",
                            metric_value=ratio,
                            threshold="5x",
                            recommendation="Ensure pricing strategy accounts for cost differences. "
                                          "Verify margins are appropriate for each category."
                        )

        # Rule 3: Brand diversity in purchasing
        if brands and len(brands) > 10:
            total_brand_cost = sum(b.get('total_cost', 0) for b in brands.values())
            top_5_cost = sum(
                sorted([b.get('total_cost', 0) for b in brands.values()], reverse=True)[:5]
            )
            top_5_pct = (top_5_cost / total_brand_cost) * 100 if total_brand_cost > 0 else 0

            if top_5_pct > 70:
                self._create_insight(
                    title="Purchasing concentrated in few brands",
                    description=f"Top 5 brands represent {top_5_pct:.0f}% of total brand purchasing.",
                    priority=InsightPriority.MEDIUM,
                    category=InsightCategory.PURCHASING,
                    data_source="products",
                    metric_value=top_5_pct,
                    threshold="70%",
                    recommendation="Consider expanding brand portfolio to reduce risk and offer "
                                  "customers more variety. Evaluate emerging brands for test runs."
                )

    # =========================================================================
    # RESEARCH FINDINGS ANALYSIS RULES
    # =========================================================================

    def _analyze_research(self, research_data: Dict) -> None:
        """Analyze industry research findings for actionable insights."""

        key_findings = research_data.get('key_findings', [])
        action_items = research_data.get('action_items', [])
        tracking_items = research_data.get('tracking_items', [])

        # Rule 1: Urgent action items
        urgent_actions = [a for a in action_items if 'urgent' in str(a).lower() or 'immediate' in str(a).lower()]
        if urgent_actions:
            self._create_insight(
                title="Urgent action items from research",
                description=f"{len(urgent_actions)} urgent action items identified from industry research.",
                priority=InsightPriority.CRITICAL,
                category=InsightCategory.COMPLIANCE,
                data_source="research",
                metric_value=len(urgent_actions),
                threshold="any urgent items",
                recommendation=f"Review and address: {urgent_actions[0] if urgent_actions else 'See research findings'}"
            )

        # Rule 2: Regulatory findings
        regulatory_keywords = ['regulation', 'compliance', 'law', 'license', 'permit', 'legal', 'requirement']
        regulatory_findings = [
            f for f in key_findings
            if any(kw in str(f).lower() for kw in regulatory_keywords)
        ]

        if regulatory_findings:
            self._create_insight(
                title="Regulatory updates require attention",
                description=f"{len(regulatory_findings)} regulatory-related findings identified.",
                priority=InsightPriority.HIGH,
                category=InsightCategory.COMPLIANCE,
                data_source="research",
                metric_value=len(regulatory_findings),
                threshold="any regulatory items",
                recommendation="Review regulatory findings and ensure compliance. "
                              "Consult with legal/compliance team if needed."
            )

        # Rule 3: Market opportunity findings
        opportunity_keywords = ['opportunity', 'growth', 'trend', 'emerging', 'demand', 'expand']
        opportunities = [
            f for f in key_findings
            if any(kw in str(f).lower() for kw in opportunity_keywords)
        ]

        if opportunities:
            self._create_insight(
                title="Market opportunities identified",
                description=f"{len(opportunities)} potential market opportunities found in research.",
                priority=InsightPriority.MEDIUM,
                category=InsightCategory.MARKETING,
                data_source="research",
                metric_value=len(opportunities),
                threshold="any opportunities",
                recommendation="Evaluate identified opportunities for strategic fit. "
                              "Prioritize based on potential ROI and resource requirements."
            )

        # Rule 4: Competitive threats
        threat_keywords = ['competitor', 'threat', 'risk', 'challenge', 'pressure', 'decline']
        threats = [
            f for f in key_findings
            if any(kw in str(f).lower() for kw in threat_keywords)
        ]

        if threats:
            self._create_insight(
                title="Competitive threats identified",
                description=f"{len(threats)} potential competitive threats found in research.",
                priority=InsightPriority.MEDIUM,
                category=InsightCategory.OPERATIONS,
                data_source="research",
                metric_value=len(threats),
                threshold="any threats",
                recommendation="Analyze competitive landscape and develop defensive strategies. "
                              "Focus on differentiation and customer retention."
            )

        # Rule 5: Pending tracking items
        if tracking_items:
            self._create_insight(
                title="Items being tracked from research",
                description=f"{len(tracking_items)} items flagged for ongoing monitoring.",
                priority=InsightPriority.LOW,
                category=InsightCategory.OPERATIONS,
                data_source="research",
                metric_value=len(tracking_items),
                threshold="any tracking items",
                recommendation="Set up regular review cycle for tracked items. "
                              "Consider automation for monitoring key metrics."
            )

    # =========================================================================
    # SEO ANALYSIS RULES
    # =========================================================================

    def _analyze_seo(self, seo_data: Dict) -> None:
        """Analyze SEO data for website optimization insights."""

        for site_name, data in seo_data.items():
            overall_score = data.get('overall_score', 0)
            categories = data.get('categories', {})
            top_priorities = data.get('top_priorities', [])
            quick_wins = data.get('quick_wins', [])

            # Rule 1: Critical SEO score
            if overall_score < self.THRESHOLDS['seo_score_critical']:
                self._create_insight(
                    title=f"{site_name} SEO score critically low",
                    description=f"{site_name} has an SEO score of {overall_score}/100, well below acceptable levels.",
                    priority=InsightPriority.CRITICAL,
                    category=InsightCategory.MARKETING,
                    data_source="seo",
                    metric_value=overall_score,
                    threshold=self.THRESHOLDS['seo_score_critical'],
                    recommendation="Immediate SEO remediation needed. Focus on technical issues first, "
                                  "then content optimization. Consider SEO audit and professional help."
                )
            elif overall_score < self.THRESHOLDS['seo_score_warning']:
                self._create_insight(
                    title=f"{site_name} SEO needs improvement",
                    description=f"{site_name} SEO score is {overall_score}/100, below target of {self.THRESHOLDS['seo_improvement_target']}.",
                    priority=InsightPriority.MEDIUM,
                    category=InsightCategory.MARKETING,
                    data_source="seo",
                    metric_value=overall_score,
                    threshold=self.THRESHOLDS['seo_score_warning'],
                    recommendation="Implement SEO improvements to reach target score. "
                                  "Start with quick wins and prioritized issues."
                )

            # Rule 2: Category-specific issues
            for category, cat_data in categories.items():
                if isinstance(cat_data, dict):
                    cat_score = cat_data.get('score', 100)
                    if cat_score < 50:
                        self._create_insight(
                            title=f"{site_name}: {category} SEO severely lacking",
                            description=f"{category} category scored only {cat_score}/100 on {site_name}.",
                            priority=InsightPriority.HIGH,
                            category=InsightCategory.MARKETING,
                            data_source="seo",
                            metric_value=cat_score,
                            threshold=50,
                            recommendation=f"Focus on improving {category} for {site_name}. "
                                          "This category is significantly impacting overall SEO performance."
                        )

            # Rule 3: Quick wins available
            if quick_wins and len(quick_wins) >= 3:
                self._create_insight(
                    title=f"{site_name} has easy SEO wins",
                    description=f"{len(quick_wins)} quick SEO improvements available for {site_name}.",
                    priority=InsightPriority.LOW,
                    category=InsightCategory.MARKETING,
                    data_source="seo",
                    metric_value=len(quick_wins),
                    threshold="3+ quick wins",
                    recommendation=f"Implement quick wins: {quick_wins[0] if quick_wins else 'See SEO analysis'}"
                )

            # Rule 4: Top priorities
            if top_priorities:
                critical_priorities = [p for p in top_priorities if 'critical' in str(p).lower()]
                if critical_priorities:
                    self._create_insight(
                        title=f"{site_name} has critical SEO issues",
                        description=f"{len(critical_priorities)} critical SEO priorities for {site_name}.",
                        priority=InsightPriority.HIGH,
                        category=InsightCategory.MARKETING,
                        data_source="seo",
                        metric_value=len(critical_priorities),
                        threshold="any critical",
                        recommendation=f"Address critical issue: {critical_priorities[0]}"
                    )

    # =========================================================================
    # CROSS-DATA ANALYSIS RULES
    # =========================================================================

    def _analyze_cross_data(self, data: Dict) -> None:
        """Analyze relationships across multiple data sources."""

        sales_data = data.get('sales_data', {})
        invoice_data = data.get('invoice_data', {})
        brand_data = data.get('brand_data', {})
        customer_data = data.get('customer_data', {})

        # Rule 1: Sales growth vs purchasing alignment
        sales_change = sales_data.get('trend', {}).get('period_over_period_change', 0)
        spend_change = invoice_data.get('spend_change_pct', 0)

        if sales_change > 0 and spend_change > sales_change * 1.5:
            self._create_insight(
                title="Purchasing outpacing sales growth",
                description=f"Spending up {spend_change:.1f}% while sales only up {sales_change:.1f}%.",
                priority=InsightPriority.HIGH,
                category=InsightCategory.PURCHASING,
                data_source="cross-analysis",
                metric_value={'sales': sales_change, 'spend': spend_change},
                threshold="spend > 1.5x sales growth",
                recommendation="Review purchasing strategy. May be building inventory, "
                              "but ensure alignment with demand forecasts."
            )
        elif sales_change < 0 and spend_change > 0:
            self._create_insight(
                title="Spending increasing despite sales decline",
                description=f"Sales down {abs(sales_change):.1f}% but spending up {spend_change:.1f}%.",
                priority=InsightPriority.CRITICAL,
                category=InsightCategory.PURCHASING,
                data_source="cross-analysis",
                metric_value={'sales': sales_change, 'spend': spend_change},
                threshold="sales down + spend up",
                recommendation="Immediate review needed. Reduce purchasing to align with demand. "
                              "Risk of overstocking and cash flow issues."
            )

        # Rule 2: Brand concentration in both purchasing and sales
        purchase_brands = data.get('product_data', {}).get('brands', {})
        sales_brands = brand_data.get('brands', {})

        if purchase_brands and sales_brands:
            # Find brands we buy a lot but don't sell well
            for brand_name in purchase_brands:
                purchase_pct = 0
                sales_pct = 0

                total_purchase = sum(b.get('total_cost', 0) for b in purchase_brands.values())
                if total_purchase > 0:
                    purchase_pct = (purchase_brands[brand_name].get('total_cost', 0) / total_purchase) * 100

                if brand_name in sales_brands:
                    total_sales = sum(b.get('sales', 0) for b in sales_brands.values())
                    if total_sales > 0:
                        sales_pct = (sales_brands[brand_name].get('sales', 0) / total_sales) * 100

                # High purchase % but low sales %
                if purchase_pct > 15 and sales_pct < 5:
                    self._create_insight(
                        title=f"{brand_name}: high purchasing, low sales",
                        description=f"{brand_name} is {purchase_pct:.0f}% of purchases but only {sales_pct:.0f}% of sales.",
                        priority=InsightPriority.HIGH,
                        category=InsightCategory.INVENTORY,
                        data_source="cross-analysis",
                        metric_value={'purchase_pct': purchase_pct, 'sales_pct': sales_pct},
                        threshold="purchase >15%, sales <5%",
                        recommendation=f"Reduce {brand_name} orders. Product may be overstocked or underperforming. "
                                      "Consider promotions or discontinuation."
                    )
                    break  # Only report first one found

        # Rule 3: Customer health vs sales performance
        churn_rate = customer_data.get('churn_rate', 0)
        sales_trend = sales_data.get('trend', {}).get('period_over_period_change', 0)

        if churn_rate > 15 and sales_trend > 10:
            self._create_insight(
                title="Sales growing despite customer churn",
                description=f"Sales up {sales_trend:.1f}% but churn at {churn_rate:.1f}%.",
                priority=InsightPriority.MEDIUM,
                category=InsightCategory.CUSTOMERS,
                data_source="cross-analysis",
                metric_value={'churn': churn_rate, 'sales': sales_trend},
                threshold="churn >15% + sales >10%",
                recommendation="Growth is coming from higher spend per customer or new customers. "
                              "Still prioritize retention - acquiring new customers costs more than keeping existing ones."
            )


def get_insights_engine(aws_config: Dict = None) -> Optional[InsightsEngine]:
    """
    Helper function to initialize insights engine from Streamlit secrets or config.

    Args:
        aws_config: Optional AWS config dict

    Returns:
        InsightsEngine instance or None if setup fails
    """
    if not aws_config:
        aws_config = {}

        # Try Streamlit secrets
        if STREAMLIT_AVAILABLE:
            try:
                aws_secrets = st.secrets.get("aws", {})
                aws_config['aws_access_key'] = aws_secrets.get('access_key_id')
                aws_config['aws_secret_key'] = aws_secrets.get('secret_access_key')
                aws_config['region'] = aws_secrets.get('region', 'us-west-1')
            except:
                pass

        # Try environment variables
        if not aws_config.get('aws_access_key'):
            aws_config['aws_access_key'] = os.environ.get('AWS_ACCESS_KEY_ID')
            aws_config['aws_secret_key'] = os.environ.get('AWS_SECRET_ACCESS_KEY')
            aws_config['region'] = os.environ.get('AWS_REGION', 'us-west-1')

    try:
        return InsightsEngine(
            aws_access_key=aws_config.get('aws_access_key'),
            aws_secret_key=aws_config.get('aws_secret_key'),
            region=aws_config.get('region', 'us-west-1')
        )
    except Exception as e:
        print(f"Failed to initialize InsightsEngine: {e}")
        return None
