"""
SEO Research Agent
Autonomous agent that performs daily SEO analysis on multiple target websites.
Designed to run on AWS Lambda with EventBridge scheduling.
Integrates with the retail-analytics-dashboard.

Configured to scan:
- barbarycoastsf.com
- grassrootssf.com

Rate Limiting:
- Uses TokenThrottleManager to stay under 30k tokens/minute
- 85% safety margin (25,500 effective limit)
- Automatic window-based throttling
"""

import json
import os
import boto3
import anthropic
from datetime import datetime, timedelta
from typing import Optional, List
import hashlib
import time

# Configuration from environment
# Multiple websites supported - comma-separated in env var
TARGET_WEBSITES_STR = os.environ.get("TARGET_WEBSITES", "https://barbarycoastsf.com,https://grassrootssf.com")
TARGET_WEBSITES = [w.strip() for w in TARGET_WEBSITES_STR.split(",") if w.strip()]

# Fallback for single website config (backwards compatibility)
if not TARGET_WEBSITES:
    single_site = os.environ.get("TARGET_WEBSITE", "https://barbarycoastsf.com")
    TARGET_WEBSITES = [single_site]

S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "retail-data-bcgr")
S3_PREFIX = os.environ.get("S3_PREFIX", "seo-analysis")

# Rate Limiting Configuration
TOKENS_PER_MINUTE_LIMIT = int(os.environ.get("TOKENS_PER_MINUTE_LIMIT", "30000"))
SAFETY_MARGIN = float(os.environ.get("SAFETY_MARGIN", "0.85"))


# =============================================================================
# TOKEN THROTTLE MANAGER
# =============================================================================

class TokenThrottleManager:
    """Manages API rate limits by tracking token usage and implementing pauses."""

    def __init__(self, tokens_per_minute: int = 30000, safety_margin: float = 0.85):
        self.tokens_per_minute = tokens_per_minute
        self.safety_margin = safety_margin  # Use 85% of limit to be safe
        self.max_tokens = int(tokens_per_minute * safety_margin)

        self.window_start = datetime.utcnow()
        self.tokens_used = 0
        self.api_calls = []

    def estimate_tokens(self, text: str) -> int:
        """Rough token estimation: ~4 characters per token."""
        return len(text) // 4

    def record_usage(self, input_tokens: int, output_tokens: int = 0):
        """Record token usage from an API call."""
        total = input_tokens + output_tokens
        self.tokens_used += total
        self.api_calls.append({
            "timestamp": datetime.utcnow(),
            "tokens": total
        })
        print(f"  📊 Tokens used: {total} (total: {self.tokens_used}/{self.max_tokens})")

    def get_usage_from_response(self, response) -> tuple:
        """Extract token usage from Anthropic API response."""
        input_tokens = getattr(response.usage, 'input_tokens', 0)
        output_tokens = getattr(response.usage, 'output_tokens', 0)
        return input_tokens, output_tokens

    def can_proceed(self, estimated_tokens: int = 5000) -> bool:
        """Check if we can make another API call without exceeding limits."""
        self._reset_window_if_needed()
        return (self.tokens_used + estimated_tokens) < self.max_tokens

    def wait_if_needed(self, estimated_tokens: int = 5000) -> float:
        """Wait if necessary to stay under rate limits. Returns seconds waited."""
        self._reset_window_if_needed()

        if (self.tokens_used + estimated_tokens) >= self.max_tokens:
            elapsed = (datetime.utcnow() - self.window_start).total_seconds()
            wait_time = max(65 - elapsed, 5)  # Always wait at least 5 seconds

            if wait_time > 0:
                print(f"  ⏸️  Rate limit approaching ({self.tokens_used}/{self.max_tokens} tokens used)")
                print(f"  ⏸️  Pausing for {wait_time:.1f} seconds to reset rate limit window...")
                time.sleep(wait_time)
                self._reset_window()
                return wait_time

        # Add a small delay between all API calls to spread out requests
        time.sleep(2)
        return 0

    def _reset_window_if_needed(self):
        """Reset the tracking window if 60 seconds have passed."""
        elapsed = (datetime.utcnow() - self.window_start).total_seconds()
        if elapsed >= 60:
            self._reset_window()

    def _reset_window(self):
        """Reset the rate limit tracking window."""
        self.window_start = datetime.utcnow()
        self.tokens_used = 0
        self.api_calls = []
        print(f"  🔄 Rate limit window reset")

    def get_stats(self) -> dict:
        """Get current usage statistics."""
        elapsed = (datetime.utcnow() - self.window_start).total_seconds()
        return {
            "tokens_used": self.tokens_used,
            "tokens_limit": self.max_tokens,
            "utilization": f"{(self.tokens_used / self.max_tokens * 100):.1f}%",
            "window_elapsed": f"{elapsed:.1f}s",
            "api_calls": len(self.api_calls)
        }

# SEO Analysis Categories
SEO_ANALYSIS_CATEGORIES = [
    {
        "id": "technical_seo",
        "name": "Technical SEO",
        "description": "Site structure, crawlability, indexing, page speed indicators",
        "importance": "high",
        "queries": [
            "{site} site structure analysis",
            "{site} page load speed",
            "{site} mobile optimization",
            "{site} sitemap robots.txt"
        ]
    },
    {
        "id": "on_page_seo",
        "name": "On-Page SEO",
        "description": "Title tags, meta descriptions, headings, content quality",
        "importance": "high",
        "queries": [
            "{site} meta tags analysis",
            "{site} content structure",
            "{site} keyword optimization",
            "{site} internal linking"
        ]
    },
    {
        "id": "content_analysis",
        "name": "Content Analysis",
        "description": "Content freshness, depth, relevance, and keyword coverage",
        "importance": "high",
        "queries": [
            "{site} content quality",
            "{site} blog posts articles",
            "{site} product descriptions",
            "{site} content gaps"
        ]
    },
    {
        "id": "backlink_profile",
        "name": "Backlink Profile",
        "description": "Domain authority, backlink quality, referring domains",
        "importance": "medium",
        "queries": [
            "{site} backlinks",
            "{site} domain authority",
            "{site} referring domains",
            "{site} link building"
        ]
    },
    {
        "id": "competitor_analysis",
        "name": "Competitor Analysis",
        "description": "SEO comparison with competitors, market positioning",
        "importance": "medium",
        "queries": [
            "{site} vs competitors SEO",
            "{site} market competitors",
            "{site} industry SEO benchmark",
            "{site} competitive analysis"
        ]
    },
    {
        "id": "local_seo",
        "name": "Local SEO",
        "description": "Local listings, Google Business Profile, local citations",
        "importance": "medium",
        "queries": [
            "{site} local SEO",
            "{site} Google Business Profile",
            "{site} local listings citations",
            "{site} local search ranking"
        ]
    },
    {
        "id": "user_experience",
        "name": "User Experience Signals",
        "description": "Core Web Vitals, mobile usability, navigation",
        "importance": "high",
        "queries": [
            "{site} Core Web Vitals",
            "{site} user experience",
            "{site} mobile usability",
            "{site} navigation structure"
        ]
    }
]


class SEOStorage:
    """Handles S3 storage operations for SEO analysis findings."""
    
    def __init__(self, bucket: str, prefix: str, website: Optional[str] = None):
        self.bucket = bucket
        self.base_prefix = prefix
        self.website = website
        self.s3 = boto3.client('s3')
        
        # Create a safe folder name from the website URL
        if website:
            self.site_folder = self._url_to_folder(website)
            self.prefix = f"{prefix}/{self.site_folder}"
        else:
            self.site_folder = None
            self.prefix = prefix
    
    def _url_to_folder(self, url: str) -> str:
        """Convert URL to safe folder name."""
        # Remove protocol and trailing slashes
        folder = url.replace("https://", "").replace("http://", "").rstrip("/")
        # Replace unsafe characters
        folder = folder.replace("/", "_").replace(":", "_")
        return folder
    
    def _key(self, *parts) -> str:
        """Build S3 key from parts."""
        return f"{self.prefix}/{'/'.join(parts)}"
    
    def save_findings(self, findings: dict, date: Optional[datetime] = None) -> str:
        """Save daily SEO findings."""
        date = date or datetime.utcnow()
        key = self._key(date.strftime("%Y/%m/%d"), "seo-findings.json")
        
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(findings, indent=2, default=str),
            ContentType="application/json"
        )
        return key
    
    def save_summary(self, summary: dict) -> str:
        """Save/update the latest SEO summary."""
        key = self._key("summary", "latest.json")
        
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(summary, indent=2, default=str),
            ContentType="application/json"
        )
        return key
    
    def load_summary(self) -> Optional[dict]:
        """Load the latest summary."""
        try:
            key = self._key("summary", "latest.json")
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error loading summary: {e}")
            return None
    
    def load_historical_context(self) -> Optional[dict]:
        """Load historical SEO context for trend analysis."""
        try:
            key = self._key("archive", "historical-context.json")
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error loading historical context: {e}")
            return None
    
    def save_historical_context(self, context: dict) -> str:
        """Save updated historical context."""
        key = self._key("archive", "historical-context.json")
        
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(context, indent=2, default=str),
            ContentType="application/json"
        )
        return key
    
    def load_recent_findings(self, days: int = 7) -> list:
        """Load findings from the last N days."""
        findings = []
        today = datetime.utcnow()
        
        for i in range(days):
            date = today - timedelta(days=i)
            try:
                key = self._key(date.strftime("%Y/%m/%d"), "seo-findings.json")
                response = self.s3.get_object(Bucket=self.bucket, Key=key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                findings.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "data": data
                })
            except self.s3.exceptions.NoSuchKey:
                continue
            except Exception as e:
                print(f"Error loading findings for {date}: {e}")
                continue
        
        return findings
    
    def list_monthly_archives(self) -> list:
        """List all monthly archive summaries."""
        archives = []
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            prefix = self._key("archive") + "/"
            
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    if 'monthly-summary.json' in obj['Key']:
                        archives.append(obj['Key'])
        except Exception as e:
            print(f"Error listing archives: {e}")
        
        return sorted(archives)
    
    def run_archival_cycle(self, delete_after: bool = False) -> dict:
        """Condense findings older than 30 days into monthly archives."""
        today = datetime.utcnow()
        cutoff = today - timedelta(days=30)
        archived_months = []
        
        # Find months to archive
        paginator = self.s3.get_paginator('list_objects_v2')
        prefix = self._key("")
        
        daily_keys_by_month = {}
        
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Match pattern: prefix/YYYY/MM/DD/seo-findings.json
                if '/seo-findings.json' in key and '/archive/' not in key and '/summary/' not in key:
                    try:
                        parts = key.replace(prefix, '').strip('/').split('/')
                        if len(parts) >= 3:
                            year, month, day = parts[0], parts[1], parts[2]
                            file_date = datetime(int(year), int(month), int(day))
                            
                            if file_date < cutoff:
                                month_key = f"{year}/{month}"
                                if month_key not in daily_keys_by_month:
                                    daily_keys_by_month[month_key] = []
                                daily_keys_by_month[month_key].append(key)
                    except (ValueError, IndexError):
                        continue
        
        # Archive each month
        # Create a throttle manager for archival operations
        throttle = TokenThrottleManager(
            tokens_per_minute=TOKENS_PER_MINUTE_LIMIT,
            safety_margin=SAFETY_MARGIN
        )
        
        for month_key, keys in daily_keys_by_month.items():
            archive_key = self._key("archive", month_key, "monthly-summary.json")
            
            # Check if already archived
            try:
                self.s3.head_object(Bucket=self.bucket, Key=archive_key)
                print(f"Month {month_key} already archived, skipping")
                continue
            except:
                pass
            
            # Load all findings for this month
            month_findings = []
            for key in keys:
                try:
                    response = self.s3.get_object(Bucket=self.bucket, Key=key)
                    data = json.loads(response['Body'].read().decode('utf-8'))
                    month_findings.append(data)
                except Exception as e:
                    print(f"Error loading {key}: {e}")
            
            if month_findings:
                # Condense using Claude
                condensed = self._condense_month(month_key, month_findings, throttle)
                
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=archive_key,
                    Body=json.dumps(condensed, indent=2, default=str),
                    ContentType="application/json"
                )
                
                archived_months.append(month_key)
                
                # Optionally delete original files
                if delete_after:
                    for key in keys:
                        try:
                            self.s3.delete_object(Bucket=self.bucket, Key=key)
                        except Exception as e:
                            print(f"Error deleting {key}: {e}")
        
        # Update historical context
        if archived_months:
            self._update_historical_context(throttle)
        
        return {
            "archived_months": archived_months,
            "total_months": len(archived_months)
        }
    
    def _condense_month(self, month_key: str, findings: list, throttle: TokenThrottleManager) -> dict:
        """Use Claude to condense a month's findings into a summary."""
        client = anthropic.Anthropic()
        
        prompt = f"""Analyze the following SEO findings from {month_key} and create a condensed monthly summary.

FINDINGS:
{json.dumps(findings, indent=2, default=str)[:50000]}

Create a JSON response with:
{{
    "month": "{month_key}",
    "executive_summary": "2-3 paragraph overview of SEO performance this month",
    "key_metrics": {{
        "overall_health_score": "1-100 estimate",
        "trend_direction": "improving/stable/declining",
        "priority_issues_count": number
    }},
    "category_summaries": {{
        "technical_seo": "summary",
        "on_page_seo": "summary",
        "content_analysis": "summary",
        "backlink_profile": "summary",
        "competitor_analysis": "summary",
        "local_seo": "summary",
        "user_experience": "summary"
    }},
    "major_changes": ["list of significant changes this month"],
    "improvements_made": ["list of improvements noted"],
    "ongoing_issues": ["list of persistent issues"],
    "recommendations_for_next_month": ["prioritized action items"]
}}

Return ONLY valid JSON."""

        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Check throttle before making API call
                estimated = throttle.estimate_tokens(prompt) + 1500  # Add max_tokens estimate
                throttle.wait_if_needed(estimated)

                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Record actual token usage
                input_tokens, output_tokens = throttle.get_usage_from_response(response)
                throttle.record_usage(input_tokens, output_tokens)
                
                try:
                    return json.loads(response.content[0].text)
                except json.JSONDecodeError:
                    return {
                        "month": month_key,
                        "executive_summary": response.content[0].text[:2000],
                        "raw_response": True
                    }
                    
            except Exception as e:
                error_str = str(e)
                if "rate_limit_error" in error_str and attempt < max_retries - 1:
                    wait_time = 65
                    print(f"  ⏸️  Rate limit hit on condense (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    throttle._reset_window()
                    continue
                else:
                    return {
                        "month": month_key,
                        "executive_summary": f"Condensation failed: {error_str}",
                        "error": error_str,
                        "raw_response": True
                    }
    
    def _update_historical_context(self, throttle: TokenThrottleManager):
        """Update the historical context document with all archived data."""
        client = anthropic.Anthropic()
        
        # Load all monthly archives
        archives = self.list_monthly_archives()
        archive_data = []
        
        for archive_key in archives[-12:]:  # Last 12 months
            try:
                response = self.s3.get_object(Bucket=self.bucket, Key=archive_key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                archive_data.append(data)
            except:
                continue
        
        if not archive_data:
            return
        
        prompt = f"""Based on the following monthly SEO archive summaries, create a comprehensive historical context document.

MONTHLY ARCHIVES:
{json.dumps(archive_data, indent=2, default=str)[:50000]}

Create a JSON response with:
{{
    "last_updated": "{datetime.utcnow().isoformat()}",
    "site_overview": "Current understanding of the site's SEO status",
    "long_term_trends": {{
        "technical_seo": "trend analysis",
        "content_performance": "trend analysis",
        "backlink_growth": "trend analysis",
        "competitive_position": "trend analysis"
    }},
    "historical_timeline": [
        {{"period": "YYYY/MM", "event": "significant event or change"}}
    ],
    "recurring_patterns": ["patterns observed over time"],
    "successful_strategies": ["what has worked well"],
    "persistent_challenges": ["ongoing issues to address"],
    "strategic_recommendations": ["long-term SEO strategy suggestions"]
}}

Return ONLY valid JSON."""

        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Check throttle before making API call
                estimated = throttle.estimate_tokens(prompt) + 1500  # Add max_tokens estimate
                throttle.wait_if_needed(estimated)

                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Record actual token usage
                input_tokens, output_tokens = throttle.get_usage_from_response(response)
                throttle.record_usage(input_tokens, output_tokens)
                
                try:
                    context = json.loads(response.content[0].text)
                except json.JSONDecodeError:
                    context = {
                        "last_updated": datetime.utcnow().isoformat(),
                        "site_overview": response.content[0].text[:2000],
                        "raw_response": True
                    }
                
                self.save_historical_context(context)
                return
                
            except Exception as e:
                error_str = str(e)
                if "rate_limit_error" in error_str and attempt < max_retries - 1:
                    wait_time = 65
                    print(f"  ⏸️  Rate limit hit on context update (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    throttle._reset_window()
                    continue
                else:
                    print(f"  ❌ Failed to update historical context: {error_str}")
                    return


class SEOResearchAgent:
    """Main SEO research agent that performs autonomous website analysis."""
    
    def __init__(self, target_website: str, storage: SEOStorage):
        self.target_website = target_website
        self.storage = storage
        self.client = anthropic.Anthropic()
        self.throttle = TokenThrottleManager(
            tokens_per_minute=TOKENS_PER_MINUTE_LIMIT,
            safety_margin=SAFETY_MARGIN
        )
        
        # Extract domain for queries
        self.domain = target_website.replace("https://", "").replace("http://", "").split("/")[0]
    
    def run_analysis_cycle(self, force_full: bool = False, categories: Optional[list] = None) -> dict:
        """Run a complete SEO analysis cycle."""
        print(f"Starting SEO analysis for {self.target_website}")
        
        # Load prior context
        prior_context = self._load_prior_context()
        
        # Determine which categories to analyze
        categories_to_analyze = categories or [c["id"] for c in SEO_ANALYSIS_CATEGORIES]
        
        # Run analysis for each category
        findings = {
            "target_website": self.target_website,
            "analysis_date": datetime.utcnow().isoformat(),
            "categories": {},
            "overall_score": None,
            "priority_issues": [],
            "quick_wins": [],
            "strategic_recommendations": []
        }
        
        for category in SEO_ANALYSIS_CATEGORIES:
            if category["id"] not in categories_to_analyze:
                continue
            
            print(f"Analyzing: {category['name']}")
            
            # Quick scan first (unless forced full or high importance)
            if not force_full and category["importance"] != "high":
                scan_result = self._quick_scan(category)
                if not scan_result.get("has_new_insights", True):
                    print(f"  Skipping {category['name']} - no significant changes detected")
                    findings["categories"][category["id"]] = {
                        "name": category["name"],
                        "status": "skipped",
                        "reason": "No significant changes detected"
                    }
                    continue
            
            # Full analysis
            analysis = self._analyze_category(category, prior_context)
            findings["categories"][category["id"]] = analysis
        
        # Generate overall assessment
        overall = self._generate_overall_assessment(findings, prior_context)
        findings["overall_score"] = overall.get("score")
        findings["priority_issues"] = overall.get("priority_issues", [])
        findings["quick_wins"] = overall.get("quick_wins", [])
        findings["strategic_recommendations"] = overall.get("strategic_recommendations", [])
        findings["executive_summary"] = overall.get("executive_summary", "")
        
        # Save findings
        self.storage.save_findings(findings)
        
        # Update summary
        summary = self._generate_summary(findings, prior_context)
        self.storage.save_summary(summary)
        
        print("SEO analysis cycle complete")
        return findings
    
    def _load_prior_context(self) -> dict:
        """Load historical context and recent findings."""
        context = {
            "historical": self.storage.load_historical_context(),
            "latest_summary": self.storage.load_summary(),
            "recent_findings": self.storage.load_recent_findings(days=7)
        }
        return context
    
    def _quick_scan(self, category: dict) -> dict:
        """Quick scan to check if full analysis is needed."""
        query = category["queries"][0].format(site=self.domain)
        
        prompt = f"""Quick scan for SEO changes on {self.domain} related to {category['name']}.
                
Search query: {query}

Respond with JSON:
{{"has_new_insights": true/false, "reason": "brief explanation"}}"""
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Check throttle before making API call
                estimated = self.throttle.estimate_tokens(prompt) + 500  # Add max_tokens estimate
                self.throttle.wait_if_needed(estimated)
                
                response = self.client.messages.create(
                    model="claude-haiku-4-20250514",
                    max_tokens=500,
                    tools=[{
                        "type": "web_search_20250305",
                        "name": "web_search"
                    }],
                    messages=[{
                        "role": "user",
                        "content": prompt
                    }]
                )
                
                # Record actual token usage
                input_tokens, output_tokens = self.throttle.get_usage_from_response(response)
                self.throttle.record_usage(input_tokens, output_tokens)
                
                # Extract text from response
                text = ""
                for block in response.content:
                    if hasattr(block, 'text'):
                        text += block.text
                
                return json.loads(text)
                
            except Exception as e:
                error_str = str(e)
                if "rate_limit_error" in error_str and attempt < max_retries - 1:
                    wait_time = 65  # Wait for rate limit window to reset
                    print(f"  ⏸️  Rate limit hit (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    self.throttle._reset_window()
                    continue
                else:
                    return {"has_new_insights": True, "reason": f"Scan failed: {error_str}, proceeding with full analysis"}
    
    def _analyze_category(self, category: dict, prior_context: dict) -> dict:
        """Perform full SEO analysis for a category."""
        
        # Build context from prior findings
        context_str = ""
        if prior_context.get("latest_summary"):
            prev = prior_context["latest_summary"].get("categories", {}).get(category["id"])
            if prev:
                context_str = f"\n\nPREVIOUS ANALYSIS:\n{json.dumps(prev, indent=2)}"
        
        queries = [q.format(site=self.domain) for q in category["queries"]]
        
        prompt = f"""Perform a comprehensive SEO analysis for {self.target_website} focusing on: {category['name']}

Category Description: {category['description']}

Search using these queries to gather current data:
{json.dumps(queries, indent=2)}
{context_str}

After searching, provide a detailed JSON analysis:
{{
    "name": "{category['name']}",
    "status": "good/warning/critical",
    "score": 1-100,
    "current_state": "detailed description of current SEO state for this category",
    "strengths": ["list of SEO strengths"],
    "weaknesses": ["list of SEO weaknesses"],
    "issues": [
        {{
            "severity": "high/medium/low",
            "issue": "description",
            "impact": "business impact",
            "recommendation": "how to fix"
        }}
    ],
    "opportunities": ["improvement opportunities"],
    "changes_since_last": "what changed since previous analysis (if available)",
    "data_sources": ["sources used for this analysis"]
}}

Return ONLY valid JSON."""

        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Check throttle before making API call
                estimated = self.throttle.estimate_tokens(prompt) + 4000  # Add max_tokens estimate
                self.throttle.wait_if_needed(estimated)

                response = self.client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=4000,
                    tools=[{
                        "type": "web_search_20250305",
                        "name": "web_search"
                    }],
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Record actual token usage
                input_tokens, output_tokens = self.throttle.get_usage_from_response(response)
                self.throttle.record_usage(input_tokens, output_tokens)
                
                # Extract text from response
                text = ""
                for block in response.content:
                    if hasattr(block, 'text'):
                        text += block.text
                
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {
                        "name": category["name"],
                        "status": "unknown",
                        "raw_analysis": text[:3000],
                        "parse_error": True
                    }
                    
            except Exception as e:
                error_str = str(e)
                if "rate_limit_error" in error_str and attempt < max_retries - 1:
                    wait_time = 65
                    print(f"  ⏸️  Rate limit hit on analysis (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    self.throttle._reset_window()
                    continue
                else:
                    return {
                        "name": category["name"],
                        "status": "error",
                        "error": error_str,
                        "parse_error": True
                    }
    
    def _generate_overall_assessment(self, findings: dict, prior_context: dict) -> dict:
        """Generate overall SEO health assessment."""
        
        prompt = f"""Based on the following SEO analysis findings for {self.target_website}, generate an overall assessment.

FINDINGS:
{json.dumps(findings["categories"], indent=2, default=str)[:30000]}

PRIOR CONTEXT:
{json.dumps(prior_context.get("latest_summary", {}), indent=2, default=str)[:10000]}

Generate a JSON response:
{{
    "score": 1-100,
    "grade": "A/B/C/D/F",
    "trend": "improving/stable/declining",
    "executive_summary": "2-3 paragraph executive summary for business stakeholders",
    "priority_issues": [
        {{
            "rank": 1,
            "category": "category name",
            "issue": "issue description",
            "business_impact": "impact on business",
            "effort": "low/medium/high",
            "recommendation": "specific action to take"
        }}
    ],
    "quick_wins": [
        {{
            "action": "quick action that can be taken",
            "expected_impact": "expected improvement",
            "effort": "low"
        }}
    ],
    "strategic_recommendations": [
        "Long-term strategic SEO recommendations"
    ],
    "kpis_to_track": [
        {{
            "metric": "metric name",
            "current_estimate": "current value if known",
            "target": "recommended target"
        }}
    ]
}}

Return ONLY valid JSON."""

        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Check throttle before making API call
                estimated = self.throttle.estimate_tokens(prompt) + 4000  # Add max_tokens estimate
                self.throttle.wait_if_needed(estimated)

                response = self.client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=4000,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Record actual token usage
                input_tokens, output_tokens = self.throttle.get_usage_from_response(response)
                self.throttle.record_usage(input_tokens, output_tokens)
                
                try:
                    return json.loads(response.content[0].text)
                except json.JSONDecodeError:
                    return {
                        "score": None,
                        "executive_summary": response.content[0].text[:2000],
                        "parse_error": True
                    }
                    
            except Exception as e:
                error_str = str(e)
                if "rate_limit_error" in error_str and attempt < max_retries - 1:
                    wait_time = 65
                    print(f"  ⏸️  Rate limit hit on assessment (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    self.throttle._reset_window()
                    continue
                else:
                    return {
                        "score": None,
                        "executive_summary": f"Assessment generation failed: {error_str}",
                        "error": error_str,
                        "parse_error": True
                    }
    
    def _generate_summary(self, findings: dict, prior_context: dict) -> dict:
        """Generate a summary for the dashboard."""
        
        # Calculate category scores
        category_scores = {}
        for cat_id, cat_data in findings.get("categories", {}).items():
            if isinstance(cat_data, dict) and "score" in cat_data:
                category_scores[cat_id] = cat_data["score"]
        
        summary = {
            "generated_at": datetime.utcnow().isoformat(),
            "target_website": self.target_website,
            "overall_score": findings.get("overall_score"),
            "executive_summary": findings.get("executive_summary", ""),
            "category_scores": category_scores,
            "priority_issues": findings.get("priority_issues", [])[:5],
            "quick_wins": findings.get("quick_wins", [])[:3],
            "strategic_recommendations": findings.get("strategic_recommendations", [])[:5],
            "categories": {}
        }
        
        # Add category summaries
        for cat_id, cat_data in findings.get("categories", {}).items():
            if isinstance(cat_data, dict):
                summary["categories"][cat_id] = {
                    "name": cat_data.get("name", cat_id),
                    "status": cat_data.get("status", "unknown"),
                    "score": cat_data.get("score"),
                    "issue_count": len(cat_data.get("issues", []))
                }
        
        return summary


def lambda_handler(event, context):
    """AWS Lambda entry point."""
    
    mode = event.get("mode", "analyze")
    force_full = event.get("force_full", False)
    categories = event.get("categories")
    delete_after_archive = event.get("delete_after_archive", False)
    
    # Get target websites - from event or environment
    target_websites = event.get("target_websites")
    if not target_websites:
        # Check for single website in event (backwards compatibility)
        single_site = event.get("target_website")
        if single_site:
            target_websites = [single_site]
        else:
            target_websites = TARGET_WEBSITES
    
    results = {
        "mode": mode,
        "websites_processed": [],
        "errors": [],
        "throttle_config": {
            "tokens_per_minute_limit": TOKENS_PER_MINUTE_LIMIT,
            "safety_margin": SAFETY_MARGIN,
            "effective_limit": int(TOKENS_PER_MINUTE_LIMIT * SAFETY_MARGIN)
        }
    }
    
    # Collect throttle stats from all agents
    all_throttle_stats = []
    
    if mode == "archive":
        # Run archival cycle for all websites
        for idx, website in enumerate(target_websites):
            try:
                print(f"\n{'='*60}")
                print(f"📦 Starting archival for: {website}")
                print(f"   Website {idx + 1} of {len(target_websites)}")
                print(f"{'='*60}")
                
                storage = SEOStorage(S3_BUCKET, S3_PREFIX, website)
                archive_result = storage.run_archival_cycle(delete_after=delete_after_archive)
                results["websites_processed"].append({
                    "website": website,
                    "result": archive_result
                })
                
                print(f"\n✅ Completed archival for: {website}")
                
            except Exception as e:
                print(f"\n❌ Error archiving {website}: {str(e)}")
                results["errors"].append({
                    "website": website,
                    "error": str(e)
                })
        
        return {
            "statusCode": 200,
            "body": json.dumps(results)
        }
    
    elif mode == "analyze":
        # Run analysis cycle for each website
        for idx, website in enumerate(target_websites):
            try:
                print(f"\n{'='*60}")
                print(f"🔍 Starting analysis for: {website}")
                print(f"   Website {idx + 1} of {len(target_websites)}")
                print(f"   Token limit: {int(TOKENS_PER_MINUTE_LIMIT * SAFETY_MARGIN)}/min (85% of {TOKENS_PER_MINUTE_LIMIT})")
                print(f"{'='*60}")
                
                storage = SEOStorage(S3_BUCKET, S3_PREFIX, website)
                agent = SEOResearchAgent(website, storage)
                findings = agent.run_analysis_cycle(
                    force_full=force_full,
                    categories=categories
                )
                
                # Collect throttle stats from this agent
                throttle_stats = agent.throttle.get_stats()
                all_throttle_stats.append({
                    "website": website,
                    "stats": throttle_stats
                })
                
                results["websites_processed"].append({
                    "website": website,
                    "overall_score": findings.get("overall_score"),
                    "categories_analyzed": list(findings.get("categories", {}).keys()),
                    "priority_issues_count": len(findings.get("priority_issues", [])),
                    "throttle_stats": throttle_stats
                })
                
                print(f"\n✅ Completed analysis for: {website}")
                print(f"   Token usage: {throttle_stats['tokens_used']}/{throttle_stats['tokens_limit']} ({throttle_stats['utilization']})")
                
            except Exception as e:
                print(f"\n❌ Error analyzing {website}: {str(e)}")
                results["errors"].append({
                    "website": website,
                    "error": str(e)
                })
        
        # Add combined throttle stats
        results["throttle_stats"] = all_throttle_stats
        
        return {
            "statusCode": 200,
            "body": json.dumps(results)
        }
    
    else:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": f"Unknown mode: {mode}",
                "valid_modes": ["analyze", "archive"]
            })
        }


def get_all_websites() -> List[str]:
    """Return list of all configured target websites."""
    return TARGET_WEBSITES


# For local testing
if __name__ == "__main__":
    import sys
    
    # Parse command line arguments
    mode = sys.argv[1] if len(sys.argv) > 1 else "analyze"
    
    # Can specify websites as additional arguments
    websites = sys.argv[2:] if len(sys.argv) > 2 else None
    
    event = {
        "mode": mode,
        "force_full": True
    }
    if websites:
        event["target_websites"] = websites
    
    result = lambda_handler(event, None)
    print(json.dumps(json.loads(result["body"]), indent=2))
