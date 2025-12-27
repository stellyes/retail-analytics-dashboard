"""
Cannabis Industry Research Agent
Autonomous agent that monitors industry trends, news, and regulatory changes.
Designed to run on AWS Lambda with EventBridge scheduling.
"""

import json
import os
import boto3
from datetime import datetime, timedelta
from typing import Optional
import hashlib

# Anthropic client
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


# =============================================================================
# CONFIGURATION
# =============================================================================

# Research topics to monitor
# NOTE: To avoid rate limits, topics rotate - only 1-2 researched per cycle
RESEARCH_TOPICS = [
    {
        "id": "regulatory",
        "name": "Regulatory Updates",
        "queries": [
            "cannabis regulation California 2025",
            "marijuana dispensary license California",
        ],
        "importance": "high"
    },
    {
        "id": "market_trends",
        "name": "Market Trends",
        "queries": [
            "cannabis retail trends 2025",
            "marijuana consumer preferences",
        ],
        "importance": "medium"
    },
    {
        "id": "competition",
        "name": "Competitive Landscape",
        "queries": [
            "San Francisco cannabis dispensary",
            "Bay Area cannabis retail",
        ],
        "importance": "medium"
    },
    {
        "id": "products",
        "name": "Product Innovation",
        "queries": [
            "new cannabis products 2025",
            "cannabis brands California",
        ],
        "importance": "low"
    },
    {
        "id": "pricing",
        "name": "Pricing & Economics",
        "queries": [
            "cannabis prices California",
            "marijuana tax California",
        ],
        "importance": "high"
    }
]

# S3 Configuration
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "retail-data-bcgr")
S3_PREFIX = "research-findings/"
MAX_HISTORY_DAYS = 30  # How many days of daily findings to keep before condensing


# =============================================================================
# S3 STORAGE MANAGER
# =============================================================================

class ResearchStorage:
    """Manages research findings storage in S3."""
    
    def __init__(self, bucket_name: str, prefix: str = "research-findings/"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3 = boto3.client('s3')
    
    def _get_date_key(self, date: datetime = None) -> str:
        """Generate S3 key for a specific date."""
        date = date or datetime.utcnow()
        return f"{self.prefix}{date.strftime('%Y/%m/%d')}/findings.json"
    
    def _get_summary_key(self) -> str:
        """Get key for the running summary file."""
        return f"{self.prefix}summary/latest.json"
    
    def _get_history_key(self) -> str:
        """Get key for the historical summary file."""
        return f"{self.prefix}summary/history.json"
    
    def _get_monthly_archive_key(self, year: int, month: int) -> str:
        """Get key for a monthly archive file."""
        return f"{self.prefix}archive/{year}/{month:02d}/monthly-summary.json"
    
    def _get_historical_context_key(self) -> str:
        """Get key for the rolling historical context document."""
        return f"{self.prefix}archive/historical-context.json"
    
    def save_findings(self, findings: dict) -> bool:
        """Save today's research findings."""
        try:
            key = self._get_date_key()
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(findings, indent=2, default=str),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            print(f"Error saving findings: {e}")
            return False
    
    def load_findings(self, date: datetime = None) -> Optional[dict]:
        """Load findings for a specific date."""
        try:
            key = self._get_date_key(date)
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error loading findings: {e}")
            return None
    
    def load_recent_findings(self, days: int = 7) -> list:
        """Load findings from the past N days."""
        findings = []
        for i in range(days):
            date = datetime.utcnow() - timedelta(days=i)
            result = self.load_findings(date)
            if result:
                findings.append(result)
        return findings
    
    def save_summary(self, summary: dict) -> bool:
        """Save the latest cumulative summary."""
        try:
            # Save as latest
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=self._get_summary_key(),
                Body=json.dumps(summary, indent=2, default=str),
                ContentType='application/json'
            )
            
            # Also append to history
            history = self.load_history() or []
            history.append({
                "timestamp": datetime.utcnow().isoformat(),
                "summary": summary.get("executive_summary", ""),
                "key_items": summary.get("key_findings", [])[:5]
            })
            
            # Keep only last 30 days of history
            history = history[-MAX_HISTORY_DAYS:]
            
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=self._get_history_key(),
                Body=json.dumps(history, indent=2, default=str),
                ContentType='application/json'
            )
            
            return True
        except Exception as e:
            print(f"Error saving summary: {e}")
            return False
    
    def load_summary(self) -> Optional[dict]:
        """Load the latest cumulative summary."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name,
                Key=self._get_summary_key()
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error loading summary: {e}")
            return None
    
    def load_history(self) -> Optional[list]:
        """Load the historical summary list."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name,
                Key=self._get_history_key()
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3.exceptions.NoSuchKey:
            return []
        except Exception as e:
            print(f"Error loading history: {e}")
            return []
    
    def list_all_findings(self) -> list:
        """List all finding files in S3."""
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            files = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('findings.json'):
                        files.append({
                            'key': obj['Key'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'size': obj['Size']
                        })
            return files
        except Exception as e:
            print(f"Error listing findings: {e}")
            return []
    
    # =========================================================================
    # ARCHIVAL & CONDENSATION METHODS
    # =========================================================================
    
    def list_findings_for_month(self, year: int, month: int) -> list:
        """List all daily findings for a specific month."""
        prefix = f"{self.prefix}{year}/{month:02d}/"
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            files = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('findings.json'):
                        # Extract day from path
                        parts = obj['Key'].split('/')
                        day = int(parts[-2]) if len(parts) >= 2 else 0
                        files.append({
                            'key': obj['Key'],
                            'date': f"{year}-{month:02d}-{day:02d}",
                            'last_modified': obj['LastModified']
                        })
            return sorted(files, key=lambda x: x['date'])
        except Exception as e:
            print(f"Error listing monthly findings: {e}")
            return []
    
    def load_findings_by_key(self, key: str) -> Optional[dict]:
        """Load findings from a specific S3 key."""
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            print(f"Error loading {key}: {e}")
            return None
    
    def save_monthly_archive(self, year: int, month: int, archive: dict) -> bool:
        """Save a monthly archive document."""
        try:
            key = self._get_monthly_archive_key(year, month)
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(archive, indent=2, default=str),
                ContentType='application/json'
            )
            print(f"Saved monthly archive: {key}")
            return True
        except Exception as e:
            print(f"Error saving monthly archive: {e}")
            return False
    
    def load_monthly_archive(self, year: int, month: int) -> Optional[dict]:
        """Load a monthly archive document."""
        try:
            key = self._get_monthly_archive_key(year, month)
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error loading monthly archive: {e}")
            return None
    
    def list_monthly_archives(self) -> list:
        """List all available monthly archives."""
        prefix = f"{self.prefix}archive/"
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            archives = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('monthly-summary.json'):
                        # Extract year/month from path
                        parts = obj['Key'].split('/')
                        if len(parts) >= 4:
                            year = int(parts[-3])
                            month = int(parts[-2])
                            archives.append({
                                'key': obj['Key'],
                                'year': year,
                                'month': month,
                                'period': f"{year}-{month:02d}"
                            })
            return sorted(archives, key=lambda x: x['period'])
        except Exception as e:
            print(f"Error listing archives: {e}")
            return []
    
    def save_historical_context(self, context: dict) -> bool:
        """Save the rolling historical context document."""
        try:
            key = self._get_historical_context_key()
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(context, indent=2, default=str),
                ContentType='application/json'
            )
            print(f"Saved historical context: {key}")
            return True
        except Exception as e:
            print(f"Error saving historical context: {e}")
            return False
    
    def load_historical_context(self) -> Optional[dict]:
        """Load the rolling historical context document."""
        try:
            key = self._get_historical_context_key()
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error loading historical context: {e}")
            return None
    
    def delete_daily_findings(self, keys: list) -> int:
        """Delete daily findings files after archival. Returns count deleted."""
        deleted = 0
        for key in keys:
            try:
                self.s3.delete_object(Bucket=self.bucket_name, Key=key)
                deleted += 1
            except Exception as e:
                print(f"Error deleting {key}: {e}")
        return deleted
    
    def get_months_to_archive(self, min_age_days: int = 30) -> list:
        """Get list of months that are old enough to archive."""
        cutoff = datetime.utcnow() - timedelta(days=min_age_days)
        
        # List all daily findings
        all_findings = self.list_all_findings()
        
        # Group by year/month
        months = {}
        for f in all_findings:
            key = f['key']
            # Parse date from key: research-findings/2024/01/15/findings.json
            parts = key.replace(self.prefix, '').split('/')
            if len(parts) >= 3:
                try:
                    year = int(parts[0])
                    month = int(parts[1])
                    day = int(parts[2])
                    date = datetime(year, month, day)
                    
                    # Only include if older than cutoff
                    if date < cutoff:
                        period = f"{year}-{month:02d}"
                        if period not in months:
                            months[period] = {'year': year, 'month': month, 'files': []}
                        months[period]['files'].append(key)
                except (ValueError, IndexError):
                    continue
        
        # Check which months don't already have archives
        existing_archives = {a['period'] for a in self.list_monthly_archives()}
        
        result = []
        for period, data in months.items():
            if period not in existing_archives:
                result.append(data)
        
        return sorted(result, key=lambda x: f"{x['year']}-{x['month']:02d}")


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
            wait_time = max(60 - elapsed, 0)

            if wait_time > 0:
                print(f"  ⏸️  Rate limit approaching ({self.tokens_used}/{self.max_tokens} tokens used)")
                print(f"  ⏸️  Pausing for {wait_time:.1f} seconds to reset rate limit window...")
                import time
                time.sleep(wait_time)
                self._reset_window()
                return wait_time

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


# =============================================================================
# RESEARCH AGENT
# =============================================================================

class IndustryResearchAgent:
    """AI-powered research agent for cannabis industry monitoring."""

    def __init__(self, api_key: str, storage: ResearchStorage):
        if not ANTHROPIC_AVAILABLE:
            raise ImportError("anthropic package not installed")

        self.client = anthropic.Anthropic(api_key=api_key)
        self.storage = storage
        self.model = "claude-sonnet-4-20250514"
        self.throttle = TokenThrottleManager(tokens_per_minute=30000, safety_margin=0.85)
    
    def _generate_finding_id(self, content: str) -> str:
        """Generate a unique ID for a finding to detect duplicates."""
        return hashlib.md5(content[:200].encode()).hexdigest()[:12]
    
    def quick_scan_topic(self, topic: dict, known_findings: list = None) -> dict:
        """
        Do a quick, cheap scan to check if there's new content worth researching.
        Uses Haiku for cost efficiency (~$0.001 per scan vs $0.05 for full research).
        Returns: {"has_new_content": bool, "signals": [...], "skip_reason": str|None}
        """
        known_findings = known_findings or []
        
        # Build a condensed prompt for quick scanning
        primary_query = topic["queries"][0] if topic["queries"] else topic["name"]
        
        # Create a fingerprint of what we already know
        known_summary = ""
        if known_findings:
            known_items = [f.get('title', '')[:50] for f in known_findings[:5]]
            known_summary = f"\n\nWe already know about: {', '.join(known_items)}"
        
        prompt = f"""Quick scan for NEW developments only.
Topic: {topic["name"]}
Search: {primary_query}{known_summary}

Search and respond with ONLY a JSON object:
{{
    "has_new_content": true/false,
    "signals": ["brief description of new item 1", "new item 2"],
    "newest_date": "YYYY-MM-DD or null if nothing recent",
    "skip_reason": "why no new content" or null
}}

Only mark has_new_content=true if there are developments from the past 7 days that we don't already know about. Be conservative - skip if uncertain."""

        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Check throttle before making API call
                estimated = self.throttle.estimate_tokens(prompt)
                self.throttle.wait_if_needed(estimated)

                response = self.client.messages.create(
                    model="claude-haiku-4-5-20251001",  # Use Haiku for cheap quick scan
                    max_tokens=300,
                    tools=[{
                        "type": "web_search_20250305",
                        "name": "web_search"
                    }],
                    messages=[{"role": "user", "content": prompt}]
                )

                # Record actual token usage
                input_tokens, output_tokens = self.throttle.get_usage_from_response(response)
                self.throttle.record_usage(input_tokens, output_tokens)

                result_text = ""
                for block in response.content:
                    if hasattr(block, 'text'):
                        result_text += block.text

                # Parse JSON response
                if "```json" in result_text:
                    result_text = result_text.split("```json")[1].split("```")[0]
                elif "```" in result_text:
                    result_text = result_text.split("```")[1].split("```")[0]

                return json.loads(result_text.strip())

            except Exception as e:
                error_str = str(e)
                if "rate_limit_error" in error_str and attempt < max_retries - 1:
                    wait_time = 65  # Wait for rate limit window to reset
                    print(f"  ⏸️  Rate limit hit (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
                    import time
                    time.sleep(wait_time)
                    self.throttle._reset_window()  # Reset our tracking
                    continue
                else:
                    # On final error or non-rate-limit error, default to doing full research
                    return {
                        "has_new_content": True,
                        "signals": ["scan_failed"],
                        "skip_reason": None,
                        "error": error_str
                    }
    
    def research_topic(self, topic: dict, skip_scan: bool = False, known_findings: list = None) -> dict:
        """Research a single topic using Claude with web search."""
        
        topic_findings = {
            "topic_id": topic["id"],
            "topic_name": topic["name"],
            "importance": topic["importance"],
            "researched_at": datetime.utcnow().isoformat(),
            "findings": [],
            "raw_sources": [],
            "skipped": False,
            "skip_reason": None
        }
        
        # Quick scan first (unless skipped or high importance - always research high importance)
        if not skip_scan and topic["importance"] != "high":
            scan_result = self.quick_scan_topic(topic, known_findings)
            
            if not scan_result.get("has_new_content", True):
                topic_findings["skipped"] = True
                topic_findings["skip_reason"] = scan_result.get("skip_reason", "No new content detected")
                topic_findings["summary"] = f"Skipped: {topic_findings['skip_reason']}"
                topic_findings["scan_result"] = scan_result
                return topic_findings
            else:
                # Store signals for context in full research
                topic_findings["new_signals"] = scan_result.get("signals", [])
        
        # Build research prompt
        queries_str = "\n".join(f"- {q}" for q in topic["queries"])
        
        # Add signals from scan if available
        signals_context = ""
        if topic_findings.get("new_signals"):
            signals_context = f"\n\nPreliminary scan found these new developments to investigate:\n- " + "\n- ".join(topic_findings["new_signals"])
        
        prompt = f"""Research cannabis industry trends for SF dispensary.

Topic: {topic["name"]}
Queries: {queries_str}{signals_context}

Search for recent info (past 7 days preferred). Focus on:
- Regulatory changes
- Market/consumer trends
- Competition
- Business impact

For each finding:
- 2-3 sentence summary
- Source & date
- Relevance (high/med/low)
- Actions needed

Be specific with dates, numbers, sources."""

        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Check throttle before making API call
                estimated = self.throttle.estimate_tokens(prompt) + 2000  # Add max_tokens estimate
                self.throttle.wait_if_needed(estimated)

                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=2000,
                    tools=[{
                        "type": "web_search_20250305",
                        "name": "web_search"
                    }],
                    messages=[{"role": "user", "content": prompt}]
                )

                # Record actual token usage
                input_tokens, output_tokens = self.throttle.get_usage_from_response(response)
                self.throttle.record_usage(input_tokens, output_tokens)

                # Extract the research results
                research_text = ""
                for block in response.content:
                    if hasattr(block, 'text'):
                        research_text += block.text

                topic_findings["raw_response"] = research_text

                # Extract summary and findings directly from response (avoid extra API call)
                # This prevents rate limit errors from the parsing step
                topic_findings["summary"] = self._extract_summary(research_text)
                topic_findings["findings"] = self._extract_findings_simple(research_text)
                break  # Success, exit retry loop

            except Exception as e:
                error_str = str(e)
                if "rate_limit_error" in error_str and attempt < max_retries - 1:
                    wait_time = 65
                    print(f"  ⏸️  Rate limit hit on research (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
                    import time
                    time.sleep(wait_time)
                    self.throttle._reset_window()
                    continue
                else:
                    topic_findings["error"] = error_str
                    topic_findings["summary"] = f"Research failed: {e}"
                    break
        
        return topic_findings
    
    def _extract_summary(self, text: str) -> str:
        """Extract a simple summary from research text without API call."""
        # Look for executive summary sections
        lines = text.split('\n')
        summary_lines = []

        for i, line in enumerate(lines):
            if any(keyword in line.lower() for keyword in ['summary:', 'key findings:', 'overview:', '##']):
                # Get next 3-5 lines as summary
                summary_lines = lines[i+1:i+4]
                break

        if summary_lines:
            summary = ' '.join(summary_lines).strip()
            return summary[:500] if len(summary) > 500 else summary

        # Fallback: first 300 chars
        return text[:300].strip() + "..."

    def _extract_findings_simple(self, text: str) -> list:
        """Extract findings from structured text without API call."""
        findings = []
        lines = text.split('\n')

        current_finding = {}
        for line in lines:
            line = line.strip()

            # Look for numbered findings or bold headers
            if line.startswith('**') and '**' in line[2:]:
                # Save previous finding
                if current_finding:
                    findings.append(current_finding)

                # Start new finding
                title = line.replace('**', '').strip()
                current_finding = {
                    "title": title[:100],
                    "description": "",
                    "source": "Web search",
                    "date": "2025",
                    "relevance": "medium",
                    "action_required": False,
                    "recommended_action": ""
                }
            elif current_finding and line and not line.startswith('#'):
                # Add to description
                if current_finding["description"]:
                    current_finding["description"] += " " + line
                else:
                    current_finding["description"] = line

                # Extract relevance if mentioned
                if 'high relevance' in line.lower():
                    current_finding["relevance"] = "high"
                elif 'low relevance' in line.lower():
                    current_finding["relevance"] = "low"

                # Limit description length
                if len(current_finding["description"]) > 300:
                    current_finding["description"] = current_finding["description"][:300] + "..."

        # Add last finding
        if current_finding:
            findings.append(current_finding)

        return findings[:10]  # Limit to 10 findings

    def _parse_findings(self, topic: dict, raw_text: str) -> dict:
        """Parse raw research text into structured findings."""
        
        prompt = f"""Extract structured findings from research.

Topic: {topic["name"]}
Raw Output: {raw_text[:2000]}

Return JSON:
{{
    "summary": "2-3 sentence key findings",
    "findings": [
        {{
            "title": "Brief title",
            "description": "2-3 sentences",
            "source": "Source",
            "date": "YYYY-MM-DD",
            "relevance": "high/medium/low",
            "action_required": true/false,
            "recommended_action": "Action if any"
        }}
    ]
}}

ONLY valid JSON."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            result_text = response.content[0].text
            
            # Clean up potential markdown formatting
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]
            
            return json.loads(result_text.strip())
            
        except Exception as e:
            return {
                "summary": f"Failed to parse findings: {e}",
                "findings": []
            }
    
    def load_prior_context(self) -> str:
        """Load prior research findings and historical context to provide context."""
        
        context_parts = []
        
        # Load historical context (long-term memory)
        historical = self.storage.load_historical_context()
        if historical:
            context_parts.append(f"""
HISTORICAL CONTEXT (Industry Background):
Industry State: {historical.get('industry_overview', {}).get('current_state', 'Unknown')}
Trajectory: {historical.get('industry_overview', {}).get('trajectory', 'Unknown')}

Long-term Trends:
- Regulatory: {historical.get('long_term_trends', {}).get('regulatory', {}).get('summary', 'No data')}
- Market: {historical.get('long_term_trends', {}).get('market', {}).get('summary', 'No data')}
- Pricing: {historical.get('long_term_trends', {}).get('pricing', {}).get('summary', 'No data')}

Ongoing Stories:
{json.dumps(historical.get('ongoing_stories', [])[:3], indent=2)}
""")
        
        # Load the latest summary (recent memory)
        summary = self.storage.load_summary()
        if summary:
            context_parts.append(f"""
LATEST RESEARCH SUMMARY (from {summary.get('generated_at', 'unknown')}):
{summary.get('executive_summary', 'No summary available')}

Key ongoing items to track:
{json.dumps(summary.get('tracking_items', []), indent=2)}
""")
        
        # Load recent history (short-term memory)
        history = self.storage.load_history() or []
        recent_history = history[-7:] if history else []
        
        if recent_history:
            context_parts.append("\nRECENT RESEARCH HISTORY (last 7 entries):")
            for entry in recent_history[-5:]:
                context_parts.append(f"- {entry.get('timestamp', 'Unknown')}: {entry.get('summary', 'No summary')[:200]}")
        
        return "\n".join(context_parts) if context_parts else "No prior research context available."
    
    def generate_cumulative_summary(self, new_findings: dict, prior_summary: dict = None) -> dict:
        """Generate an updated cumulative summary incorporating new findings."""
        
        prior_context = ""
        if prior_summary:
            prior_context = f"""
PRIOR SUMMARY:
{prior_summary.get('executive_summary', 'None')}

Previously tracked items:
{json.dumps(prior_summary.get('tracking_items', []), indent=2)}
"""
        
        # Compile new findings
        new_findings_text = ""
        for topic_result in new_findings.get("topics", []):
            new_findings_text += f"\n\n### {topic_result['topic_name']}\n"
            new_findings_text += topic_result.get("summary", "No summary")
            for finding in topic_result.get("findings", [])[:3]:
                new_findings_text += f"\n- {finding.get('title', 'Untitled')}: {finding.get('description', '')[:200]}"
        
        prompt = f"""You are maintaining a cumulative research summary for a cannabis dispensary.

{prior_context}

NEW FINDINGS FROM TODAY'S RESEARCH:
{new_findings_text}

Generate an updated summary JSON with:
{{
    "executive_summary": "3-5 sentence overview of the most important current trends and developments",
    "key_findings": [
        {{
            "category": "regulatory/market/competition/products/pricing",
            "finding": "Brief description",
            "importance": "high/medium/low",
            "first_identified": "YYYY-MM-DD",
            "status": "new/ongoing/resolved"
        }}
    ],
    "tracking_items": [
        {{
            "item": "What to track",
            "reason": "Why it matters",
            "next_check": "When to follow up"
        }}
    ],
    "action_items": [
        {{
            "action": "Specific action to take",
            "priority": "high/medium/low",
            "deadline": "YYYY-MM-DD or 'ongoing'"
        }}
    ],
    "generated_at": "{datetime.utcnow().isoformat()}"
}}

Merge new findings with prior context. Remove resolved items. Prioritize actionable insights.
Return ONLY valid JSON."""

        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Check throttle before making API call
                estimated = self.throttle.estimate_tokens(prompt) + 2000
                self.throttle.wait_if_needed(estimated)

                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=2000,
                    messages=[{"role": "user", "content": prompt}]
                )

                # Record actual token usage
                input_tokens, output_tokens = self.throttle.get_usage_from_response(response)
                self.throttle.record_usage(input_tokens, output_tokens)

                result_text = response.content[0].text

                # Clean up potential markdown formatting
                if "```json" in result_text:
                    result_text = result_text.split("```json")[1].split("```")[0]
                elif "```" in result_text:
                    result_text = result_text.split("```")[1].split("```")[0]

                return json.loads(result_text.strip())

            except Exception as e:
                error_str = str(e)
                if "rate_limit_error" in error_str and attempt < max_retries - 1:
                    wait_time = 65
                    print(f"  ⏸️  Rate limit hit on summary (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
                    import time
                    time.sleep(wait_time)
                    self.throttle._reset_window()
                    continue
                else:
                    return {
                        "executive_summary": f"Summary generation failed: {e}",
                        "key_findings": [],
                        "tracking_items": [],
                        "action_items": [],
                        "generated_at": datetime.utcnow().isoformat(),
                        "error": error_str
                    }
    
    def run_research_cycle(self, topics: list = None, force_full: bool = False, max_queries: int = 2) -> dict:
        """Run a complete research cycle with intelligent throttling.

        Args:
            topics: List of topics to research (default: all RESEARCH_TOPICS)
            force_full: Skip preliminary scans and do full research on all topics
            max_queries: Maximum queries to research per cycle (default: 2, with throttling and retry)
        """

        all_topics = topics or RESEARCH_TOPICS

        # Flatten all queries from all topics and shuffle for random selection
        import random
        from datetime import datetime

        all_query_items = []
        for topic in all_topics:
            for query in topic["queries"]:
                all_query_items.append({
                    "topic": topic,
                    "query": query
                })

        # Shuffle and pick up to max_queries
        random.shuffle(all_query_items)
        selected_items = all_query_items[:max_queries]

        # Group by topic to reduce redundant API calls
        topics_map = {}
        for item in selected_items:
            topic_id = item["topic"]["id"]
            if topic_id not in topics_map:
                topics_map[topic_id] = {
                    "id": item["topic"]["id"],
                    "name": item["topic"]["name"],
                    "queries": [],
                    "importance": item["topic"]["importance"]
                }
            topics_map[topic_id]["queries"].append(item["query"])

        topics = list(topics_map.values())

        print(f"Starting research cycle at {datetime.utcnow().isoformat()}")
        print(f"Researching {sum(len(t['queries']) for t in topics)} queries across {len(topics)} topics")
        print(f"Token throttling enabled: {self.throttle.max_tokens} tokens/min max")
        for topic in topics:
            print(f"  - {topic['name']}: {len(topic['queries'])} queries")

        # Add delay between topics to avoid rate limits (30k tokens/min)
        import time
        
        # Load prior context
        prior_context = self.load_prior_context()
        prior_summary = self.storage.load_summary()
        
        print(f"Prior context loaded: {len(prior_context)} characters")
        
        # Extract known findings by topic for comparison
        known_by_topic = {}
        if prior_summary:
            for finding in prior_summary.get("key_findings", []):
                cat = finding.get("category", "unknown")
                if cat not in known_by_topic:
                    known_by_topic[cat] = []
                known_by_topic[cat].append(finding)
        
        # Research each topic
        results = {
            "research_cycle_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
            "started_at": datetime.utcnow().isoformat(),
            "topics": [],
            "topics_skipped": 0,
            "topics_researched": 0,
            "errors": []
        }
        
        for idx, topic in enumerate(topics):
            print(f"Researching: {topic['name']}")
            try:
                # Get known findings for this topic's category
                known_findings = known_by_topic.get(topic["id"], [])

                topic_result = self.research_topic(
                    topic,
                    skip_scan=force_full,
                    known_findings=known_findings
                )
                results["topics"].append(topic_result)

                if topic_result.get("skipped"):
                    results["topics_skipped"] += 1
                    print(f"  ⏭️  Skipped: {topic_result.get('skip_reason', 'no new content')}")
                else:
                    results["topics_researched"] += 1
                    print(f"  ✅ Found {len(topic_result.get('findings', []))} findings")

            except Exception as e:
                error_msg = f"Error researching {topic['name']}: {e}"
                print(f"  ❌ {error_msg}")
                results["errors"].append(error_msg)
        
        results["completed_at"] = datetime.utcnow().isoformat()

        # Add throttle statistics
        throttle_stats = self.throttle.get_stats()
        results["throttle_stats"] = throttle_stats

        print(f"\nCycle summary: {results['topics_researched']} researched, {results['topics_skipped']} skipped")
        print(f"Token usage: {throttle_stats['tokens_used']}/{throttle_stats['tokens_limit']} ({throttle_stats['utilization']})")
        print(f"API calls: {throttle_stats['api_calls']} in {throttle_stats['window_elapsed']}")

        # Save raw findings
        self.storage.save_findings(results)
        print("Saved raw findings to S3")
        
        # Generate and save cumulative summary (with rate limit protection)
        try:
            summary = self.generate_cumulative_summary(results, prior_summary)
            self.storage.save_summary(summary)
            print("Saved cumulative summary to S3")
        except Exception as e:
            print(f"Summary generation skipped due to rate limits: {e}")
            summary = {
                "executive_summary": "Summary generation delayed due to rate limits. Raw findings saved to S3.",
                "generated_at": datetime.utcnow().isoformat()
            }
        
        results["summary"] = summary
        
        # Check if any months need archiving (runs monthly)
        months_to_archive = self.storage.get_months_to_archive(min_age_days=30)
        if months_to_archive:
            results["archival_pending"] = [f"{m['year']}-{m['month']:02d}" for m in months_to_archive]
            print(f"📦 {len(months_to_archive)} month(s) ready for archival: {results['archival_pending']}")
        
        return results
    
    # =========================================================================
    # ARCHIVAL & CONDENSATION METHODS
    # =========================================================================
    
    def condense_month(self, year: int, month: int, daily_findings: list) -> dict:
        """
        Condense a month's worth of daily findings into a single archive document.
        Uses Claude to synthesize patterns and key developments.
        """
        
        # Compile all findings from the month
        all_topics = {}
        all_key_findings = []
        research_dates = []
        
        for day_data in daily_findings:
            if not day_data:
                continue
                
            research_dates.append(day_data.get('started_at', 'unknown')[:10])
            
            for topic in day_data.get('topics', []):
                topic_id = topic.get('topic_id', 'unknown')
                
                if topic_id not in all_topics:
                    all_topics[topic_id] = {
                        'name': topic.get('topic_name', topic_id),
                        'findings': [],
                        'summaries': []
                    }
                
                all_topics[topic_id]['summaries'].append(topic.get('summary', ''))
                all_topics[topic_id]['findings'].extend(topic.get('findings', []))
        
        # Deduplicate findings by title similarity
        for topic_id in all_topics:
            unique_findings = []
            seen_titles = set()
            for f in all_topics[topic_id]['findings']:
                title = f.get('title', '')[:50].lower()
                if title and title not in seen_titles:
                    seen_titles.add(title)
                    unique_findings.append(f)
            all_topics[topic_id]['findings'] = unique_findings
        
        # Build prompt for Claude to synthesize
        month_name = datetime(year, month, 1).strftime('%B %Y')
        
        topics_text = ""
        for topic_id, topic_data in all_topics.items():
            topics_text += f"\n\n### {topic_data['name']}\n"
            topics_text += f"Daily summaries: {'; '.join(topic_data['summaries'][:10])}\n"
            topics_text += f"Key findings ({len(topic_data['findings'])} total):\n"
            for f in topic_data['findings'][:15]:
                topics_text += f"- {f.get('title', 'Untitled')}: {f.get('description', '')[:150]}\n"
        
        prompt = f"""You are creating a monthly archive summary for cannabis industry research.

Month: {month_name}
Research conducted on {len(research_dates)} days

DATA COLLECTED THIS MONTH:
{topics_text}

Create a comprehensive monthly archive document as JSON:
{{
    "period": "{year}-{month:02d}",
    "month_name": "{month_name}",
    "executive_summary": "3-5 sentence overview of the month's key developments",
    "category_summaries": {{
        "regulatory": "Summary of regulatory developments this month",
        "market_trends": "Summary of market trends observed",
        "competition": "Summary of competitive landscape changes",
        "products": "Summary of product/brand developments",
        "pricing": "Summary of pricing/economic factors"
    }},
    "major_developments": [
        {{
            "title": "Brief title",
            "description": "What happened and why it matters",
            "category": "regulatory/market/competition/products/pricing",
            "date_range": "When this occurred or was reported",
            "impact": "high/medium/low",
            "ongoing": true/false
        }}
    ],
    "trend_indicators": {{
        "direction": "improving/stable/declining/mixed",
        "confidence": "high/medium/low",
        "key_drivers": ["driver1", "driver2"]
    }},
    "items_to_watch": [
        {{
            "item": "What to monitor",
            "reason": "Why it matters going forward"
        }}
    ],
    "data_quality": {{
        "days_with_data": {len(research_dates)},
        "total_findings": {sum(len(t['findings']) for t in all_topics.values())},
        "coverage_notes": "Any gaps or limitations in this month's data"
    }}
}}

Be thorough but concise. Preserve important details while eliminating redundancy.
Return ONLY valid JSON."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            result_text = response.content[0].text
            
            # Clean up JSON
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]
            
            archive = json.loads(result_text.strip())
            
            # Add metadata
            archive['archived_at'] = datetime.utcnow().isoformat()
            archive['source_dates'] = research_dates
            archive['raw_finding_count'] = sum(len(t['findings']) for t in all_topics.values())
            
            return archive
            
        except Exception as e:
            # Return a basic archive if Claude fails
            return {
                "period": f"{year}-{month:02d}",
                "month_name": month_name,
                "executive_summary": f"Archive generation failed: {e}",
                "error": str(e),
                "raw_topics": {k: len(v['findings']) for k, v in all_topics.items()},
                "source_dates": research_dates,
                "archived_at": datetime.utcnow().isoformat()
            }
    
    def update_historical_context(self, new_archive: dict = None) -> dict:
        """
        Update the rolling historical context document with all available archives.
        This provides the agent with long-term memory of industry evolution.
        """
        
        # Load existing context
        existing_context = self.storage.load_historical_context() or {
            "created_at": datetime.utcnow().isoformat(),
            "periods_covered": [],
            "industry_timeline": [],
            "long_term_trends": {},
            "historical_baseline": {}
        }
        
        # Load all monthly archives
        archives = self.storage.list_monthly_archives()
        
        if not archives:
            print("No monthly archives found to build context from")
            return existing_context
        
        # Load archive contents
        archive_data = []
        for arch in archives:
            data = self.storage.load_monthly_archive(arch['year'], arch['month'])
            if data:
                archive_data.append(data)
        
        if not archive_data:
            return existing_context
        
        # Build comprehensive prompt
        archives_text = ""
        for arch in archive_data[-12:]:  # Last 12 months max for context
            archives_text += f"\n\n## {arch.get('month_name', arch.get('period', 'Unknown'))}\n"
            archives_text += f"Summary: {arch.get('executive_summary', 'No summary')}\n"
            
            if arch.get('major_developments'):
                archives_text += "Major developments:\n"
                for dev in arch.get('major_developments', [])[:5]:
                    archives_text += f"- {dev.get('title', 'Untitled')}: {dev.get('description', '')[:100]}\n"
            
            if arch.get('trend_indicators'):
                ti = arch['trend_indicators']
                archives_text += f"Trend: {ti.get('direction', 'unknown')} ({ti.get('confidence', 'unknown')} confidence)\n"
        
        prompt = f"""You are building a comprehensive historical context document for a cannabis retail business.
This document will help the research agent understand where the industry has been and where it's heading.

AVAILABLE MONTHLY ARCHIVES:
{archives_text}

Create an updated historical context document as JSON:
{{
    "last_updated": "{datetime.utcnow().isoformat()}",
    "periods_covered": ["YYYY-MM", ...],
    "industry_overview": {{
        "current_state": "2-3 sentence description of where the industry is NOW",
        "trajectory": "improving/stable/declining/volatile",
        "confidence": "high/medium/low"
    }},
    "historical_timeline": [
        {{
            "period": "YYYY-MM or date range",
            "event": "What happened",
            "significance": "Why it mattered",
            "category": "regulatory/market/competition/products/pricing"
        }}
    ],
    "long_term_trends": {{
        "regulatory": {{
            "direction": "tightening/loosening/stable/mixed",
            "summary": "Brief description of regulatory trajectory",
            "key_events": ["event1", "event2"]
        }},
        "market": {{
            "direction": "growing/shrinking/stable/volatile",
            "summary": "Brief description of market trajectory",
            "key_drivers": ["driver1", "driver2"]
        }},
        "competition": {{
            "intensity": "increasing/decreasing/stable",
            "summary": "Brief description of competitive landscape evolution"
        }},
        "pricing": {{
            "direction": "rising/falling/stable/volatile",
            "summary": "Brief description of pricing trends"
        }}
    }},
    "baseline_metrics": {{
        "description": "Reference points the agent should know",
        "key_facts": ["fact1", "fact2", "fact3"]
    }},
    "ongoing_stories": [
        {{
            "story": "What's unfolding",
            "started": "When it began",
            "current_status": "Where things stand",
            "watch_for": "What to look for next"
        }}
    ],
    "lessons_learned": [
        "Key insight 1 from historical data",
        "Key insight 2 from historical data"
    ]
}}

This document should give the research agent deep context about industry history.
Prioritize patterns, turning points, and ongoing narratives.
Return ONLY valid JSON."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            result_text = response.content[0].text
            
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]
            
            context = json.loads(result_text.strip())
            
            # Save and return
            self.storage.save_historical_context(context)
            return context
            
        except Exception as e:
            print(f"Error updating historical context: {e}")
            return existing_context
    
    def run_archival_cycle(self, delete_after_archive: bool = False) -> dict:
        """
        Run the archival process:
        1. Find months that are 30+ days old and not yet archived
        2. Condense each month's findings into an archive
        3. Update the historical context
        4. Optionally delete the original daily files
        """
        
        print(f"Starting archival cycle at {datetime.utcnow().isoformat()}")
        
        results = {
            "started_at": datetime.utcnow().isoformat(),
            "months_processed": [],
            "files_archived": 0,
            "files_deleted": 0,
            "errors": []
        }
        
        # Find months to archive
        months_to_archive = self.storage.get_months_to_archive(min_age_days=30)
        
        if not months_to_archive:
            print("No months ready for archival")
            results["message"] = "No months ready for archival"
            results["completed_at"] = datetime.utcnow().isoformat()
            return results
        
        print(f"Found {len(months_to_archive)} months to archive")
        
        for month_data in months_to_archive:
            year = month_data['year']
            month = month_data['month']
            files = month_data['files']
            
            print(f"Processing {year}-{month:02d} ({len(files)} files)")
            
            try:
                # Load all daily findings for this month
                daily_findings = []
                for key in files:
                    findings = self.storage.load_findings_by_key(key)
                    if findings:
                        daily_findings.append(findings)
                
                if not daily_findings:
                    print(f"  No valid findings found, skipping")
                    continue
                
                # Condense into monthly archive
                archive = self.condense_month(year, month, daily_findings)
                
                # Save archive
                if self.storage.save_monthly_archive(year, month, archive):
                    results["months_processed"].append(f"{year}-{month:02d}")
                    results["files_archived"] += len(files)
                    
                    # Optionally delete original files
                    if delete_after_archive:
                        deleted = self.storage.delete_daily_findings(files)
                        results["files_deleted"] += deleted
                        print(f"  Archived and deleted {deleted} files")
                    else:
                        print(f"  Archived {len(files)} files (originals retained)")
                else:
                    results["errors"].append(f"Failed to save archive for {year}-{month:02d}")
                    
            except Exception as e:
                error_msg = f"Error archiving {year}-{month:02d}: {e}"
                print(f"  {error_msg}")
                results["errors"].append(error_msg)
        
        # Update historical context with new archives
        if results["months_processed"]:
            print("Updating historical context...")
            try:
                self.update_historical_context()
                print("Historical context updated")
            except Exception as e:
                results["errors"].append(f"Failed to update historical context: {e}")
        
        results["completed_at"] = datetime.utcnow().isoformat()
        return results


# =============================================================================
# LAMBDA HANDLER
# =============================================================================

def lambda_handler(event, context):
    """AWS Lambda entry point.
    
    Event parameters:
        mode: "research" (default) or "archive"
        topics: List of topic IDs to research (optional, default: all)
        force_full: Skip preliminary scans, do full research (optional, default: false)
        delete_after_archive: Delete daily files after archiving (optional, default: false)
    """
    
    # Get configuration from environment
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    bucket_name = os.environ.get("S3_BUCKET_NAME", "retail-data-bcgr")
    
    if not api_key:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "ANTHROPIC_API_KEY not configured"})
        }
    
    # Initialize components
    storage = ResearchStorage(bucket_name)
    agent = IndustryResearchAgent(api_key, storage)
    
    # Check mode
    mode = event.get("mode", "research")
    
    try:
        if mode == "archive":
            # Run archival cycle
            delete_after = event.get("delete_after_archive", False)
            results = agent.run_archival_cycle(delete_after_archive=delete_after)
            
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Archival cycle completed",
                    "mode": "archive",
                    "months_processed": results.get("months_processed", []),
                    "files_archived": results.get("files_archived", 0),
                    "files_deleted": results.get("files_deleted", 0),
                    "errors": results.get("errors", [])
                }, default=str)
            }
        
        else:
            # Default: Run research cycle
            topics = None
            if event.get("topics"):
                topic_ids = event["topics"]
                topics = [t for t in RESEARCH_TOPICS if t["id"] in topic_ids]
            
            force_full = event.get("force_full", False)
            results = agent.run_research_cycle(topics, force_full=force_full)
            
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Research cycle completed",
                    "mode": "research",
                    "cycle_id": results["research_cycle_id"],
                    "topics_researched": results.get("topics_researched", 0),
                    "topics_skipped": results.get("topics_skipped", 0),
                    "errors": len(results["errors"]),
                    "summary": results.get("summary", {}).get("executive_summary", "")
                }, default=str)
            }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "mode": mode})
        }


# =============================================================================
# LOCAL TESTING
# =============================================================================

if __name__ == "__main__":
    """Run locally for testing."""
    import sys
    
    # Check for API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY environment variable not set")
        sys.exit(1)
    
    bucket_name = os.environ.get("S3_BUCKET_NAME", "retail-data-bcgr")
    
    print(f"Running local test with bucket: {bucket_name}")
    
    # Initialize and run
    storage = ResearchStorage(bucket_name)
    agent = IndustryResearchAgent(api_key, storage)
    
    # Run with just one topic for testing
    test_topics = [RESEARCH_TOPICS[0]]  # Just regulatory
    results = agent.run_research_cycle(test_topics)
    
    print("\n" + "="*50)
    print("RESEARCH RESULTS")
    print("="*50)
    print(json.dumps(results, indent=2, default=str))
