"""
SEO Analysis Dashboard Integration
Integrates SEO research agent findings into the retail-analytics-dashboard.
Supports multiple websites: barbarycoastsf.com and grassrootssf.com
"""

import streamlit as st
import boto3
import json
from datetime import datetime, timedelta
from typing import Optional, List
import os

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# Configuration
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "retail-data-bcgr")
S3_PREFIX = os.environ.get("SEO_S3_PREFIX", "seo-analysis")

# Target websites
TARGET_WEBSITES = [
    "https://barbarycoastsf.com",
    "https://grassrootssf.com"
]


def url_to_folder(url: str) -> str:
    """Convert URL to safe folder name."""
    folder = url.replace("https://", "").replace("http://", "").rstrip("/")
    folder = folder.replace("/", "_").replace(":", "_")
    return folder


class SEOFindingsViewer:
    """Viewer for SEO analysis findings stored in S3."""
    
    def __init__(self, website: Optional[str] = None):
        self.bucket = S3_BUCKET
        self.base_prefix = S3_PREFIX
        self.website = website
        self._s3 = None
        
        # Set prefix based on website
        if website:
            self.site_folder = url_to_folder(website)
            self.prefix = f"{S3_PREFIX}/{self.site_folder}"
        else:
            self.site_folder = None
            self.prefix = S3_PREFIX
    
    @property
    def s3(self):
        if self._s3 is None:
            try:
                # Try to get credentials from Streamlit secrets
                if hasattr(st, 'secrets') and 'aws' in st.secrets:
                    self._s3 = boto3.client(
                        's3',
                        aws_access_key_id=st.secrets['aws']['access_key_id'],
                        aws_secret_access_key=st.secrets['aws']['secret_access_key'],
                        region_name=st.secrets['aws'].get('region', 'us-west-2')
                    )
                else:
                    # Fall back to environment/IAM credentials
                    self._s3 = boto3.client('s3')
            except Exception as e:
                st.error(f"Failed to initialize S3 client: {e}")
                return None
        return self._s3
    
    def _key(self, *parts) -> str:
        """Build S3 key from parts."""
        return f"{self.prefix}/{'/'.join(parts)}"
    
    def is_available(self) -> bool:
        """Check if SEO findings are available."""
        if self.s3 is None:
            return False
        try:
            self.s3.head_object(
                Bucket=self.bucket,
                Key=self._key("summary", "latest.json")
            )
            return True
        except:
            return False
    
    def load_latest_summary(self) -> Optional[dict]:
        """Load the latest SEO summary."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=self._key("summary", "latest.json")
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            st.error(f"Error loading SEO summary: {e}")
            return None
    
    def load_findings(self, date: datetime) -> Optional[dict]:
        """Load findings for a specific date."""
        try:
            key = self._key(date.strftime("%Y/%m/%d"), "seo-findings.json")
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            return None
    
    def load_recent_findings(self, days: int = 7) -> list:
        """Load findings from the last N days."""
        findings = []
        today = datetime.utcnow()
        
        for i in range(days):
            date = today - timedelta(days=i)
            data = self.load_findings(date)
            if data:
                findings.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "data": data
                })
        
        return findings
    
    def load_historical_context(self) -> Optional[dict]:
        """Load historical SEO context."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=self._key("archive", "historical-context.json")
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except:
            return None
    
    def list_available_dates(self) -> list:
        """List all dates with available findings."""
        dates = []
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
                for obj in page.get('Contents', []):
                    if '/seo-findings.json' in obj['Key']:
                        parts = obj['Key'].replace(f"{self.prefix}/", "").split("/")
                        if len(parts) >= 3:
                            try:
                                date_str = f"{parts[0]}-{parts[1]}-{parts[2]}"
                                dates.append(date_str)
                            except:
                                continue
        except Exception as e:
            st.error(f"Error listing dates: {e}")
        
        return sorted(dates, reverse=True)


class ManualSEOAnalyzer:
    """Manual SEO analysis triggered from the dashboard."""

    def __init__(self, api_key: str):
        if not ANTHROPIC_AVAILABLE:
            raise ImportError("anthropic package not installed")

        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-haiku-4-5-20251001"  # Cost-effective for SEO
        self.s3 = boto3.client('s3')
        self.bucket = S3_BUCKET

    def analyze_website_seo(self, website: str) -> dict:
        """
        Perform SEO analysis on a website.
        Cost: ~$0.05-0.10 per analysis with Haiku
        """

        prompt = f"""Analyze the SEO of this website: {website}

Perform a comprehensive SEO analysis covering:

1. **Technical SEO**
   - Page speed
   - Mobile responsiveness
   - HTTPS/SSL
   - Site structure

2. **On-Page SEO**
   - Title tags and meta descriptions
   - Header structure (H1, H2, etc.)
   - Content quality and keyword usage
   - Image optimization

3. **Local SEO** (for cannabis dispensary)
   - Google Business Profile optimization
   - Local keywords
   - NAP consistency
   - Local citations

4. **Content & Keywords**
   - Keyword targeting
   - Content gaps
   - Competitor comparison

5. **User Experience**
   - Navigation
   - Call-to-actions
   - Mobile experience

Return analysis as JSON:
{{
    "website": "{website}",
    "analyzed_at": "{datetime.utcnow().isoformat()}",
    "overall_score": 0-100,
    "categories": {{
        "technical_seo": {{
            "score": 0-100,
            "findings": ["finding 1", "finding 2"],
            "issues": ["issue 1", "issue 2"],
            "recommendations": ["rec 1", "rec 2"]
        }},
        "on_page_seo": {{...}},
        "local_seo": {{...}},
        "content": {{...}},
        "user_experience": {{...}}
    }},
    "top_priorities": [
        {{
            "priority": "Issue title",
            "severity": "high/medium/low",
            "category": "technical/content/local/ux",
            "action": "Recommended action"
        }}
    ],
    "competitive_insights": "Brief comparison with competitors",
    "quick_wins": ["Easy fix 1", "Easy fix 2"]
}}

Focus on actionable recommendations for a cannabis dispensary in San Francisco.
Return ONLY valid JSON."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )

            result_text = response.content[0].text

            # Clean JSON
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]

            analysis = json.loads(result_text.strip())

            # Add metadata
            analysis['model_used'] = self.model
            input_tokens = getattr(response.usage, 'input_tokens', 0)
            output_tokens = getattr(response.usage, 'output_tokens', 0)
            analysis['tokens_used'] = {
                'input': input_tokens,
                'output': output_tokens,
                'total': input_tokens + output_tokens
            }

            # Estimate cost
            cost = (input_tokens * 0.80 / 1000000) + (output_tokens * 4.00 / 1000000)
            analysis['estimated_cost_usd'] = round(cost, 4)

            return {
                'success': True,
                'analysis': analysis
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def save_to_s3(self, website: str, analysis: dict) -> bool:
        """Save SEO analysis results to S3."""
        try:
            timestamp = datetime.utcnow()
            site_folder = url_to_folder(website)

            # Save to dated folder
            s3_key = f"{S3_PREFIX}/{site_folder}/{timestamp.strftime('%Y/%m/%d')}/seo-findings.json"

            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json.dumps(analysis, indent=2, default=str),
                ContentType='application/json'
            )

            # Also save as latest
            latest_key = f"{S3_PREFIX}/{site_folder}/summary/latest.json"
            self.s3.put_object(
                Bucket=self.bucket,
                Key=latest_key,
                Body=json.dumps(analysis, indent=2, default=str),
                ContentType='application/json'
            )

            return True

        except Exception as e:
            st.error(f"Error saving to S3: {e}")
            return False


def render_seo_page():
    """Main render function for SEO analysis page."""
    
    st.header("🔍 SEO Analysis Dashboard")
    st.markdown("""
    AI-powered SEO monitoring and analysis for your websites.
    The agent runs daily to track SEO health, identify issues, and provide recommendations.
    """)
    
    # Website selector
    st.sidebar.markdown("---")
    st.sidebar.subheader("🌐 Select Website")
    
    website_options = {
        "Barbary Coast SF": "https://barbarycoastsf.com",
        "Grassroots SF": "https://grassrootssf.com"
    }
    
    selected_site_name = st.sidebar.radio(
        "Choose website to analyze:",
        list(website_options.keys())
    )
    selected_website = website_options[selected_site_name]
    
    st.sidebar.caption(f"📍 {selected_website}")
    
    # Show comparison option
    show_comparison = st.sidebar.checkbox("📊 Show site comparison", value=False)
    
    # Initialize viewer for selected website
    viewer = SEOFindingsViewer(website=selected_website)
    
    if not viewer.is_available():
        st.warning(f"""
        ⚠️ **SEO analysis data not available for {selected_site_name}**
        
        The SEO agent stores findings in S3. Please ensure:
        1. AWS credentials are configured in `.streamlit/secrets.toml`
        2. The SEO research agent has been deployed and run at least once
        
        See the setup guide for deployment instructions.
        """)
        
        # Show setup instructions
        with st.expander("📋 Setup Instructions"):
            st.markdown(f"""
            ### Quick Setup
            
            1. **Configure AWS Credentials**
            
            Create `.streamlit/secrets.toml`:
            ```toml
            [aws]
            access_key_id = "YOUR_ACCESS_KEY"
            secret_access_key = "YOUR_SECRET_KEY"
            region = "us-west-2"
            ```
            
            2. **Deploy the SEO Agent**
            ```bash
            export ANTHROPIC_API_KEY="sk-ant-..."
            ./deploy-seo-agent.sh
            ```
            
            3. **Trigger First Analysis**
            ```bash
            aws lambda invoke --function-name seo-research-agent-analyzer \\
              --payload '{{"mode": "analyze"}}' out.json
            ```
            
            The agent will scan both websites:
            - barbarycoastsf.com
            - grassrootssf.com
            """)
        return
    
    # Show comparison view if selected
    if show_comparison:
        render_site_comparison()
        return
    
    # Display selected website prominently
    st.info(f"**Viewing SEO analysis for:** [{selected_site_name}]({selected_website})")
    
    # Tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Executive Summary",
        "📋 Category Details",
        "📈 Trend Analysis",
        "🔄 Manual Analysis",
        "📚 Historical Archives"
    ])

    with tab1:
        render_executive_summary(viewer)

    with tab2:
        render_category_details(viewer)

    with tab3:
        render_trend_analysis(viewer)

    with tab4:
        render_manual_analysis_tab(selected_website, selected_site_name)

    with tab5:
        render_historical_archives(viewer)


def render_site_comparison():
    """Render side-by-side comparison of both websites."""
    
    st.subheader("📊 Website SEO Comparison")
    st.markdown("Side-by-side comparison of SEO metrics for both websites.")
    
    # Load data for both sites
    viewers = {
        "Barbary Coast SF": SEOFindingsViewer(website="https://barbarycoastsf.com"),
        "Grassroots SF": SEOFindingsViewer(website="https://grassrootssf.com")
    }
    
    summaries = {}
    for name, viewer in viewers.items():
        if viewer.is_available():
            summaries[name] = viewer.load_latest_summary()
        else:
            summaries[name] = None
    
    # Overall scores comparison
    st.markdown("### Overall SEO Scores")
    
    col1, col2 = st.columns(2)
    
    for idx, (name, summary) in enumerate(summaries.items()):
        col = col1 if idx == 0 else col2
        
        with col:
            st.markdown(f"#### {name}")
            
            if not summary:
                st.warning("No data available")
                continue
            
            score = summary.get("overall_score")
            if score:
                # Color based on score
                if score >= 70:
                    st.success(f"**Score: {score}/100** ✅")
                elif score >= 50:
                    st.warning(f"**Score: {score}/100** ⚠️")
                else:
                    st.error(f"**Score: {score}/100** ❌")
            else:
                st.info("Score not available")
            
            # Category scores
            st.markdown("**Category Breakdown:**")
            categories = summary.get("categories", {})
            for cat_id, cat_data in categories.items():
                cat_name = cat_data.get("name", cat_id)
                cat_score = cat_data.get("score", "N/A")
                status = cat_data.get("status", "unknown")
                
                status_emoji = {"good": "✅", "warning": "⚠️", "critical": "❌"}.get(status, "❓")
                st.markdown(f"- {status_emoji} {cat_name}: {cat_score}")
    
    st.markdown("---")
    
    # Issues comparison
    st.markdown("### Priority Issues Comparison")
    
    col1, col2 = st.columns(2)
    
    for idx, (name, summary) in enumerate(summaries.items()):
        col = col1 if idx == 0 else col2
        
        with col:
            st.markdown(f"#### {name}")
            
            if not summary:
                continue
            
            issues = summary.get("priority_issues", [])
            if not issues:
                st.success("No priority issues! 🎉")
            else:
                for i, issue in enumerate(issues[:3], 1):
                    st.markdown(f"{i}. {issue.get('issue', 'Unknown')[:50]}...")
    
    st.markdown("---")
    
    # Quick wins comparison
    st.markdown("### Quick Wins Available")
    
    col1, col2 = st.columns(2)
    
    for idx, (name, summary) in enumerate(summaries.items()):
        col = col1 if idx == 0 else col2
        
        with col:
            st.markdown(f"#### {name}")
            
            if not summary:
                continue
            
            wins = summary.get("quick_wins", [])
            if not wins:
                st.info("No quick wins identified")
            else:
                for win in wins[:3]:
                    st.markdown(f"⚡ {win.get('action', 'Unknown')[:50]}...")
    
    with tab1:
        render_executive_summary(viewer)
    
    with tab2:
        render_category_details(viewer)
    
    with tab3:
        render_trend_analysis(viewer)
    
    with tab4:
        render_historical_archives(viewer)


def render_executive_summary(viewer: SEOFindingsViewer):
    """Render the executive summary view."""
    
    summary = viewer.load_latest_summary()
    
    if not summary:
        st.info("No SEO summary available yet. The agent will generate one after its first run.")
        return
    
    # Last updated
    generated_at = summary.get('generated_at', 'Unknown')
    if generated_at != 'Unknown':
        try:
            dt = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
            st.caption(f"Last updated: {dt.strftime('%B %d, %Y at %I:%M %p')} UTC")
        except ValueError:
            st.caption(f"Last updated: {generated_at}")
    
    # Target website
    st.markdown(f"**Target Website:** [{summary.get('target_website', 'N/A')}]({summary.get('target_website', '#')})")
    
    st.markdown("---")
    
    # Overall score and metrics
    col1, col2, col3, col4 = st.columns(4)
    
    overall_score = summary.get('overall_score')
    with col1:
        if overall_score:
            score_color = "green" if overall_score >= 70 else "orange" if overall_score >= 50 else "red"
            st.metric("Overall SEO Score", f"{overall_score}/100")
        else:
            st.metric("Overall SEO Score", "N/A")
    
    with col2:
        priority_count = len(summary.get('priority_issues', []))
        st.metric("Priority Issues", priority_count)
    
    with col3:
        quick_wins = len(summary.get('quick_wins', []))
        st.metric("Quick Wins Available", quick_wins)
    
    with col4:
        categories_analyzed = len(summary.get('categories', {}))
        st.metric("Categories Analyzed", categories_analyzed)
    
    st.markdown("---")
    
    # Executive summary
    st.subheader("📋 Executive Summary")
    st.markdown(summary.get('executive_summary', 'No summary available.'))
    
    st.markdown("---")
    
    # Priority issues and quick wins in columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🚨 Priority Issues")
        priority_issues = summary.get('priority_issues', [])
        
        if not priority_issues:
            st.success("No critical issues detected!")
        else:
            for i, issue in enumerate(priority_issues[:5], 1):
                severity_color = {
                    "high": "🔴",
                    "medium": "🟡",
                    "low": "🟢"
                }.get(issue.get('severity', 'medium'), "⚪")
                
                with st.expander(f"{severity_color} #{i}: {issue.get('issue', 'Unknown issue')[:50]}..."):
                    st.markdown(f"**Category:** {issue.get('category', 'N/A')}")
                    st.markdown(f"**Issue:** {issue.get('issue', 'N/A')}")
                    st.markdown(f"**Business Impact:** {issue.get('business_impact', 'N/A')}")
                    st.markdown(f"**Effort Required:** {issue.get('effort', 'N/A')}")
                    st.markdown(f"**Recommendation:** {issue.get('recommendation', 'N/A')}")
    
    with col2:
        st.subheader("⚡ Quick Wins")
        quick_wins = summary.get('quick_wins', [])
        
        if not quick_wins:
            st.info("No quick wins identified at this time.")
        else:
            for win in quick_wins[:5]:
                st.markdown(f"""
                **Action:** {win.get('action', 'N/A')}
                
                Expected Impact: {win.get('expected_impact', 'N/A')}
                
                Effort: {win.get('effort', 'N/A')}
                
                ---
                """)
    
    # Strategic recommendations
    st.subheader("🎯 Strategic Recommendations")
    recommendations = summary.get('strategic_recommendations', [])
    
    if recommendations:
        for i, rec in enumerate(recommendations, 1):
            st.markdown(f"{i}. {rec}")
    else:
        st.info("No strategic recommendations available.")


def render_category_details(viewer: SEOFindingsViewer):
    """Render detailed category analysis view."""
    
    # Date selector
    available_dates = viewer.list_available_dates()
    
    if not available_dates:
        st.info("No analysis data available yet.")
        return
    
    selected_date = st.selectbox(
        "Select Analysis Date",
        available_dates,
        format_func=lambda x: datetime.strptime(x, "%Y-%m-%d").strftime("%B %d, %Y")
    )
    
    date = datetime.strptime(selected_date, "%Y-%m-%d")
    findings = viewer.load_findings(date)
    
    if not findings:
        st.error(f"Could not load findings for {selected_date}")
        return
    
    # Category scores overview
    st.subheader("📊 Category Scores")
    
    categories = findings.get('categories', {})
    
    if categories:
        # Create score chart
        scores_data = []
        for cat_id, cat_data in categories.items():
            if isinstance(cat_data, dict) and 'score' in cat_data:
                scores_data.append({
                    "Category": cat_data.get('name', cat_id),
                    "Score": cat_data.get('score', 0)
                })
        
        if scores_data:
            import pandas as pd
            df = pd.DataFrame(scores_data)
            st.bar_chart(df.set_index("Category"))
    
    st.markdown("---")
    
    # Detailed category analysis
    st.subheader("📋 Detailed Analysis by Category")
    
    for cat_id, cat_data in categories.items():
        if not isinstance(cat_data, dict):
            continue
        
        cat_name = cat_data.get('name', cat_id)
        cat_status = cat_data.get('status', 'unknown')
        cat_score = cat_data.get('score', 'N/A')
        
        status_emoji = {
            "good": "✅",
            "warning": "⚠️",
            "critical": "❌",
            "unknown": "❓",
            "skipped": "⏭️"
        }.get(cat_status, "❓")
        
        with st.expander(f"{status_emoji} {cat_name} (Score: {cat_score})"):
            if cat_status == "skipped":
                st.info(f"Skipped: {cat_data.get('reason', 'No significant changes detected')}")
                continue
            
            # Current state
            st.markdown("**Current State:**")
            st.markdown(cat_data.get('current_state', 'N/A'))
            
            # Strengths
            strengths = cat_data.get('strengths', [])
            if strengths:
                st.markdown("**Strengths:**")
                for s in strengths:
                    st.markdown(f"- ✅ {s}")
            
            # Weaknesses
            weaknesses = cat_data.get('weaknesses', [])
            if weaknesses:
                st.markdown("**Weaknesses:**")
                for w in weaknesses:
                    st.markdown(f"- ❌ {w}")
            
            # Issues
            issues = cat_data.get('issues', [])
            if issues:
                st.markdown("**Issues:**")
                for issue in issues:
                    severity = issue.get('severity', 'medium')
                    st.markdown(f"""
                    - **[{severity.upper()}]** {issue.get('issue', 'N/A')}
                      - Impact: {issue.get('impact', 'N/A')}
                      - Fix: {issue.get('recommendation', 'N/A')}
                    """)
            
            # Opportunities
            opportunities = cat_data.get('opportunities', [])
            if opportunities:
                st.markdown("**Opportunities:**")
                for o in opportunities:
                    st.markdown(f"- 💡 {o}")
            
            # Changes since last
            changes = cat_data.get('changes_since_last')
            if changes:
                st.markdown("**Changes Since Last Analysis:**")
                st.markdown(changes)


def render_trend_analysis(viewer: SEOFindingsViewer):
    """Render SEO trend analysis over time."""
    
    st.subheader("📈 SEO Trends Over Time")
    
    # Load recent findings
    days = st.slider("Days to analyze", 7, 30, 14)
    recent = viewer.load_recent_findings(days=days)
    
    if not recent:
        st.info("Not enough data for trend analysis. Check back after a few days of analysis.")
        return
    
    # Extract scores over time
    import pandas as pd
    
    trend_data = []
    for entry in recent:
        date = entry['date']
        data = entry['data']
        
        row = {"Date": date}
        
        # Overall score
        row["Overall"] = data.get('overall_score', 0)
        
        # Category scores
        for cat_id, cat_data in data.get('categories', {}).items():
            if isinstance(cat_data, dict) and 'score' in cat_data:
                row[cat_data.get('name', cat_id)] = cat_data.get('score', 0)
        
        trend_data.append(row)
    
    if trend_data:
        df = pd.DataFrame(trend_data)
        df = df.sort_values('Date')
        df = df.set_index('Date')
        
        st.line_chart(df)
        
        # Show data table
        with st.expander("View Raw Data"):
            st.dataframe(df)
    
    st.markdown("---")
    
    # Issue trends
    st.subheader("🚨 Issue Trends")
    
    issue_counts = []
    for entry in recent:
        date = entry['date']
        data = entry['data']
        
        total_issues = 0
        high_issues = 0
        
        for cat_id, cat_data in data.get('categories', {}).items():
            if isinstance(cat_data, dict):
                issues = cat_data.get('issues', [])
                total_issues += len(issues)
                high_issues += len([i for i in issues if i.get('severity') == 'high'])
        
        issue_counts.append({
            "Date": date,
            "Total Issues": total_issues,
            "High Priority": high_issues
        })
    
    if issue_counts:
        df_issues = pd.DataFrame(issue_counts)
        df_issues = df_issues.sort_values('Date')
        df_issues = df_issues.set_index('Date')
        
        st.bar_chart(df_issues)


def render_historical_archives(viewer: SEOFindingsViewer):
    """Render historical archives and long-term context."""
    
    st.subheader("📚 Historical SEO Context")
    
    historical = viewer.load_historical_context()
    
    if not historical:
        st.info("""
        No historical archives available yet.
        
        The archival system automatically condenses findings older than 30 days into monthly summaries.
        Check back after the first monthly archive cycle runs.
        """)
        return
    
    # Last updated
    last_updated = historical.get('last_updated', 'Unknown')
    st.caption(f"Historical context last updated: {last_updated}")
    
    # Site overview
    st.markdown("### 🌐 Site Overview")
    st.markdown(historical.get('site_overview', 'No overview available.'))
    
    st.markdown("---")
    
    # Long-term trends
    st.markdown("### 📈 Long-Term Trends")
    
    trends = historical.get('long_term_trends', {})
    if trends:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Technical SEO:**")
            st.markdown(trends.get('technical_seo', 'N/A'))
            
            st.markdown("**Content Performance:**")
            st.markdown(trends.get('content_performance', 'N/A'))
        
        with col2:
            st.markdown("**Backlink Growth:**")
            st.markdown(trends.get('backlink_growth', 'N/A'))
            
            st.markdown("**Competitive Position:**")
            st.markdown(trends.get('competitive_position', 'N/A'))
    
    st.markdown("---")
    
    # Historical timeline
    st.markdown("### 📅 Historical Timeline")
    
    timeline = historical.get('historical_timeline', [])
    if timeline:
        for event in timeline:
            st.markdown(f"**{event.get('period', 'N/A')}:** {event.get('event', 'N/A')}")
    else:
        st.info("No timeline events recorded yet.")
    
    st.markdown("---")
    
    # Patterns and strategies
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔄 Recurring Patterns")
        patterns = historical.get('recurring_patterns', [])
        if patterns:
            for p in patterns:
                st.markdown(f"- {p}")
        else:
            st.info("No patterns identified yet.")
        
        st.markdown("### ✅ Successful Strategies")
        strategies = historical.get('successful_strategies', [])
        if strategies:
            for s in strategies:
                st.markdown(f"- {s}")
        else:
            st.info("No successful strategies recorded yet.")
    
    with col2:
        st.markdown("### ⚠️ Persistent Challenges")
        challenges = historical.get('persistent_challenges', [])
        if challenges:
            for c in challenges:
                st.markdown(f"- {c}")
        else:
            st.info("No persistent challenges identified.")
        
        st.markdown("### 🎯 Strategic Recommendations")
        recs = historical.get('strategic_recommendations', [])
        if recs:
            for r in recs:
                st.markdown(f"- {r}")
        else:
            st.info("No long-term recommendations available.")


def render_manual_analysis_tab(website: str, site_name: str):
    """Render the manual SEO analysis tab."""

    st.subheader("🔄 Run Manual SEO Analysis")

    st.markdown(f"""
    Perform an on-demand SEO analysis for **{site_name}**.

    This will:
    - Analyze the website's SEO health
    - Save results to S3 for birds-eye analysis
    - Cost: ~$0.05-0.10 per analysis (using Haiku)
    - Results available immediately in other tabs
    """)

    # Check for API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        st.error("⚠️ ANTHROPIC_API_KEY not configured. Cannot perform analysis.")
        return

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("### Analysis Options")

        # Cost estimate
        st.info("💰 Estimated cost: $0.05-0.10")

        if st.button(f"🔍 Analyze {site_name}", type="primary", key=f"analyze_{website}"):
            with st.spinner(f"Analyzing {website}... This may take 30-60 seconds."):
                try:
                    # Initialize analyzer
                    analyzer = ManualSEOAnalyzer(api_key)

                    # Run analysis
                    result = analyzer.analyze_website_seo(website)

                    if result['success']:
                        analysis = result['analysis']

                        # Save to S3
                        if analyzer.save_to_s3(website, analysis):
                            st.success("✅ Analysis complete and saved to S3!")

                            # Display summary
                            st.markdown("### 📊 Analysis Summary")

                            col_a, col_b, col_c = st.columns(3)
                            col_a.metric("Overall Score", f"{analysis.get('overall_score', 'N/A')}/100")
                            col_b.metric("Cost", f"${analysis.get('estimated_cost_usd', 0):.4f}")
                            col_c.metric("Tokens Used", f"{analysis.get('tokens_used', {}).get('total', 0):,}")

                            # Top priorities
                            st.markdown("### 🎯 Top Priorities")
                            priorities = analysis.get('top_priorities', [])
                            if priorities:
                                for idx, priority in enumerate(priorities[:5], 1):
                                    severity = priority.get('severity', 'medium')
                                    emoji = "🔴" if severity == 'high' else "🟡" if severity == 'medium' else "🟢"
                                    st.markdown(f"{emoji} **{idx}. {priority.get('priority')}**")
                                    st.markdown(f"   *Action:* {priority.get('action')}")
                            else:
                                st.info("No priorities identified")

                            # Quick wins
                            st.markdown("### ⚡ Quick Wins")
                            quick_wins = analysis.get('quick_wins', [])
                            if quick_wins:
                                for win in quick_wins:
                                    st.markdown(f"- {win}")
                            else:
                                st.info("No quick wins identified")

                            st.info("💡 View detailed results in the **Executive Summary** and **Category Details** tabs")

                        else:
                            st.error("Analysis completed but failed to save to S3")

                    else:
                        st.error(f"Analysis failed: {result.get('error')}")

                except Exception as e:
                    st.error(f"Error during analysis: {e}")

    with col2:
        st.markdown("### 📋 Recent Analyses")

        # Show recent analysis history
        viewer = SEOFindingsViewer(website=website)
        dates = viewer.list_available_dates()

        if dates:
            st.write(f"**{len(dates)} analyses on record**")
            for date in dates[:5]:
                st.caption(f"📅 {date}")
        else:
            st.info("No previous analyses")

        st.markdown("---")
        st.markdown("### 💡 Tips")
        st.caption("Run analysis weekly to track SEO progress over time")
        st.caption("Results are automatically saved for historical comparison")
        st.caption("Use the comparison view to benchmark against competitors")


# Export for use in main dashboard
__all__ = ['render_seo_page', 'SEOFindingsViewer']
