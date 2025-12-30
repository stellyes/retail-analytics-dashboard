"""
Research Findings Integration Module
Adds industry research viewing capabilities to the Streamlit dashboard.
"""

import streamlit as st
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime, timedelta
from typing import Optional, Dict
import os


class ResearchFindingsViewer:
    """View and interact with research findings from S3."""
    
    def __init__(self, bucket_name: str = None, prefix: str = "research-findings/"):
        """Initialize the viewer with S3 configuration."""
        self.bucket_name = bucket_name or os.environ.get("S3_BUCKET_NAME", "retail-data-bcgr")
        self.prefix = prefix
        self.s3 = None
        self._init_s3()
    
    def _init_s3(self):
        """Initialize S3 client."""
        try:
            # Try environment variables first
            aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
            
            # Fall back to Streamlit secrets
            if not aws_access_key:
                try:
                    aws_secrets = st.secrets["aws"]
                    aws_access_key = aws_secrets.get("access_key_id")
                    aws_secret_key = aws_secrets.get("secret_access_key")
                    aws_region = aws_secrets.get("region", "us-west-2")
                    self.bucket_name = aws_secrets.get("bucket_name", self.bucket_name)
                except Exception:
                    pass
            
            if aws_access_key and aws_secret_key:
                self.s3 = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
        except Exception as e:
            st.error(f"Failed to initialize S3: {e}")
    
    def is_available(self) -> bool:
        """Check if S3 is available."""
        return self.s3 is not None
    
    def load_latest_summary(self) -> Optional[dict]:
        """Load the latest cumulative summary."""
        if not self.is_available():
            return None
        
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name,
                Key=f"{self.prefix}summary/latest.json"
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            st.error(f"Error loading summary: {e}")
            return None
    
    def load_history(self) -> list:
        """Load historical summaries."""
        if not self.is_available():
            return []
        
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name,
                Key=f"{self.prefix}summary/history.json"
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            return []
    
    def load_findings_for_date(self, date: datetime) -> Optional[dict]:
        """Load findings for a specific date."""
        if not self.is_available():
            return None
        
        try:
            key = f"{self.prefix}{date.strftime('%Y/%m/%d')}/findings.json"
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            return None
    
    def list_available_dates(self) -> list:
        """List all dates with available findings."""
        if not self.is_available():
            return []
        
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            dates = set()
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
                for obj in page.get('Contents', []):
                    if 'findings.json' in obj['Key'] and '/archive/' not in obj['Key']:
                        # Extract date from path like research-findings/2024/01/15/findings.json
                        parts = obj['Key'].split('/')
                        if len(parts) >= 4:
                            try:
                                date_str = f"{parts[1]}-{parts[2]}-{parts[3]}"
                                dates.add(date_str)
                            except (IndexError, ValueError):
                                pass
            
            return sorted(list(dates), reverse=True)
        except Exception as e:
            st.error(f"Error listing dates: {e}")
            return []
    
    def load_historical_context(self) -> Optional[dict]:
        """Load the historical context document."""
        if not self.is_available():
            return None
        
        try:
            key = f"{self.prefix}archive/historical-context.json"
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            return None
        except Exception as e:
            st.error(f"Error loading historical context: {e}")
            return None
    
    def list_monthly_archives(self) -> list:
        """List all available monthly archives."""
        if not self.is_available():
            return []
        
        try:
            prefix = f"{self.prefix}archive/"
            paginator = self.s3.get_paginator('list_objects_v2')
            archives = []
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('monthly-summary.json'):
                        parts = obj['Key'].split('/')
                        if len(parts) >= 4:
                            try:
                                year = int(parts[-3])
                                month = int(parts[-2])
                                archives.append({
                                    'key': obj['Key'],
                                    'year': year,
                                    'month': month,
                                    'period': f"{year}-{month:02d}",
                                    'display': datetime(year, month, 1).strftime('%B %Y')
                                })
                            except (ValueError, IndexError):
                                pass
            
            return sorted(archives, key=lambda x: x['period'], reverse=True)
        except Exception as e:
            st.error(f"Error listing archives: {e}")
            return []
    
    def load_monthly_archive(self, year: int, month: int) -> Optional[dict]:
        """Load a specific monthly archive."""
        if not self.is_available():
            return None
        
        try:
            key = f"{self.prefix}archive/{year}/{month:02d}/monthly-summary.json"
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            return None
        except Exception as e:
            st.error(f"Error loading archive: {e}")
            return None


def render_research_page():
    """Render the research findings page in Streamlit."""

    st.header("🔬 Industry Research & Trends")
    st.markdown("""
    AI-powered monitoring of cannabis industry trends, regulations, and market developments.
    Upload your own research documents or view historical automated findings.
    """)

    # Initialize viewer
    viewer = ResearchFindingsViewer()

    # Import manual research functionality
    try:
        from manual_research_integration import (
            render_upload_tab,
            render_analysis_tab,
            render_findings_tab,
            DocumentStorage,
            ManualResearchAnalyzer,
            MonthlyResearchSummarizer
        )
        manual_research_available = True
    except ImportError:
        manual_research_available = False

    # Tabs for different views
    if manual_research_available:
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📤 Manual Upload",
            "📊 Manual Analysis",
            "📈 Manual Findings",
            "📋 Historical Summary",
            "📅 Historical Findings",
            "📚 Historical Archives"
        ])

        # Manual research tabs
        with tab1:
            if manual_research_available:
                # Get API key
                api_key = os.environ.get("ANTHROPIC_API_KEY")
                if not api_key:
                    try:
                        api_key = st.secrets["ANTHROPIC_API_KEY"]
                    except Exception:
                        pass
                if not api_key:
                    try:
                        api_key = st.secrets["anthropic"]["ANTHROPIC_API_KEY"]
                    except Exception:
                        pass

                if api_key:
                    storage = DocumentStorage(viewer.bucket_name)
                    render_upload_tab(storage)
                else:
                    st.error("⚠️ ANTHROPIC_API_KEY not configured. Cannot perform manual research.")

        with tab2:
            if manual_research_available and api_key:
                storage = DocumentStorage(viewer.bucket_name)
                analyzer = ManualResearchAnalyzer(api_key)
                render_analysis_tab(storage, analyzer)
            else:
                st.error("⚠️ ANTHROPIC_API_KEY not configured. Cannot perform manual research.")

        with tab3:
            if manual_research_available and api_key:
                analyzer = ManualResearchAnalyzer(api_key)
                render_findings_tab(analyzer)
            else:
                st.error("⚠️ ANTHROPIC_API_KEY not configured. Cannot view manual research findings.")

        # Historical manual research tabs
        with tab4:
            # Get API key for monthly summarizer
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                try:
                    api_key = st.secrets["ANTHROPIC_API_KEY"]
                except Exception:
                    pass
            if not api_key:
                try:
                    api_key = st.secrets["anthropic"]["ANTHROPIC_API_KEY"]
                except Exception:
                    pass

            if api_key:
                summarizer = MonthlyResearchSummarizer(api_key)
                _render_manual_monthly_summary(summarizer)
            else:
                st.error("⚠️ ANTHROPIC_API_KEY not configured. Cannot generate monthly summaries.")

        with tab5:
            if api_key:
                summarizer = MonthlyResearchSummarizer(api_key)
                _render_manual_findings_list(summarizer)
            else:
                st.error("⚠️ ANTHROPIC_API_KEY not configured. Cannot view findings.")

        with tab6:
            if api_key:
                summarizer = MonthlyResearchSummarizer(api_key)
                _render_manual_archives(summarizer)
            else:
                st.error("⚠️ ANTHROPIC_API_KEY not configured. Cannot view archives.")

    else:
        # Original layout if manual research not available
        if not viewer.is_available():
            st.warning("""
            ⚠️ **Research findings not available**

            The research agent stores findings in S3. Please ensure:
            1. AWS credentials are configured in `.streamlit/secrets.toml`
            2. The research agent has been deployed and run at least once

            See the setup guide for deployment instructions.
            """)
            return

        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Executive Summary",
            "📅 Daily Findings",
            "📈 Trend History",
            "📚 Historical Archives"
        ])

        with tab1:
            _render_executive_summary(viewer)

        with tab2:
            _render_daily_findings(viewer)

        with tab3:
            render_trend_history(viewer)

        with tab4:
            _render_archives(viewer)


def _render_executive_summary(viewer: ResearchFindingsViewer):
    """Render the executive summary view."""
    
    summary = viewer.load_latest_summary()
    
    if not summary:
        st.info("No research summary available yet. The agent will generate one after its first run.")
        return
    
    # Last updated
    generated_at = summary.get('generated_at', 'Unknown')
    if generated_at != 'Unknown':
        try:
            dt = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
            st.caption(f"Last updated: {dt.strftime('%B %d, %Y at %I:%M %p')} UTC")
        except ValueError:
            st.caption(f"Last updated: {generated_at}")
    
    # Executive summary
    st.subheader("📋 Executive Summary")
    st.markdown(summary.get('executive_summary', 'No summary available.'))
    
    st.markdown("---")
    
    # Key findings and action items in columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Key Findings")
        key_findings = summary.get('key_findings', [])
        
        if not key_findings:
            st.info("No key findings recorded.")
        else:
            for finding in key_findings:
                importance = finding.get('importance', 'medium')
                emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(importance, "⚪")
                status = finding.get('status', 'new')
                status_badge = {"new": "🆕", "ongoing": "🔄", "resolved": "✅"}.get(status, "")
                
                with st.expander(f"{emoji} {finding.get('finding', 'Untitled')} {status_badge}"):
                    st.markdown(f"**Category:** {finding.get('category', 'Unknown')}")
                    st.markdown(f"**First Identified:** {finding.get('first_identified', 'Unknown')}")
                    st.markdown(f"**Status:** {status.title()}")
    
    with col2:
        st.subheader("✅ Action Items")
        action_items = summary.get('action_items', [])
        
        if not action_items:
            st.info("No action items at this time.")
        else:
            for item in action_items:
                priority = item.get('priority', 'medium')
                emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(priority, "⚪")
                
                st.markdown(f"{emoji} **{item.get('action', 'Unknown')}**")
                st.caption(f"Priority: {priority.title()} | Deadline: {item.get('deadline', 'Ongoing')}")
                st.markdown("---")
    
    # Tracking items
    st.subheader("👁️ Items Being Tracked")
    tracking_items = summary.get('tracking_items', [])
    
    if not tracking_items:
        st.info("No items currently being tracked.")
    else:
        tracking_df = pd.DataFrame(tracking_items)
        st.dataframe(tracking_df, use_container_width=True, hide_index=True)


def _render_daily_findings(viewer: ResearchFindingsViewer):
    """Render the daily findings view."""
    
    # Date selector
    available_dates = viewer.list_available_dates()
    
    if not available_dates:
        st.info("No research findings available yet. The agent will store findings after its first run.")
        return
    
    # Convert to datetime for display
    date_options = {}
    for date_str in available_dates:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            display = dt.strftime("%B %d, %Y")
            date_options[display] = dt
        except ValueError:
            pass
    
    selected_display = st.selectbox(
        "Select Date",
        options=list(date_options.keys())
    )
    
    if not selected_display:
        return
    
    selected_date = date_options[selected_display]
    findings = viewer.load_findings_for_date(selected_date)
    
    if not findings:
        st.warning(f"Could not load findings for {selected_display}")
        return
    
    # Display metadata
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Research Cycle", findings.get('research_cycle_id', 'Unknown')[:8])
    with col2:
        st.metric("Topics Researched", len(findings.get('topics', [])))
    with col3:
        error_count = len(findings.get('errors', []))
        st.metric("Errors", error_count, delta=None if error_count == 0 else error_count, delta_color="inverse")
    
    st.markdown("---")
    
    # Display findings by topic
    for topic_result in findings.get('topics', []):
        topic_name = topic_result.get('topic_name', 'Unknown Topic')
        importance = topic_result.get('importance', 'medium')
        importance_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(importance, "⚪")
        
        with st.expander(f"{importance_emoji} {topic_name}", expanded=(importance == 'high')):
            # Summary
            st.markdown(f"**Summary:** {topic_result.get('summary', 'No summary available')}")
            
            # Individual findings
            findings_list = topic_result.get('findings', [])
            if findings_list:
                st.markdown("**Findings:**")
                for finding in findings_list:
                    relevance = finding.get('relevance', 'medium')
                    rel_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(relevance, "⚪")
                    
                    st.markdown(f"{rel_emoji} **{finding.get('title', 'Untitled')}**")
                    st.markdown(finding.get('description', 'No description'))
                    
                    if finding.get('source'):
                        st.caption(f"Source: {finding['source']}")
                    if finding.get('recommended_action'):
                        st.info(f"💡 Recommended: {finding['recommended_action']}")
                    
                    st.markdown("---")
    
    # Show errors if any
    errors = findings.get('errors', [])
    if errors:
        with st.expander("⚠️ Errors During Research", expanded=False):
            for error in errors:
                st.error(error)


def render_trend_history(viewer: ResearchFindingsViewer):
    """Render the trend history view."""
    
    history = viewer.load_history()
    
    if not history:
        st.info("No historical data available yet. History will accumulate as the agent runs.")
        return
    
    # Prepare data for visualization
    history_data = []
    for entry in history:
        try:
            timestamp = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
            history_data.append({
                'date': timestamp.date(),
                'timestamp': timestamp,
                'summary': entry.get('summary', '')[:200],
                'key_items_count': len(entry.get('key_items', []))
            })
        except (ValueError, KeyError):
            continue
    
    if not history_data:
        st.warning("Could not parse historical data.")
        return
    
    df = pd.DataFrame(history_data)
    
    # Activity chart
    st.subheader("📈 Research Activity")
    
    # Group by date and count
    daily_counts = df.groupby('date').size().reset_index(name='research_runs')
    daily_counts['date'] = pd.to_datetime(daily_counts['date'])
    
    st.bar_chart(daily_counts.set_index('date')['research_runs'])
    
    st.markdown("---")
    
    # Timeline of summaries
    st.subheader("📜 Research Timeline")
    
    for entry in reversed(history[-10:]):  # Last 10 entries
        try:
            timestamp = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
            time_display = timestamp.strftime("%B %d, %Y %I:%M %p")
        except ValueError:
            time_display = entry.get('timestamp', 'Unknown')
        
        with st.expander(f"📅 {time_display}"):
            st.markdown(entry.get('summary', 'No summary'))
            
            key_items = entry.get('key_items', [])
            if key_items:
                st.markdown("**Key Items:**")
                for item in key_items:
                    if isinstance(item, dict):
                        st.markdown(f"- {item.get('finding', item.get('title', str(item)))}")
                    else:
                        st.markdown(f"- {item}")


def _render_archives(viewer: ResearchFindingsViewer):
    """Render the historical archives and long-term context view."""
    
    st.subheader("📚 Long-Term Industry Context")
    
    # Load and display historical context
    context = viewer.load_historical_context()
    
    if context:
        st.markdown("### 🌐 Industry Overview")
        
        overview = context.get('industry_overview', {})
        col1, col2, col3 = st.columns(3)
        
        with col1:
            trajectory = overview.get('trajectory', 'Unknown')
            emoji = {'improving': '📈', 'stable': '➡️', 'declining': '📉', 'volatile': '🔄'}.get(trajectory, '❓')
            st.metric("Trajectory", f"{emoji} {trajectory.title()}")
        
        with col2:
            confidence = overview.get('confidence', 'Unknown')
            st.metric("Confidence", confidence.title())
        
        with col3:
            last_updated = context.get('last_updated', 'Unknown')
            if last_updated != 'Unknown':
                try:
                    dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    last_updated = dt.strftime('%b %d, %Y')
                except ValueError:
                    pass
            st.metric("Last Updated", last_updated)
        
        st.markdown(f"**Current State:** {overview.get('current_state', 'No data available')}")
        
        st.markdown("---")
        
        # Long-term trends
        st.markdown("### 📊 Long-Term Trends")
        
        trends = context.get('long_term_trends', {})
        
        trend_cols = st.columns(2)
        trend_items = [
            ('regulatory', '⚖️ Regulatory'),
            ('market', '📈 Market'),
            ('competition', '🏪 Competition'),
            ('pricing', '💰 Pricing')
        ]
        
        for i, (key, label) in enumerate(trend_items):
            with trend_cols[i % 2]:
                trend = trends.get(key, {})
                direction = trend.get('direction', 'unknown')
                summary = trend.get('summary', 'No data')
                
                with st.expander(f"{label}: {direction.title()}", expanded=False):
                    st.markdown(summary)
        
        st.markdown("---")
        
        # Ongoing stories
        st.markdown("### 📰 Ongoing Stories")
        ongoing = context.get('ongoing_stories', [])
        
        if ongoing:
            for story in ongoing:
                with st.expander(f"🔄 {story.get('story', 'Untitled')}", expanded=False):
                    st.markdown(f"**Started:** {story.get('started', 'Unknown')}")
                    st.markdown(f"**Current Status:** {story.get('current_status', 'Unknown')}")
                    st.markdown(f"**Watch For:** {story.get('watch_for', 'Unknown')}")
        else:
            st.info("No ongoing stories tracked yet.")
        
        st.markdown("---")
        
        # Lessons learned
        lessons = context.get('lessons_learned', [])
        if lessons:
            st.markdown("### 💡 Key Insights from History")
            for lesson in lessons:
                st.markdown(f"- {lesson}")
    else:
        st.info("No historical context available yet. Context is built after monthly archives are created.")
    
    st.markdown("---")
    
    # Monthly archives
    st.subheader("📅 Monthly Archives")
    
    archives = viewer.list_monthly_archives()
    
    if not archives:
        st.info("No monthly archives available yet. Archives are created on the 1st of each month for data older than 30 days.")
        return
    
    # Archive selector
    archive_options = {a['display']: (a['year'], a['month']) for a in archives}
    selected_archive = st.selectbox("Select Month", options=list(archive_options.keys()))
    
    if selected_archive:
        year, month = archive_options[selected_archive]
        archive = viewer.load_monthly_archive(year, month)
        
        if archive:
            st.markdown(f"### {archive.get('month_name', selected_archive)}")
            
            # Executive summary
            st.markdown("**Executive Summary:**")
            st.markdown(archive.get('executive_summary', 'No summary available'))
            
            # Data quality info
            quality = archive.get('data_quality', {})
            if quality:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Days with Data", quality.get('days_with_data', 'N/A'))
                with col2:
                    st.metric("Total Findings", archive.get('raw_finding_count', quality.get('total_findings', 'N/A')))
            
            st.markdown("---")
            
            # Category summaries
            st.markdown("**Category Summaries:**")
            categories = archive.get('category_summaries', {})
            
            for cat_name, cat_summary in categories.items():
                if cat_summary:
                    with st.expander(f"📁 {cat_name.replace('_', ' ').title()}"):
                        st.markdown(cat_summary)
            
            # Major developments
            st.markdown("---")
            st.markdown("**Major Developments:**")
            
            developments = archive.get('major_developments', [])
            if developments:
                for dev in developments:
                    impact = dev.get('impact', 'medium')
                    emoji = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(impact, '⚪')
                    ongoing = " 🔄" if dev.get('ongoing') else ""
                    
                    with st.expander(f"{emoji} {dev.get('title', 'Untitled')}{ongoing}"):
                        st.markdown(dev.get('description', 'No description'))
                        st.caption(f"Category: {dev.get('category', 'Unknown')} | When: {dev.get('date_range', 'Unknown')}")
            else:
                st.info("No major developments recorded for this month.")
            
            # Trend indicators
            trend_ind = archive.get('trend_indicators', {})
            if trend_ind:
                st.markdown("---")
                st.markdown(f"**Month Trend:** {trend_ind.get('direction', 'Unknown').title()} (Confidence: {trend_ind.get('confidence', 'Unknown')})")
                if trend_ind.get('key_drivers'):
                    st.markdown(f"Key Drivers: {', '.join(trend_ind['key_drivers'])}")
        else:
            st.error(f"Could not load archive for {selected_archive}")


def _render_manual_monthly_summary(summarizer):
    """Render the manual research monthly summary tab."""
    st.header("📋 Monthly Research Summary")

    st.markdown("""
    Generate comprehensive monthly summaries from your manually uploaded research.
    Uses Claude Sonnet 4.5 to synthesize all findings into actionable insights.
    """)

    # Select month to generate summary for
    col1, col2 = st.columns(2)

    with col1:
        current_year = datetime.now().year
        year = st.selectbox("Year", range(current_year, current_year - 3, -1))

    with col2:
        month = st.selectbox("Month", range(1, 13), format_func=lambda x: datetime(2000, x, 1).strftime('%B'))

    # Check if summary already exists
    existing_summaries = summarizer.list_monthly_summaries()
    summary_exists = any(s['year'] == year and s['month'] == month for s in existing_summaries)

    if summary_exists:
        st.info(f"✅ Summary already exists for {datetime(year, month, 1).strftime('%B %Y')}. View it in the Archives tab or regenerate below.")

    # Generate summary button
    if st.button("🚀 Generate Monthly Summary", type="primary"):
        with st.spinner(f"Generating comprehensive summary for {datetime(year, month, 1).strftime('%B %Y')}..."):
            result = summarizer.generate_monthly_summary(year, month)

            if result['success']:
                summary = result['summary']

                # Save to S3
                if summarizer.save_monthly_summary(summary):
                    st.success(f"✅ Summary generated and saved!")
                else:
                    st.warning("⚠️ Summary generated but failed to save to S3")

                # Display summary
                st.markdown("---")
                _display_monthly_summary(summary)

            else:
                st.error(f"❌ Failed to generate summary: {result['error']}")

    # Show existing summary if available
    if summary_exists and not st.session_state.get('regenerating'):
        st.markdown("---")
        st.subheader(f"Existing Summary: {datetime(year, month, 1).strftime('%B %Y')}")

        summary = summarizer.load_monthly_summary(year, month)
        if summary:
            _display_monthly_summary(summary)


def _display_monthly_summary(summary: Dict):
    """Display a monthly summary."""
    # Metadata
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Documents", summary.get('documents_analyzed', 0))
    col2.metric("Findings", summary.get('findings_count', 0))
    col3.metric("Cost", f"${summary.get('estimated_cost_usd', 0):.3f}")

    tokens = summary.get('tokens_used', {})
    col4.metric("Tokens", f"{tokens.get('total', 0):,}")

    st.markdown("---")

    # Executive Summary
    st.subheader("📋 Executive Summary")
    st.markdown(summary.get('executive_summary', 'No summary available'))

    st.markdown("---")

    # Key Insights
    st.subheader("💡 Key Insights")
    for insight in summary.get('key_insights', []):
        importance = insight.get('importance', 'medium')
        emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(importance, "⚪")

        with st.expander(f"{emoji} {insight.get('insight', 'Untitled')[:100]}...", expanded=(importance == 'high')):
            st.markdown(f"**Category:** {insight.get('category', 'Unknown').title()}")
            st.markdown(f"**Insight:** {insight.get('insight')}")

            if insight.get('supporting_findings'):
                st.markdown("**Supporting Findings:**")
                for finding in insight['supporting_findings']:
                    st.markdown(f"- {finding}")

            if insight.get('recommended_actions'):
                st.markdown("**Recommended Actions:**")
                for action in insight['recommended_actions']:
                    priority = action.get('priority', 'medium')
                    emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(priority, "⚪")
                    st.markdown(f"{emoji} **{action.get('action')}**")
                    st.markdown(f"   - Timeline: {action.get('timeline')}")
                    st.markdown(f"   - Impact: {action.get('expected_impact')}")

    st.markdown("---")

    # Trends Analysis
    st.subheader("📈 Trends Analysis")
    trends = summary.get('trends_analysis', {})

    for category, analysis in trends.items():
        if analysis:
            with st.expander(f"📊 {category.replace('_', ' ').title()}"):
                st.markdown(analysis)

    # Opportunities
    if summary.get('opportunities'):
        st.markdown("---")
        st.subheader("🎯 Opportunities")
        for opp in summary['opportunities']:
            with st.expander(f"💼 {opp.get('opportunity', 'Untitled')[:80]}..."):
                st.markdown(f"**Rationale:** {opp.get('rationale')}")
                st.markdown(f"**Potential Impact:** {opp.get('potential_impact')}")
                if opp.get('actions'):
                    st.markdown("**Action Steps:**")
                    for i, action in enumerate(opp['actions'], 1):
                        st.markdown(f"{i}. {action}")

    # Risks
    if summary.get('risks_and_challenges'):
        st.markdown("---")
        st.subheader("⚠️ Risks & Challenges")
        for risk in summary['risks_and_challenges']:
            severity = risk.get('severity', 'medium')
            emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(severity, "⚪")

            with st.expander(f"{emoji} {risk.get('risk', 'Untitled')[:80]}..."):
                st.markdown(f"**Severity:** {severity.title()}")
                st.markdown(f"**Mitigation:** {risk.get('mitigation')}")

    # Strategic Recommendations
    if summary.get('strategic_recommendations'):
        st.markdown("---")
        st.subheader("🎯 Strategic Recommendations")
        for i, rec in enumerate(summary['strategic_recommendations'], 1):
            st.markdown(f"**{i}.** {rec}")

    # Download button
    st.markdown("---")
    json_data = json.dumps(summary, indent=2, default=str)
    st.download_button(
        label="📥 Download Full Summary (JSON)",
        data=json_data,
        file_name=f"monthly_summary_{summary.get('year')}_{summary.get('month'):02d}.json",
        mime="application/json"
    )


def _render_manual_findings_list(summarizer):
    """Render list of findings with actions from monthly summaries."""
    st.header("📈 Consolidated Findings")

    summaries = summarizer.list_monthly_summaries()

    if not summaries:
        st.info("No monthly summaries available yet. Generate one in the 'Historical Summary' tab.")
        return

    # Select summary to view
    selected = st.selectbox(
        "Select Month",
        options=summaries,
        format_func=lambda x: x['display']
    )

    if selected:
        summary = summarizer.load_monthly_summary(selected['year'], selected['month'])

        if summary:
            st.subheader(f"Findings: {summary.get('month_name')}")

            # Display each key insight with actions
            for insight in summary.get('key_insights', []):
                importance = insight.get('importance', 'medium')
                emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(importance, "⚪")

                with st.expander(f"{emoji} {insight.get('insight', 'Untitled')[:100]}...", expanded=True):
                    st.markdown(f"**Finding:** {insight.get('insight')}")
                    st.markdown(f"**Category:** {insight.get('category', 'Unknown').title()}")

                    # Show recommended actions
                    if insight.get('recommended_actions'):
                        st.markdown("### 📋 Actions to Take")
                        for action in insight['recommended_actions']:
                            priority = action.get('priority', 'medium')
                            emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(priority, "⚪")

                            st.markdown(f"{emoji} **{action.get('action')}**")
                            st.markdown(f"- **Priority:** {priority.title()}")
                            st.markdown(f"- **Timeline:** {action.get('timeline')}")
                            st.markdown(f"- **Expected Impact:** {action.get('expected_impact')}")
                            st.markdown("---")

                    # Show supporting findings (sources)
                    if insight.get('supporting_findings'):
                        with st.expander("📚 Source Findings"):
                            for i, finding in enumerate(insight['supporting_findings'], 1):
                                st.markdown(f"{i}. {finding}")


def _render_manual_archives(summarizer):
    """Render archives of past monthly summaries."""
    st.header("📚 Historical Archives")

    summaries = summarizer.list_monthly_summaries()

    if not summaries:
        st.info("No monthly summaries available yet. Generate one in the 'Historical Summary' tab.")
        return

    st.write(f"**{len(summaries)} monthly summaries available**")

    # Display list of summaries
    for summary_meta in summaries:
        with st.expander(f"📅 {summary_meta['display']}", expanded=False):
            summary = summarizer.load_monthly_summary(summary_meta['year'], summary_meta['month'])

            if summary:
                # Quick stats
                col1, col2, col3 = st.columns(3)
                col1.metric("Documents", summary.get('documents_analyzed', 0))
                col2.metric("Findings", summary.get('findings_count', 0))
                col3.metric("Cost", f"${summary.get('estimated_cost_usd', 0):.3f}")

                # Executive summary preview
                st.markdown("**Executive Summary:**")
                exec_sum = summary.get('executive_summary', 'No summary')
                preview = exec_sum[:300] + "..." if len(exec_sum) > 300 else exec_sum
                st.markdown(preview)

                # View full button
                if st.button(f"View Full Summary", key=f"view_{summary_meta['year']}_{summary_meta['month']}"):
                    st.session_state['selected_archive'] = summary_meta

    # Display full summary if selected
    if 'selected_archive' in st.session_state:
        st.markdown("---")
        meta = st.session_state['selected_archive']
        summary = summarizer.load_monthly_summary(meta['year'], meta['month'])

        if summary:
            st.subheader(f"📋 Full Summary: {meta['display']}")
            _display_monthly_summary(summary)

            if st.button("✖️ Close"):
                del st.session_state['selected_archive']
                st.rerun()


def add_research_to_sidebar():
    """Add research page to the dashboard sidebar navigation."""
    # This function should be called from the main app to integrate
    # Return True if research page should be shown
    return True


# =============================================================================
# INTEGRATION HELPER
# =============================================================================

def integrate_with_dashboard(main_app_code: str) -> str:
    """
    Helper to show how to integrate with the main dashboard.
    Returns instructions for integration.
    """
    return """
    # Integration Instructions
    
    Add this to your main dashboard.py navigation section:
    
    1. Import the research module:
    ```python
    from research_integration import render_research_page
    ```
    
    2. Add to your navigation radio buttons:
    ```python
    page = st.radio("Navigation", [
        "📊 Dashboard",
        "📈 Sales Analysis", 
        "🏷️ Brand Performance",
        "📦 Product Categories",
        "🔗 Brand-Product Mapping",
        "💡 Recommendations",
        "🔬 Industry Research",  # Add this
        "📤 Data Upload"
    ])
    ```
    
    3. Add the page routing:
    ```python
    elif page == "🔬 Industry Research":
        render_research_page()
    ```
    """
