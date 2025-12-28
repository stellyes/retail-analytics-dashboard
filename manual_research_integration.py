"""
Manual Research Document Upload and Analysis
Cost-effective alternative to autonomous web research agents.
Users upload HTML documents, system analyzes and extracts findings.
"""

import streamlit as st
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
import json
from typing import List, Dict
import hashlib
from bs4 import BeautifulSoup
import re

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================

S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "retail-data-bcgr")
S3_PREFIX_DOCUMENTS = "research-documents/"
S3_PREFIX_FINDINGS = "research-findings/manual/"

# Research topic categories
RESEARCH_CATEGORIES = [
    "Regulatory Updates",
    "Market Trends",
    "Competitive Landscape",
    "Product Innovation",
    "Pricing & Economics",
    "Other"
]

# =============================================================================
# DOCUMENT STORAGE
# =============================================================================

class DocumentStorage:
    """Manages research document uploads and storage in S3."""

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')

    def upload_document(self, file_content: bytes, filename: str,
                       category: str, source_url: str = None) -> Dict:
        """Upload a research document to S3."""

        # Generate document ID and metadata
        doc_id = hashlib.md5(file_content[:1000]).hexdigest()[:12]
        timestamp = datetime.utcnow()

        # Determine file type
        file_ext = filename.split('.')[-1].lower()
        content_type = 'text/html' if file_ext == 'html' else 'text/plain'

        # S3 key structure: research-documents/YYYY/MM/DD/{doc_id}_{filename}
        s3_key = f"{S3_PREFIX_DOCUMENTS}{timestamp.strftime('%Y/%m/%d')}/{doc_id}_{filename}"

        metadata = {
            'category': category,
            'uploaded_at': timestamp.isoformat(),
            'original_filename': filename,
            'doc_id': doc_id,
        }

        if source_url:
            metadata['source_url'] = source_url

        try:
            # Upload document
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType=content_type,
                Metadata=metadata
            )

            # Upload metadata separately for easy querying
            metadata_key = s3_key.replace(filename, f"{doc_id}_metadata.json")
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )

            return {
                'success': True,
                'doc_id': doc_id,
                's3_key': s3_key,
                'metadata': metadata
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def list_uploaded_documents(self, days: int = 30) -> List[Dict]:
        """List recently uploaded documents."""

        try:
            # List all metadata files
            prefix = S3_PREFIX_DOCUMENTS
            paginator = self.s3.get_paginator('list_objects_v2')

            documents = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('_metadata.json'):
                        # Load metadata
                        try:
                            response = self.s3.get_object(
                                Bucket=self.bucket_name,
                                Key=obj['Key']
                            )
                            metadata = json.loads(response['Body'].read().decode('utf-8'))
                            metadata['s3_key'] = obj['Key'].replace('_metadata.json', '')
                            metadata['uploaded_at'] = obj['LastModified'].isoformat()
                            documents.append(metadata)
                        except:
                            continue

            # Sort by upload date
            documents.sort(key=lambda x: x.get('uploaded_at', ''), reverse=True)

            return documents

        except Exception as e:
            st.error(f"Error listing documents: {e}")
            return []

    def get_document_content(self, s3_key: str) -> str:
        """Retrieve document content from S3."""

        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read().decode('utf-8', errors='ignore')
        except Exception as e:
            return f"Error loading document: {e}"

    def delete_document(self, s3_key: str) -> bool:
        """Delete a document and its metadata."""

        try:
            # Delete document
            self.s3.delete_object(Bucket=self.bucket_name, Key=s3_key)

            # Delete metadata
            filename = s3_key.split('/')[-1]
            doc_id = filename.split('_')[0]
            metadata_key = s3_key.replace(filename, f"{doc_id}_metadata.json")
            self.s3.delete_object(Bucket=self.bucket_name, Key=metadata_key)

            return True
        except Exception as e:
            st.error(f"Error deleting document: {e}")
            return False


# =============================================================================
# DOCUMENT ANALYZER
# =============================================================================

class ManualResearchAnalyzer:
    """Analyzes uploaded documents to extract research findings."""

    def __init__(self, api_key: str):
        if not ANTHROPIC_AVAILABLE:
            raise ImportError("anthropic package not installed")

        self.client = anthropic.Anthropic(api_key=api_key)
        # Use Haiku for cost efficiency - ~95% cheaper than Sonnet
        self.model = "claude-haiku-4-5-20251001"
        self.s3 = boto3.client('s3')

    def extract_text_from_html(self, html_content: str) -> str:
        """Extract clean text from HTML document."""

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()

            # Get text
            text = soup.get_text()

            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)

            return text

        except Exception as e:
            return html_content  # Fallback to raw content

    def analyze_document(self, content: str, category: str,
                        source_url: str = None, filename: str = None) -> Dict:
        """
        Analyze a single document and extract findings.
        Cost: ~$0.02-0.05 per document with Haiku (vs $0.50+ with Sonnet + web search)
        """

        # Extract clean text if HTML
        if '<html' in content.lower() or '<body' in content.lower():
            clean_text = self.extract_text_from_html(content)
        else:
            clean_text = content

        # Limit to first 8000 characters to control costs (~2000 tokens)
        # This is sufficient for most news articles and reports
        clean_text = clean_text[:8000]

        prompt = f"""Analyze this cannabis industry document and extract key findings.

Category: {category}
Source: {source_url or filename or 'Uploaded document'}

DOCUMENT CONTENT:
{clean_text}

Extract structured findings as JSON:
{{
    "summary": "2-3 sentence summary of the main points",
    "key_findings": [
        {{
            "finding": "Brief finding description",
            "relevance": "high/medium/low",
            "category": "regulatory/market/competition/products/pricing/other",
            "action_required": true/false,
            "recommended_action": "Specific action if needed"
        }}
    ],
    "date_mentioned": "YYYY-MM-DD or null if no specific date",
    "key_facts": ["fact 1", "fact 2", "fact 3"],
    "relevance_score": "high/medium/low for SF cannabis dispensary"
}}

Focus on actionable insights relevant to a San Francisco cannabis dispensary.
Return ONLY valid JSON."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,  # Keep output concise for cost control
                messages=[{"role": "user", "content": prompt}]
            )

            result_text = response.content[0].text

            # Clean JSON
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]

            findings = json.loads(result_text.strip())

            # Add metadata
            findings['analyzed_at'] = datetime.utcnow().isoformat()
            findings['source'] = source_url or filename
            findings['category'] = category
            findings['model_used'] = self.model
            findings['char_count'] = len(clean_text)

            # Get token usage for cost tracking
            input_tokens = getattr(response.usage, 'input_tokens', 0)
            output_tokens = getattr(response.usage, 'output_tokens', 0)
            findings['tokens_used'] = {
                'input': input_tokens,
                'output': output_tokens,
                'total': input_tokens + output_tokens
            }

            # Estimate cost (Haiku pricing: $0.80 per million input, $4 per million output)
            cost = (input_tokens * 0.80 / 1000000) + (output_tokens * 4.00 / 1000000)
            findings['estimated_cost_usd'] = round(cost, 4)

            return {
                'success': True,
                'findings': findings
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def batch_analyze_documents(self, documents: List[Dict],
                               storage: DocumentStorage) -> Dict:
        """
        Analyze multiple documents and generate a consolidated findings report.
        Cost: ~$0.10-0.30 for 10 documents vs $5-10 for autonomous agent
        """

        results = {
            'started_at': datetime.utcnow().isoformat(),
            'documents_analyzed': 0,
            'total_findings': 0,
            'total_cost_usd': 0,
            'total_tokens': 0,
            'findings_by_category': {},
            'all_findings': [],
            'errors': []
        }

        for doc in documents:
            try:
                # Load document content
                s3_key = doc.get('s3_key') or doc.get('doc_id')
                content = storage.get_document_content(s3_key)

                # Analyze
                analysis = self.analyze_document(
                    content=content,
                    category=doc.get('category', 'Other'),
                    source_url=doc.get('source_url'),
                    filename=doc.get('original_filename')
                )

                if analysis['success']:
                    findings = analysis['findings']

                    # Track stats
                    results['documents_analyzed'] += 1
                    results['total_cost_usd'] += findings.get('estimated_cost_usd', 0)
                    results['total_tokens'] += findings.get('tokens_used', {}).get('total', 0)

                    # Group by category
                    category = findings.get('category', 'Other')
                    if category not in results['findings_by_category']:
                        results['findings_by_category'][category] = []

                    results['findings_by_category'][category].append(findings)
                    results['all_findings'].append(findings)
                    results['total_findings'] += len(findings.get('key_findings', []))

                else:
                    results['errors'].append({
                        'document': doc.get('original_filename'),
                        'error': analysis.get('error')
                    })

            except Exception as e:
                results['errors'].append({
                    'document': doc.get('original_filename', 'unknown'),
                    'error': str(e)
                })

        results['completed_at'] = datetime.utcnow().isoformat()
        results['total_cost_usd'] = round(results['total_cost_usd'], 2)

        return results

    def generate_consolidated_summary(self, batch_results: Dict) -> Dict:
        """
        Generate an executive summary from batch analysis results.
        Cost: ~$0.01-0.02
        """

        # Compile findings
        findings_text = ""
        for category, findings_list in batch_results.get('findings_by_category', {}).items():
            findings_text += f"\n\n## {category}\n"
            for finding in findings_list:
                findings_text += f"\nSummary: {finding.get('summary', 'N/A')}\n"
                findings_text += f"Key Facts: {', '.join(finding.get('key_facts', [])[:3])}\n"

        prompt = f"""Create an executive summary from these cannabis industry research findings.

FINDINGS FROM {batch_results.get('documents_analyzed', 0)} DOCUMENTS:
{findings_text[:4000]}

Generate a consolidated summary as JSON:
{{
    "executive_summary": "3-5 sentence overview of key insights",
    "top_priorities": [
        {{
            "priority": "Brief description",
            "category": "regulatory/market/competition/products/pricing",
            "urgency": "high/medium/low",
            "action": "Recommended next step"
        }}
    ],
    "trends_identified": ["trend 1", "trend 2", "trend 3"],
    "competitive_intelligence": "Brief summary of competitor activity",
    "regulatory_updates": "Brief summary of regulatory changes"
}}

Return ONLY valid JSON."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}]
            )

            result_text = response.content[0].text

            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]

            summary = json.loads(result_text.strip())
            summary['generated_at'] = datetime.utcnow().isoformat()

            return summary

        except Exception as e:
            return {
                'executive_summary': f"Summary generation failed: {e}",
                'generated_at': datetime.utcnow().isoformat()
            }

    def save_findings_to_s3(self, findings: Dict, bucket: str) -> bool:
        """Save analysis findings to S3."""

        try:
            timestamp = datetime.utcnow()
            s3_key = f"{S3_PREFIX_FINDINGS}{timestamp.strftime('%Y/%m/%d')}/analysis_{timestamp.strftime('%H%M%S')}.json"

            self.s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=json.dumps(findings, indent=2, default=str),
                ContentType='application/json'
            )

            return True
        except Exception as e:
            st.error(f"Error saving findings: {e}")
            return False


# =============================================================================
# STREAMLIT UI
# =============================================================================

def render_manual_research_page():
    """Main page for manual research document upload and analysis."""

    st.title("📄 Manual Research Upload")
    st.markdown("""
    **Cost-Effective Research Analysis**

    Upload HTML documents (saved webpages) for AI analysis. This approach costs ~95% less than
    autonomous web research while giving you full control over source quality.

    **Cost comparison:**
    - Autonomous agent: ~$0.50-1.00 per topic search
    - Manual upload: ~$0.02-0.05 per document
    """)

    # Check for API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        st.error("⚠️ ANTHROPIC_API_KEY not configured. Cannot perform analysis.")
        return

    # Initialize storage
    storage = DocumentStorage(S3_BUCKET)
    analyzer = ManualResearchAnalyzer(api_key)

    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📤 Upload Documents", "📊 Analyze Documents", "📈 View Findings"])

    with tab1:
        render_upload_tab(storage)

    with tab2:
        render_analysis_tab(storage, analyzer)

    with tab3:
        render_findings_tab()


def render_upload_tab(storage: DocumentStorage):
    """Tab for uploading research documents."""

    st.header("Upload Research Documents")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("""
        **How to save webpages as HTML:**
        1. In your browser, navigate to the article/report
        2. Press `Ctrl+S` (Windows) or `Cmd+S` (Mac)
        3. Choose "Webpage, HTML Only" or "Webpage, Complete"
        4. Save the file
        5. Upload it here
        """)

        # File uploader
        uploaded_files = st.file_uploader(
            "Choose HTML files",
            type=['html', 'htm', 'txt'],
            accept_multiple_files=True,
            help="Upload saved webpages (HTML files)"
        )

        # Category selector
        category = st.selectbox(
            "Research Category",
            RESEARCH_CATEGORIES,
            help="Categorize this research document"
        )

        # Optional source URL
        source_url = st.text_input(
            "Source URL (optional)",
            placeholder="https://example.com/article",
            help="Original URL of the webpage"
        )

        if uploaded_files and st.button("📤 Upload Documents", type="primary"):
            with st.spinner("Uploading documents..."):
                results = []
                for uploaded_file in uploaded_files:
                    file_content = uploaded_file.read()
                    result = storage.upload_document(
                        file_content=file_content,
                        filename=uploaded_file.name,
                        category=category,
                        source_url=source_url or None
                    )
                    results.append(result)

                # Show results
                success_count = sum(1 for r in results if r['success'])
                st.success(f"✅ Uploaded {success_count}/{len(results)} documents successfully!")

                if success_count < len(results):
                    st.error("Some uploads failed. Check errors below.")
                    for r in results:
                        if not r['success']:
                            st.error(f"Error: {r['error']}")

    with col2:
        st.subheader("📚 Recently Uploaded")
        documents = storage.list_uploaded_documents(days=7)

        if documents:
            st.metric("Total Documents (7 days)", len(documents))

            for doc in documents[:5]:
                with st.expander(f"{doc.get('category', 'Other')}: {doc.get('original_filename', 'Unknown')[:30]}..."):
                    st.write(f"**Uploaded:** {doc.get('uploaded_at', 'Unknown')[:10]}")
                    st.write(f"**Category:** {doc.get('category', 'Unknown')}")
                    if doc.get('source_url'):
                        st.write(f"**Source:** {doc['source_url']}")

                    if st.button("🗑️ Delete", key=f"del_{doc.get('doc_id')}"):
                        if storage.delete_document(doc['s3_key']):
                            st.success("Deleted!")
                            st.rerun()
        else:
            st.info("No documents uploaded yet")


def render_analysis_tab(storage: DocumentStorage, analyzer: ManualResearchAnalyzer):
    """Tab for analyzing uploaded documents."""

    st.header("Analyze Uploaded Documents")

    # Load available documents
    documents = storage.list_uploaded_documents(days=30)

    if not documents:
        st.info("📭 No documents available. Upload some documents first!")
        return

    st.write(f"**{len(documents)} documents available for analysis**")

    # Document selection
    st.subheader("Select Documents to Analyze")

    # Group by category for easier selection
    by_category = {}
    for doc in documents:
        cat = doc.get('category', 'Other')
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(doc)

    selected_docs = []

    for category, docs in by_category.items():
        with st.expander(f"{category} ({len(docs)} documents)", expanded=True):
            for doc in docs:
                if st.checkbox(
                    f"{doc.get('original_filename', 'Unknown')} - {doc.get('uploaded_at', '')[:10]}",
                    key=f"select_{doc.get('doc_id')}"
                ):
                    selected_docs.append(doc)

    # Analyze button
    if selected_docs:
        st.write(f"**{len(selected_docs)} documents selected**")

        # Cost estimate
        estimated_cost = len(selected_docs) * 0.04  # ~$0.04 per doc average
        st.info(f"💰 Estimated cost: ${estimated_cost:.2f}")

        if st.button("🔍 Analyze Selected Documents", type="primary"):
            with st.spinner(f"Analyzing {len(selected_docs)} documents..."):

                # Progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Analyze documents one by one to show progress
                results = {
                    'started_at': datetime.utcnow().isoformat(),
                    'documents_analyzed': 0,
                    'total_findings': 0,
                    'total_cost_usd': 0,
                    'total_tokens': 0,
                    'findings_by_category': {},
                    'all_findings': [],
                    'errors': []
                }

                for idx, doc in enumerate(selected_docs):
                    status_text.text(f"Analyzing: {doc.get('original_filename', 'Unknown')}")

                    try:
                        # Load and analyze
                        content = storage.get_document_content(doc['s3_key'])
                        analysis = analyzer.analyze_document(
                            content=content,
                            category=doc.get('category', 'Other'),
                            source_url=doc.get('source_url'),
                            filename=doc.get('original_filename')
                        )

                        if analysis['success']:
                            findings = analysis['findings']
                            results['documents_analyzed'] += 1
                            results['total_cost_usd'] += findings.get('estimated_cost_usd', 0)
                            results['total_tokens'] += findings.get('tokens_used', {}).get('total', 0)
                            results['all_findings'].append(findings)

                            category = findings.get('category', 'Other')
                            if category not in results['findings_by_category']:
                                results['findings_by_category'][category] = []
                            results['findings_by_category'][category].append(findings)

                        else:
                            results['errors'].append({
                                'document': doc.get('original_filename'),
                                'error': analysis.get('error')
                            })

                    except Exception as e:
                        results['errors'].append({
                            'document': doc.get('original_filename', 'unknown'),
                            'error': str(e)
                        })

                    progress_bar.progress((idx + 1) / len(selected_docs))

                results['completed_at'] = datetime.utcnow().isoformat()
                results['total_cost_usd'] = round(results['total_cost_usd'], 2)

                # Generate consolidated summary
                status_text.text("Generating consolidated summary...")
                summary = analyzer.generate_consolidated_summary(results)
                results['executive_summary'] = summary

                # Save to S3
                analyzer.save_findings_to_s3(results, S3_BUCKET)

                # Show results
                progress_bar.empty()
                status_text.empty()

                st.success(f"✅ Analysis complete! Processed {results['documents_analyzed']} documents")

                # Display summary metrics
                col1, col2, col3 = st.columns(3)
                col1.metric("Documents Analyzed", results['documents_analyzed'])
                col2.metric("Total Cost", f"${results['total_cost_usd']}")
                col3.metric("Total Tokens", f"{results['total_tokens']:,}")

                # Executive summary
                st.subheader("📋 Executive Summary")
                st.write(summary.get('executive_summary', 'No summary available'))

                # Top priorities
                if summary.get('top_priorities'):
                    st.subheader("🎯 Top Priorities")
                    for priority in summary['top_priorities']:
                        st.markdown(f"**{priority.get('priority')}** ({priority.get('urgency')} urgency)")
                        st.markdown(f"*Action:* {priority.get('action')}")
                        st.markdown("---")

                # Store in session state for viewing
                st.session_state['latest_analysis'] = results

                st.info("💾 Findings saved to S3. View them in the 'View Findings' tab.")


def render_findings_tab():
    """Tab for viewing analysis findings."""

    st.header("📈 Analysis Findings")

    # Check if we have recent analysis in session state
    if 'latest_analysis' in st.session_state:
        results = st.session_state['latest_analysis']

        st.subheader("Latest Analysis")
        st.write(f"Completed: {results.get('completed_at', 'Unknown')[:19]}")

        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Documents", results['documents_analyzed'])
        col2.metric("Findings", len(results['all_findings']))
        col3.metric("Cost", f"${results['total_cost_usd']}")
        col4.metric("Tokens", f"{results['total_tokens']:,}")

        # Executive summary
        if results.get('executive_summary'):
            st.subheader("📋 Executive Summary")
            summary = results['executive_summary']
            st.write(summary.get('executive_summary', 'No summary'))

            if summary.get('trends_identified'):
                st.write("**Trends Identified:**")
                for trend in summary['trends_identified']:
                    st.write(f"- {trend}")

        # Findings by category
        st.subheader("🔍 Findings by Category")

        for category, findings_list in results.get('findings_by_category', {}).items():
            with st.expander(f"{category} ({len(findings_list)} documents)", expanded=True):
                for finding in findings_list:
                    st.markdown(f"**Source:** {finding.get('source', 'Unknown')}")
                    st.markdown(f"**Summary:** {finding.get('summary', 'N/A')}")

                    if finding.get('key_findings'):
                        st.write("**Key Findings:**")
                        for kf in finding['key_findings']:
                            relevance_emoji = "🔴" if kf.get('relevance') == 'high' else "🟡" if kf.get('relevance') == 'medium' else "🟢"
                            st.write(f"{relevance_emoji} {kf.get('finding')}")
                            if kf.get('action_required'):
                                st.write(f"   ➡️ Action: {kf.get('recommended_action')}")

                    st.markdown("---")

        # Download as JSON
        json_data = json.dumps(results, indent=2, default=str)
        st.download_button(
            label="📥 Download Full Report (JSON)",
            data=json_data,
            file_name=f"research_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

    else:
        st.info("No analysis results yet. Analyze some documents first!")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    render_manual_research_page()
