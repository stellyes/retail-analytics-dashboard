# 📊 Retail Analytics Dashboard

A comprehensive retail analytics platform for cannabis dispensaries with AI-powered research and SEO analysis capabilities.

## Overview

This Streamlit-based dashboard provides:
- 📈 **Sales Analytics** - Multi-store sales tracking and analysis
- 🤖 **AI-Powered Insights** - Natural language queries with Claude
- 📄 **Manual Research** - Cost-effective industry research ($0.02-0.05 per document)
- 🔍 **SEO Analysis** - On-demand SEO monitoring for your websites
- 🔬 **Industry Research** - View historical autonomous research findings

## Features

### Core Dashboard
- Multi-store analytics (Barbary Coast & Grass Roots)
- Password authentication with role-based access
- S3 integration for data persistence
- Sales trends, top products, brand performance
- Product mapping tools
- Promotional analysis

### Manual Research System
**Cost: ~$0.02-0.05 per document (95% cheaper than autonomous agents)**

- Upload HTML documents (saved webpages)
- AI analysis with Claude Haiku
- Batch processing (5-15 documents at once)
- Findings saved to S3
- Executive summaries and actionable insights
- Full cost tracking and transparency

### SEO Analysis
**Cost: ~$0.05-0.10 per analysis**

- Manual SEO analysis for barbarycoastsf.com and grassrootssf.com
- Comprehensive SEO scoring across 5 categories:
  - Technical SEO
  - On-Page SEO
  - Local SEO (cannabis-specific)
  - Content & Keywords
  - User Experience
- Results saved to S3 for historical tracking
- Side-by-side website comparison
- Actionable recommendations and quick wins

### Industry Research (Historical)
- View findings from previous autonomous research agents
- Data persists in S3 even after agents are removed
- Organized by topic and date

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Secrets

Create `.streamlit/secrets.toml`:

```toml
[passwords]
admin = "your-hashed-password"
analyst = "your-hashed-password"

[aws]
bucket_name = "retail-data-bcgr"
region = "us-west-1"
```

### 3. Set Environment Variables

```bash
# Required for AI features
export ANTHROPIC_API_KEY="sk-ant-..."

# AWS credentials (or use IAM role)
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_DEFAULT_REGION="us-west-1"

# Optional
export S3_BUCKET_NAME="retail-data-bcgr"
```

### 4. Run Dashboard

```bash
streamlit run app.py
```

Navigate to `http://localhost:8501`

## Usage

### Manual Research Workflow

**Recommended: Weekly research routine**

1. **Monday** - Collect articles (15 min)
   - Browse MJBizDaily, Cannabis Business Times, etc.
   - Save 5-8 articles as HTML (Ctrl+S → "Webpage, HTML Only")
   - Upload to dashboard via **📄 Manual Research** page

2. **Friday** - Analyze & review (15 min)
   - Select all week's documents
   - Click "Analyze Selected Documents"
   - Review executive summary
   - Extract top 3-5 insights for reporting

**Cost: ~$0.50-0.75 per week = $2-3 per month**

### SEO Analysis Workflow

**Recommended: Weekly analysis**

1. Navigate to **🔍 SEO Analysis** page
2. Select website (Barbary Coast SF or Grassroots SF)
3. Go to **🔄 Manual Analysis** tab
4. Click "Analyze [Website]"
5. Wait 30-60 seconds
6. Review results and priorities
7. Track progress in **📈 Trend Analysis** tab

**Cost: ~$0.10-0.20 per week (both sites) = $0.40-0.80 per month**

## Project Structure

```
retail-analytics-dashboard/
├── app.py                           # Main Streamlit dashboard
├── claude_integration.py            # Claude AI for analytics queries
├── research_integration.py          # Historical research findings viewer
├── seo_integration.py               # SEO analysis with manual trigger
├── manual_research_integration.py   # Manual document upload & analysis
├── requirements.txt                 # Python dependencies
├── .streamlit/
│   └── secrets.toml.example         # Configuration template
├── .gitignore
├── Dockerfile                       # For containerization (optional)
└── README.md                        # This file
```

## Data Storage (S3)

```
s3://retail-data-bcgr/
├── sales-data/                      # CSV uploads
│   ├── barbary_coast/
│   └── grass_roots/
├── product-mappings/                # Product normalization
│   └── mappings.json
├── research-documents/              # Uploaded HTML files
│   └── YYYY/MM/DD/
│       ├── {doc_id}_article.html
│       └── {doc_id}_metadata.json
├── research-findings/
│   ├── manual/                      # Manual research analysis
│   │   └── YYYY/MM/DD/
│   │       └── analysis_{time}.json
│   └── [historical autonomous findings]
└── seo-analysis/
    ├── barbarycoastsf.com/
    │   ├── YYYY/MM/DD/
    │   │   └── seo-findings.json
    │   └── summary/latest.json
    └── grassrootssf.com/
        ├── YYYY/MM/DD/
        │   └── seo-findings.json
        └── summary/latest.json
```

## Cost Breakdown

### Monthly Costs (Estimated)

| Component | Usage | Monthly Cost |
|-----------|-------|--------------|
| **Streamlit Dashboard** | Always running | $0-20 (Streamlit Cloud) |
| **AWS S3** | Storage | $1-5 |
| **Manual Research** | 15 docs/week | **$2-3** |
| **SEO Analysis** | 2 sites, weekly | **$0.40-0.80** |
| **Claude Analytics** | Occasional queries | $1-2 |
| **Total** | | **$5-30/month** |

**Compare to previous autonomous agents: $120-300/month**
**Savings: $90-270/month (90-95% reduction)**

## Key Integrations

### 1. Manual Research Integration ✅
- File: `manual_research_integration.py`
- Page: **📄 Manual Research**
- Features:
  - Upload HTML documents
  - Batch AI analysis with Haiku
  - S3 storage
  - Executive summaries
  - Cost tracking

### 2. SEO Integration ✅
- File: `seo_integration.py`
- Page: **🔍 SEO Analysis**
- Features:
  - Manual SEO analysis button
  - Analysis for both websites
  - Results saved to S3
  - Historical tracking
  - Side-by-side comparison

### 3. Industry Research (View Only)
- File: `research_integration.py`
- Page: **🔬 Industry Research**
- Features:
  - View historical autonomous research
  - Read-only access to S3 findings

## Development

### Adding New Features

1. Create new module (e.g., `new_feature_integration.py`)
2. Add import to `app.py`:
   ```python
   try:
       from new_feature_integration import render_new_feature
       NEW_FEATURE_AVAILABLE = True
   except ImportError:
       NEW_FEATURE_AVAILABLE = False
   ```
3. Add navigation item:
   ```python
   if NEW_FEATURE_AVAILABLE:
       nav_options.append("🎯 New Feature")
   ```
4. Add routing:
   ```python
   elif page == "🎯 New Feature":
       if NEW_FEATURE_AVAILABLE:
           render_new_feature()
   ```

### Local Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Set API key
export ANTHROPIC_API_KEY="sk-ant-..."

# Run dashboard
streamlit run app.py
```

## Deployment

### Streamlit Cloud

1. Push to GitHub
2. Connect to Streamlit Cloud
3. Add secrets in Streamlit Cloud dashboard
4. Deploy

### Self-Hosted

```bash
# Using Docker
docker build -t retail-analytics .
docker run -p 8501:8501 \
  -e ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  retail-analytics
```

## Troubleshooting

### Manual Research Issues

**Problem**: Upload fails
- **Solution**: Ensure file is .html or .htm format
- Try "Webpage, HTML Only" when saving
- Check file size (< 10MB)

**Problem**: Analysis costs more than expected
- **Solution**: Very long documents use more tokens
- Consider excerpting key sections only

### SEO Analysis Issues

**Problem**: "No data available"
- **Solution**: Run manual analysis first
- Check AWS credentials in secrets.toml
- Verify S3 bucket access

**Problem**: Analysis takes too long
- **Solution**: Normal for comprehensive SEO analysis (30-60s)
- Check ANTHROPIC_API_KEY is valid

### Dashboard Issues

**Problem**: Can't connect to S3
- **Solution**:
  - Check AWS credentials
  - Verify bucket exists: `aws s3 ls s3://retail-data-bcgr/`
  - Check region in secrets.toml

**Problem**: Claude integration not working
- **Solution**:
  - Verify ANTHROPIC_API_KEY environment variable
  - Check API key has sufficient credits
  - Review rate limits at console.anthropic.com

## Security

**Important Notes:**
- Never commit secrets to git
- Use `.gitignore` to exclude:
  - `.streamlit/secrets.toml`
  - `*.env` files
  - `*.json` response files
- Hash passwords in `secrets.toml`
- Use IAM roles when possible instead of access keys
- Rotate API keys regularly

## Research Sources

**Recommended daily/weekly sources:**
- [MJBizDaily](https://mjbizdaily.com) - Industry news
- [Cannabis Business Times](https://www.cannabisbusinesstimes.com) - Market trends
- [Marijuana Moment](https://www.marijuanamoment.net) - Regulatory updates
- [California DCC](https://cannabis.ca.gov) - Official regulations
- [Headset Analytics](https://www.headset.io/blog) - Market data

**How to save articles:**
1. Navigate to article
2. Press `Ctrl+S` (Windows) or `Cmd+S` (Mac)
3. Choose "Webpage, HTML Only"
4. Save to folder
5. Upload to dashboard

## Support & Maintenance

### Weekly Checklist
- [ ] Upload 5-10 research articles
- [ ] Analyze documents (Friday)
- [ ] Run SEO analysis for both sites
- [ ] Review findings with team
- [ ] Archive important insights

### Monthly Checklist
- [ ] Review total costs (should be < $30)
- [ ] Export findings as JSON for records
- [ ] Update product mappings if needed
- [ ] Clean up old S3 files (if desired)

## Future Enhancements

Potential features to add:
- [ ] PDF upload support for research
- [ ] Email digest of weekly findings
- [ ] Automated Google Alerts → S3 pipeline
- [ ] Competitor website tracking
- [ ] Trend visualization dashboard
- [ ] Integration with Google Analytics
- [ ] Automated monthly reports

## License

Internal use only - Cannabis retail analytics platform

## Contact

For questions or support, contact your development team.

---

**Last Updated**: December 2025
**Version**: 2.0 (Manual Research & SEO Integration)
