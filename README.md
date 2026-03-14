# NEXUS ASIA DISTRESS RADAR
### Automated Distress Signal Intelligence Platform

> Continuously crawls India's regulatory bodies, financial media, and bank auction portals for insolvency signals, CIRP filings, SARFAESI notices, and asset auctions. Converts raw web data into structured deal intelligence — running automatically every 30 minutes, free, in the cloud.

---

## ARCHITECTURE

```
Internet Sources
    │
    ▼
GitHub Actions (Cron: */30 * * * *)
    │
    ▼
Python Crawlers (requests + BeautifulSoup)
    │
    ▼
Signal Detection (keyword matching + entity extraction)
    │
    ▼
Supabase (PostgreSQL database)
    │
    ▼
Static Dashboard (HTML + JS → reads Supabase REST API)
```

---

## FOLDER STRUCTURE

```
nexus-asia-distress-radar/
│
├── crawler.py                    # Main orchestrator
│
├── crawlers/
│   ├── __init__.py               # Crawler registry
│   ├── base.py                   # Abstract base class + keyword config
│   ├── economic_times.py         # Economic Times crawler
│   ├── business_standard.py      # Business Standard crawler
│   ├── mint.py                   # LiveMint crawler
│   ├── reuters.py                # Reuters India crawler
│   ├── ibbi.py                   # IBBI regulatory crawler
│   ├── regulatory.py             # NCLT + MCA crawlers
│   └── bank_auctions.py          # IBAPI + SBI auction crawlers
│
├── db/
│   ├── client.py                 # Supabase REST client
│   └── schema.sql                # Full database schema
│
├── dashboard/
│   └── dashboard.html            # Standalone dashboard
│
├── .github/
│   └── workflows/
│       └── crawl.yml             # GitHub Actions automation
│
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## STEP-BY-STEP DEPLOYMENT (Beginner Friendly)

### STEP 1 — Create a Supabase Project (Free)

1. Go to **https://supabase.com** and sign up (free)
2. Click **"New Project"**
3. Name it: `nexus-distress-radar`
4. Choose a region close to you (e.g., Singapore for India)
5. Set a database password (save it somewhere safe)
6. Wait ~2 minutes for the project to provision

**Get your credentials:**
- Go to **Project Settings → API**
- Copy **Project URL** → this is your `SUPABASE_URL`
- Copy **anon public** key → this is your `SUPABASE_ANON_KEY`

### STEP 2 — Run the Database Schema

1. In Supabase, go to **SQL Editor** (left sidebar)
2. Click **"New Query"**
3. Open the file `db/schema.sql` from this project
4. Paste the entire contents into the SQL editor
5. Click **"Run"** (green button)
6. You should see: `Success. No rows returned`

This creates all tables, indexes, policies, views, and seed data.

### STEP 3 — Create a GitHub Repository

1. Go to **https://github.com** and sign in
2. Click **"New repository"**
3. Name: `nexus-asia-distress-radar`
4. Set to **Private** (recommended)
5. Click **"Create repository"**

**Upload the project:**

Option A — GitHub Web UI (easiest):
- Click "uploading an existing file"
- Drag and drop all files from this folder
- Maintain the folder structure

Option B — Git CLI:
```bash
cd nexus-asia-distress-radar
git init
git add .
git commit -m "Initial commit: Nexus Asia Distress Radar"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/nexus-asia-distress-radar.git
git push -u origin main
```

### STEP 4 — Add GitHub Secrets

This is how the crawler accesses your Supabase database without hardcoding credentials.

1. Go to your GitHub repository
2. Click **Settings** (top tab)
3. Left sidebar: **Secrets and variables → Actions**
4. Click **"New repository secret"**

Add these two secrets:

| Name | Value |
|------|-------|
| `SUPABASE_URL` | Your Supabase Project URL (e.g., `https://abcxyz.supabase.co`) |
| `SUPABASE_ANON_KEY` | Your Supabase anon/public key |

### STEP 5 — Enable GitHub Actions

1. In your repository, click the **Actions** tab
2. If prompted "Workflows aren't running", click **"I understand my workflows, go ahead and enable them"**
3. You should see **"Nexus Asia Distress Radar — Crawler"** listed

**Test it manually:**
1. Click on the workflow name
2. Click **"Run workflow"** (right side)
3. Keep `dry_run` as `false`
4. Click the green **"Run workflow"** button
5. Watch the run logs — you should see crawlers executing

After this, the workflow will run **automatically every 30 minutes** with no action required.

### STEP 6 — Set Up the Dashboard

The dashboard is a single HTML file that connects directly to Supabase.

**You need to insert your credentials into the dashboard:**

Open `dashboard/dashboard.html` and find these two lines near the bottom:

```javascript
const SUPABASE_URL = window.SUPABASE_URL || "YOUR_SUPABASE_URL";
const SUPABASE_ANON_KEY = window.SUPABASE_ANON_KEY || "YOUR_SUPABASE_ANON_KEY";
```

Replace the placeholder values with your actual Supabase credentials:

```javascript
const SUPABASE_URL = "https://YOUR_PROJECT_ID.supabase.co";
const SUPABASE_ANON_KEY = "your_actual_anon_key";
```

**Host the dashboard for free:**

Option A — GitHub Pages (easiest, free):
1. Rename `dashboard.html` to `index.html`
2. Move it to the root of the repo or a `/docs` folder
3. Go to **Settings → Pages**
4. Source: **Deploy from a branch → main → /root (or /docs)**
5. Your dashboard will be live at `https://YOUR_USERNAME.github.io/nexus-asia-distress-radar/`

Option B — Netlify Drop (instant, free):
1. Go to **https://app.netlify.com/drop**
2. Drag and drop the `dashboard.html` file
3. Netlify gives you a URL instantly

Option C — Open locally:
- Just open `dashboard.html` in your browser

---

## LOCAL DEVELOPMENT

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/nexus-asia-distress-radar.git
cd nexus-asia-distress-radar

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.example .env
# Edit .env with your actual Supabase credentials

# Run crawler (writes to DB)
python crawler.py

# Dry run (no DB writes, just shows what would be detected)
python crawler.py --dry-run

# Run a specific crawler only
python crawler.py --source ibbi
python crawler.py --source "economic times"
```

---

## ADDING NEW CRAWLERS

To add a new source:

1. Create `crawlers/my_new_source.py`:

```python
from .base import BaseCrawler, DistressEvent
import requests
from bs4 import BeautifulSoup

class MyNewSourceCrawler(BaseCrawler):
    SOURCE_NAME = "My Source Name"
    SOURCE_URL = "https://example.com"
    CATEGORY = "financial_media"  # or regulatory, auction, legal, other

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()
        
        resp = self.safe_get(session, "https://example.com/news")
        if not resp:
            return events
            
        soup = BeautifulSoup(resp.text, "html.parser")
        
        for article in soup.find_all("a", href=True):
            headline = article.get_text(strip=True)
            keywords = self.detect_keywords(headline)
            
            if keywords:
                companies = self.extract_company_names(headline)
                kw, category = keywords[0]
                events.append(self.make_event(
                    company_name=companies[0] if companies else "Unknown",
                    keyword=kw,
                    category=category,
                    url=article["href"],
                    headline=headline,
                ))
        
        return events
```

2. Register in `crawlers/__init__.py`:

```python
from .my_new_source import MyNewSourceCrawler

ALL_CRAWLERS = [
    ...
    MyNewSourceCrawler,  # Add here
]
```

That's it. The crawler will run automatically on the next scheduled execution.

---

## MONITORED SOURCES

| Source | Type | Coverage |
|--------|------|----------|
| Economic Times | Financial Media | India |
| Business Standard | Financial Media | India |
| Mint (LiveMint) | Financial Media | India |
| Reuters | Financial Media | Global |
| IBBI | Regulatory | India |
| NCLT | Regulatory | India |
| MCA | Regulatory | India |
| IBAPI Auctions | Bank Auctions | India |
| SBI e-Auctions | Bank Auctions | India |

---

## DETECTED SIGNAL CATEGORIES

| Category | Keywords |
|----------|----------|
| insolvency | insolvency, insolvent |
| cirp | cirp, corporate insolvency resolution process, resolution professional |
| liquidation | liquidation, liquidator, winding up, wound up |
| sarfaesi | sarfaesi, symbolic possession, secured creditor notice |
| default | default, npa, non-performing, stressed loan |
| distressed_asset | distressed asset, distressed sale |
| restructuring | restructuring, debt restructuring, ots, haircut |
| debt_resolution | debt resolution, resolution plan |
| creditor_action | creditor action, drt, enforcement action |
| asset_auction | auction, e-auction, bank auction, reserve price |
| nclt | nclt, ibc, insolvency code |
| bankruptcy | bankruptcy, bankrupt |

---

## DATABASE TABLES

| Table | Purpose |
|-------|---------|
| `distress_events` | Core signal log — every detected event |
| `companies` | Normalized company registry |
| `assets` | Distressed assets for auction/sale |
| `sources` | Crawler source registry |
| `crawler_runs` | Audit log of all crawl executions |

---

## COST

**100% Free** using:
- GitHub Free tier: 2,000 Actions minutes/month (30-min crawls use ~500 mins/month)
- Supabase Free tier: 500MB database, 2GB bandwidth/month
- GitHub Pages or Netlify: Free static hosting

---

## TROUBLESHOOTING

**Crawlers run but no data appears in dashboard:**
- Check that your dashboard has the correct `SUPABASE_URL` and `SUPABASE_ANON_KEY`
- Verify the schema was applied correctly (check Supabase Table Editor)

**GitHub Actions failing:**
- Go to Actions → failed run → click on the step to see logs
- Most common issue: secrets not set correctly (Step 4 above)

**No events being detected:**
- Some sources may block automated requests temporarily
- Run `python crawler.py --dry-run` locally to debug
- Check that keywords appear in the source content

**Duplicate events:**
- The system deduplicates by company + keyword + source per day
- The `is_duplicate` column flags known duplicates

---

*Built for distressed asset investors tracking India's IBC/SARFAESI ecosystem.*
