# Lead Scraper Routine — Carlile

## Purpose
This routine scrapes B2B leads from Apify's Leads Finder actor to identify Beneficial Cargo Owners (BCOs) — companies that ship physical goods to Alaska and/or Hawaii and need carrier services.

## Target Profile

### Companies (Beneficial Cargo Owners)
These are vendors, manufacturers, distributors, and retailers headquartered in the lower 48 US states that supply products to Alaska and Hawaii. Examples: Safelite, Grainger, Fastenal, Home Depot.

Target industries (use these as Leads Finder `company_industry` filters):
- Automotive Parts & Accessories
- Building Materials
- Construction
- Consumer Goods
- Distribution
- Electrical & Electronic Manufacturing
- Food & Beverages
- Food Production
- Glass, Ceramics & Concrete
- Hardware
- Health, Wellness & Fitness
- Hospital & Health Care
- Industrial Automation
- Industrial Machinery & Equipment
- Machinery
- Manufacturing
- Medical Devices
- Mechanical or Industrial Engineering
- Mining & Metals
- Oil & Energy
- Packaging & Containers
- Paper & Forest Products
- Pharmaceuticals
- Plastics
- Renewables & Environment
- Retail
- Sporting Goods
- Supermarkets
- Telecommunications
- Wholesale

Exclude these industries (use as `company_not_industry`):
- Logistics & Supply Chain
- Transportation/Trucking/Railroad
- Maritime
- Freight
- Warehousing
- Staffing & Recruiting

### Decision Maker Titles
Search for contacts with these seniority levels and functional areas:

Seniority levels: Director, VP, Manager, Head

Target title keywords (use in `contact_job_title`):
- Director of Transportation
- Director of Logistics
- Director of Supply Chain
- Director of Distribution
- VP of Transportation
- VP of Logistics
- VP of Supply Chain
- VP of Operations
- Transportation Manager
- Logistics Manager
- Supply Chain Manager
- Shipping Manager
- Freight Manager
- Distribution Manager
- Operations Manager (at relevant companies)

### Location
- Country: United States
- All 50 states are valid for company HQ location (the BCO can be anywhere in the lower 48)

### Required Output Fields
For each lead, capture:
- Full name
- Job title
- Email (verified)
- Company name
- Company industry
- Company location (city, state)
- LinkedIn URL (if available)

## Workflow Steps

### Step 1: Call Apify Leads Finder API
Use the Apify API to run the Leads Finder actor (code_crafter/leads-finder).

API endpoint:
```
POST https://api.apify.com/v2/acts/code_crafter~leads-finder/runs?token=${APIFY_API_TOKEN}
```

Request body — construct based on the trigger input or use defaults:
```json
{
  "contact_job_title": ["Director of Transportation", "Director of Logistics", "Director of Supply Chain", "Director of Distribution", "VP of Transportation", "VP of Logistics", "VP of Supply Chain", "VP of Operations", "Transportation Manager", "Logistics Manager", "Supply Chain Manager", "Shipping Manager", "Freight Manager", "Distribution Manager"],
  "seniority_level": ["Director", "VP", "Manager", "Head"],
  "contact_location": ["United States"],
  "company_industry": ["Automotive Parts & Accessories", "Building Materials", "Construction", "Consumer Goods", "Distribution", "Electrical & Electronic Manufacturing", "Food & Beverages", "Food Production", "Glass, Ceramics & Concrete", "Hardware", "Health, Wellness & Fitness", "Hospital & Health Care", "Industrial Automation", "Industrial Machinery & Equipment", "Machinery", "Manufacturing", "Medical Devices", "Mechanical or Industrial Engineering", "Mining & Metals", "Oil & Energy", "Packaging & Containers", "Paper & Forest Products", "Pharmaceuticals", "Plastics", "Renewables & Environment", "Retail", "Sporting Goods", "Supermarkets", "Telecommunications", "Wholesale"],
  "company_not_industry": ["Logistics & Supply Chain", "Transportation/Trucking/Railroad", "Maritime", "Freight", "Warehousing", "Staffing & Recruiting"],
  "email_status": "validated",
  "numberOfLeads": 500
}
```

If the trigger `text` includes custom parameters (specific industries, titles, number of leads, etc.), override the defaults accordingly.

### Step 2: Wait for Apify Run to Complete
Poll the run status:
```
GET https://api.apify.com/v2/actor-runs/{runId}?token=${APIFY_API_TOKEN}
```
Wait until status is "SUCCEEDED". Check every 30 seconds. Timeout after 10 minutes.

### Step 3: Retrieve Results
Fetch the dataset:
```
GET https://api.apify.com/v2/actor-runs/{runId}/dataset/items?token=${APIFY_API_TOKEN}
```

### Step 4: Format Results
Create a CSV file with these columns:
- Full Name
- Job Title
- Email
- Company Name
- Company Industry
- City
- State
- LinkedIn URL

Sort by Company Name, then by Job Title.

Remove any duplicates (same email address).

### Step 5: Save to Google Drive
Use the Google Drive MCP connector to:
1. Create/find a folder called "Lead Scraper Results" in Google Drive
2. Save the CSV file with the naming convention: `leads_YYYY-MM-DD_HH-MM.csv`

### Step 6: Send Email Notification
Use the Google Drive or available email connector to notify aicoachbox@gabrielomat.com with:
- Subject: "Lead Scraper Complete — [number] leads found"
- Body: Summary of the run including:
  - Number of leads found
  - Industries covered
  - Top 10 companies by number of contacts found
  - Link to the file in Google Drive
  - Any errors or warnings

## Customization via Trigger Input
When triggered via API, the `text` field can include overrides:
- `industries: [list]` — override target industries
- `titles: [list]` — override target titles
- `locations: [list]` — override target locations
- `limit: [number]` — override number of leads (default 500)
- `keywords: [list]` — add keyword filters

Example API trigger with custom input:
```json
{
  "text": "Search for building materials companies only. Limit to 200 leads. Focus on titles: Director of Logistics, VP Supply Chain."
}
```

## Error Handling
- If the Apify run fails, retry once. If it fails again, send an email notification with the error details.
- If Google Drive save fails, output the CSV content to the session so it can be retrieved manually.
- If fewer than 10 leads are found, note this in the email as the filters may need adjustment.
