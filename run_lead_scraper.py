#!/usr/bin/env python3
"""
Lead Scraper Routine — Carlile

Scrapes B2B leads from Apify's Leads Finder actor to identify Beneficial Cargo
Owners (BCOs) that ship physical goods to Alaska and/or Hawaii.

Usage:
    export APIFY_API_TOKEN="your_token_here"
    python3 run_lead_scraper.py [--trigger-text "custom instructions"]

Requires: Python 3.7+, requests library (pip install requests)
"""

import argparse
import base64
import csv
import io
import json
import os
import re
import smtplib
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    import requests
except ImportError:
    sys.exit("Error: 'requests' library required. Install with: pip install requests")


# ---------------------------------------------------------------------------
# Default configuration from CLAUDE.md
# ---------------------------------------------------------------------------

DEFAULT_JOB_TITLES = [
    "Director of Transportation",
    "Director of Logistics",
    "Director of Supply Chain",
    "Director of Distribution",
    "VP of Transportation",
    "VP of Logistics",
    "VP of Supply Chain",
    "VP of Operations",
    "Transportation Manager",
    "Logistics Manager",
    "Supply Chain Manager",
    "Shipping Manager",
    "Freight Manager",
    "Distribution Manager",
]

DEFAULT_SENIORITY = ["Director", "VP", "Manager", "Head"]

DEFAULT_INDUSTRIES = [
    "Automotive Parts & Accessories",
    "Building Materials",
    "Construction",
    "Consumer Goods",
    "Distribution",
    "Electrical & Electronic Manufacturing",
    "Food & Beverages",
    "Food Production",
    "Glass, Ceramics & Concrete",
    "Hardware",
    "Health, Wellness & Fitness",
    "Hospital & Health Care",
    "Industrial Automation",
    "Industrial Machinery & Equipment",
    "Machinery",
    "Manufacturing",
    "Medical Devices",
    "Mechanical or Industrial Engineering",
    "Mining & Metals",
    "Oil & Energy",
    "Packaging & Containers",
    "Paper & Forest Products",
    "Pharmaceuticals",
    "Plastics",
    "Renewables & Environment",
    "Retail",
    "Sporting Goods",
    "Supermarkets",
    "Telecommunications",
    "Wholesale",
]

EXCLUDED_INDUSTRIES = [
    "Logistics & Supply Chain",
    "Transportation/Trucking/Railroad",
    "Maritime",
    "Freight",
    "Warehousing",
    "Staffing & Recruiting",
]

DEFAULT_NUM_LEADS = 500
NOTIFICATION_EMAIL = "aicoachbox@gabrielomat.com"
POLL_INTERVAL_SEC = 30
POLL_TIMEOUT_SEC = 600  # 10 minutes
MAX_RETRIES = 1


# ---------------------------------------------------------------------------
# Trigger text parsing
# ---------------------------------------------------------------------------

def parse_trigger_text(text):
    """Parse optional trigger text to override default parameters."""
    overrides = {}
    if not text:
        return overrides

    # industries: [list]
    m = re.search(r"industries?\s*:\s*\[([^\]]+)\]", text, re.IGNORECASE)
    if m:
        overrides["industries"] = [i.strip().strip("'\"") for i in m.group(1).split(",")]

    # titles: [list]
    m = re.search(r"titles?\s*:\s*\[([^\]]+)\]", text, re.IGNORECASE)
    if m:
        overrides["titles"] = [t.strip().strip("'\"") for t in m.group(1).split(",")]

    # locations: [list]
    m = re.search(r"locations?\s*:\s*\[([^\]]+)\]", text, re.IGNORECASE)
    if m:
        overrides["locations"] = [l.strip().strip("'\"") for l in m.group(1).split(",")]

    # limit: number
    m = re.search(r"limit\s*:\s*(\d+)", text, re.IGNORECASE)
    if m:
        overrides["limit"] = int(m.group(1))

    # Natural language overrides
    m = re.search(r"[Ll]imit\s+to\s+(\d+)\s+leads", text)
    if m and "limit" not in overrides:
        overrides["limit"] = int(m.group(1))

    # Focus on titles: ...
    m = re.search(r"[Ff]ocus on titles?\s*:\s*(.+?)(?:\.|$)", text)
    if m and "titles" not in overrides:
        overrides["titles"] = [t.strip() for t in m.group(1).split(",")]

    # "building materials companies only" style
    m = re.search(r"[Ss]earch for (.+?) companies only", text)
    if m and "industries" not in overrides:
        overrides["industries"] = [m.group(1).strip()]

    return overrides


# ---------------------------------------------------------------------------
# Apify API helpers
# ---------------------------------------------------------------------------

def start_apify_run(token, params):
    """Start the Apify Leads Finder actor and return the run ID."""
    url = f"https://api.apify.com/v2/acts/code_crafter~leads-finder/runs?token={token}"
    resp = requests.post(url, json=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    run_id = data["data"]["id"]
    print(f"  Apify run started: {run_id}")
    return run_id


def poll_run_status(token, run_id):
    """Poll until the run succeeds, fails, or times out."""
    url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={token}"
    elapsed = 0
    while elapsed < POLL_TIMEOUT_SEC:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        status = resp.json()["data"]["status"]
        print(f"  Status: {status} (elapsed {elapsed}s)")
        if status == "SUCCEEDED":
            return "SUCCEEDED"
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            return status
        time.sleep(POLL_INTERVAL_SEC)
        elapsed += POLL_INTERVAL_SEC
    return "TIMEOUT"


def fetch_results(token, run_id):
    """Retrieve dataset items from a completed run."""
    url = f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items?token={token}"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# CSV formatting
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    "Full Name",
    "Job Title",
    "Email",
    "Company Name",
    "Company Industry",
    "City",
    "State",
    "LinkedIn URL",
]


def format_leads_csv(items):
    """Convert raw Apify items into a sorted, deduplicated CSV string."""
    seen_emails = set()
    rows = []

    for item in items:
        email = (item.get("email") or item.get("Email") or "").strip().lower()
        if not email or email in seen_emails:
            continue
        seen_emails.add(email)

        first = item.get("firstName", item.get("first_name", ""))
        last = item.get("lastName", item.get("last_name", ""))
        full_name = f"{first} {last}".strip() or item.get("name", item.get("fullName", ""))

        city = item.get("city", item.get("company_city", ""))
        state = item.get("state", item.get("company_state", ""))

        rows.append({
            "Full Name": full_name,
            "Job Title": item.get("title", item.get("jobTitle", item.get("job_title", ""))),
            "Email": email,
            "Company Name": item.get("companyName", item.get("company_name", item.get("company", ""))),
            "Company Industry": item.get("companyIndustry", item.get("company_industry", item.get("industry", ""))),
            "City": city,
            "State": state,
            "LinkedIn URL": item.get("linkedinUrl", item.get("linkedin_url", item.get("linkedInUrl", ""))),
        })

    # Sort by Company Name then Job Title
    rows.sort(key=lambda r: (r["Company Name"].lower(), r["Job Title"].lower()))

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue(), rows


# ---------------------------------------------------------------------------
# Google Drive upload (via API — requires service account or OAuth token)
# ---------------------------------------------------------------------------

def upload_to_google_drive_placeholder(csv_content, filename, folder_id):
    """
    Placeholder for Google Drive upload.

    In the Claude Code MCP environment, Google Drive upload is handled via the
    mcp__Google-Drive__create_file tool. When running this script standalone,
    you would need a Google Drive API service account or OAuth credentials.

    The folder has already been created:
      Folder ID: 1wWzebqsgdDrEjIruzkMMYjFyqhiU3Nfq
      Folder URL: https://drive.google.com/drive/folders/1wWzebqsgdDrEjIruzkMMYjFyqhiU3Nfq
    """
    print(f"  [Google Drive] CSV ready for upload: {filename}")
    print(f"  [Google Drive] Target folder ID: {folder_id}")
    print(f"  [Google Drive] CSV size: {len(csv_content)} bytes, {csv_content.count(chr(10))} lines")
    return csv_content, filename


# ---------------------------------------------------------------------------
# Email notification
# ---------------------------------------------------------------------------

def build_email_summary(rows, industries_used, run_id, filename, drive_url, errors):
    """Build the email body summarizing the run."""
    company_counts = Counter(r["Company Name"] for r in rows)
    top_10 = company_counts.most_common(10)

    industry_set = sorted(set(r["Company Industry"] for r in rows if r["Company Industry"]))

    body_lines = [
        f"Lead Scraper run completed successfully.",
        f"",
        f"Run ID: {run_id}",
        f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"Total leads found: {len(rows)}",
        f"",
        f"Industries covered ({len(industry_set)}):",
    ]
    for ind in industry_set:
        body_lines.append(f"  - {ind}")

    body_lines.append("")
    body_lines.append("Top 10 companies by number of contacts:")
    for i, (company, count) in enumerate(top_10, 1):
        body_lines.append(f"  {i}. {company} ({count} contacts)")

    if drive_url:
        body_lines.append("")
        body_lines.append(f"Google Drive file: {drive_url}")

    if len(rows) < 10:
        body_lines.append("")
        body_lines.append("WARNING: Fewer than 10 leads found. Filters may need adjustment.")

    if errors:
        body_lines.append("")
        body_lines.append("Errors/Warnings:")
        for err in errors:
            body_lines.append(f"  - {err}")

    return "\n".join(body_lines)


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Lead Scraper Routine — Carlile")
    parser.add_argument("--trigger-text", "-t", default="", help="Trigger text with custom overrides")
    parser.add_argument("--dry-run", action="store_true", help="Print config and exit without calling API")
    parser.add_argument("--output-dir", "-o", default=".", help="Directory to save CSV output locally")
    args = parser.parse_args()

    token = os.environ.get("APIFY_API_TOKEN", "")
    if not token and not args.dry_run:
        sys.exit("Error: APIFY_API_TOKEN environment variable is required")

    # Build parameters
    overrides = parse_trigger_text(args.trigger_text)

    params = {
        "contact_job_title": overrides.get("titles", DEFAULT_JOB_TITLES),
        "seniority_level": DEFAULT_SENIORITY,
        "contact_location": overrides.get("locations", ["United States"]),
        "company_industry": overrides.get("industries", DEFAULT_INDUSTRIES),
        "company_not_industry": EXCLUDED_INDUSTRIES,
        "email_status": "validated",
        "numberOfLeads": overrides.get("limit", DEFAULT_NUM_LEADS),
    }

    print("=" * 60)
    print("LEAD SCRAPER ROUTINE — CARLILE")
    print("=" * 60)
    print(f"Date:       {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Leads:      {params['numberOfLeads']}")
    print(f"Industries: {len(params['company_industry'])}")
    print(f"Titles:     {len(params['contact_job_title'])}")
    if args.trigger_text:
        print(f"Trigger:    {args.trigger_text}")
    print()

    if args.dry_run:
        print("DRY RUN — request body:")
        print(json.dumps(params, indent=2))
        return

    errors = []
    drive_folder_id = "1wWzebqsgdDrEjIruzkMMYjFyqhiU3Nfq"

    # Step 1: Start Apify run
    print("[Step 1] Starting Apify Leads Finder...")
    run_id = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            run_id = start_apify_run(token, params)
            break
        except Exception as e:
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES:
                print("  Retrying...")
                time.sleep(5)
            else:
                sys.exit(f"Error: Apify run failed after {MAX_RETRIES + 1} attempts: {e}")

    # Step 2: Poll for completion
    print("\n[Step 2] Waiting for Apify run to complete...")
    status = poll_run_status(token, run_id)
    if status != "SUCCEEDED":
        # Retry once per CLAUDE.md instructions
        print(f"  Run ended with status: {status}. Retrying...")
        try:
            run_id = start_apify_run(token, params)
            status = poll_run_status(token, run_id)
        except Exception as e:
            sys.exit(f"Error: Retry also failed: {e}")
        if status != "SUCCEEDED":
            sys.exit(f"Error: Apify run failed with status: {status}")

    # Step 3: Retrieve results
    print("\n[Step 3] Retrieving results...")
    items = fetch_results(token, run_id)
    print(f"  Raw items retrieved: {len(items)}")

    if not items:
        errors.append("No leads returned from Apify.")
        print("  WARNING: No leads found!")

    # Step 4: Format CSV
    print("\n[Step 4] Formatting CSV...")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    filename = f"leads_{timestamp}.csv"
    csv_content, rows = format_leads_csv(items)
    print(f"  Formatted leads: {len(rows)} (after deduplication)")
    print(f"  Filename: {filename}")

    # Save locally
    local_path = os.path.join(args.output_dir, filename)
    with open(local_path, "w", newline="") as f:
        f.write(csv_content)
    print(f"  Saved locally: {local_path}")

    if len(rows) < 10:
        errors.append("Fewer than 10 leads found — filters may need adjustment.")

    # Step 5: Google Drive upload
    print("\n[Step 5] Google Drive upload...")
    print(f"  CSV file saved locally: {local_path}")
    print(f"  Target Google Drive folder: https://drive.google.com/drive/folders/{drive_folder_id}")
    print(f"  To upload via Claude Code MCP, use mcp__Google-Drive__create_file with:")
    print(f"    title: {filename}")
    print(f"    mimeType: text/csv")
    print(f"    parentId: {drive_folder_id}")
    print(f"    content: <base64-encoded CSV>")

    # Output base64 for MCP upload
    csv_b64 = base64.b64encode(csv_content.encode("utf-8")).decode("ascii")
    b64_path = os.path.join(args.output_dir, f"{filename}.b64")
    with open(b64_path, "w") as f:
        f.write(csv_b64)
    print(f"  Base64 file saved: {b64_path}")

    drive_url = f"https://drive.google.com/drive/folders/{drive_folder_id}"

    # Step 6: Email summary
    print("\n[Step 6] Email notification...")
    industries_used = params["company_industry"]
    email_body = build_email_summary(rows, industries_used, run_id, filename, drive_url, errors)
    email_subject = f"Lead Scraper Complete — {len(rows)} leads found"

    print(f"  To: {NOTIFICATION_EMAIL}")
    print(f"  Subject: {email_subject}")
    print(f"\n--- Email Body ---")
    print(email_body)
    print(f"--- End Email Body ---")

    # Summary
    print("\n" + "=" * 60)
    print("WORKFLOW COMPLETE")
    print("=" * 60)
    print(f"Leads found:    {len(rows)}")
    print(f"CSV file:       {local_path}")
    print(f"Base64 file:    {b64_path}")
    print(f"Drive folder:   {drive_url}")
    print(f"Errors:         {len(errors)}")
    for err in errors:
        print(f"  - {err}")

    return {
        "csv_path": local_path,
        "b64_path": b64_path,
        "filename": filename,
        "num_leads": len(rows),
        "run_id": run_id,
        "email_subject": email_subject,
        "email_body": email_body,
        "drive_folder_id": drive_folder_id,
        "errors": errors,
    }


if __name__ == "__main__":
    main()
