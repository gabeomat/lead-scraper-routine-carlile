#!/usr/bin/env python3
"""Lead Scraper Routine — scrapes B2B leads from Apify's Leads Finder actor."""

import csv
import os
import sys
import time
from datetime import datetime

import requests

APIFY_BASE_URL = "https://api.apify.com/v2"
ACTOR_ID = "code_crafter~leads-finder"
POLL_INTERVAL = 30  # seconds
POLL_TIMEOUT = 1800  # 30 minutes — Apify runs for 500 leads can exceed 15 min

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

DEFAULT_PAYLOAD = {
    "contact_job_title": [
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
    ],
    "seniority_level": ["director", "vp", "manager", "head"],
    "contact_location": ["united states"],
    "company_industry": [
        "automotive",
        "building materials",
        "construction",
        "consumer goods",
        "electrical/electronic manufacturing",
        "food & beverages",
        "food production",
        "glass, ceramics & concrete",
        "health, wellness & fitness",
        "hospital & health care",
        "industrial automation",
        "machinery",
        "medical devices",
        "mechanical or industrial engineering",
        "mining & metals",
        "oil & energy",
        "packaging & containers",
        "paper & forest products",
        "pharmaceuticals",
        "plastics",
        "renewables & environment",
        "retail",
        "sporting goods",
        "supermarkets",
        "telecommunications",
        "wholesale",
    ],
    "company_not_industry": [
        "logistics & supply chain",
        "transportation/trucking/railroad",
        "maritime",
        "package/freight delivery",
        "warehousing",
        "staffing & recruiting",
    ],
    "email_status": ["validated"],
    "numberOfLeads": 100,
}


def get_api_token():
    token = os.environ.get("APIFY_API_TOKEN", "").strip()
    if not token:
        print("ERROR: APIFY_API_TOKEN environment variable is not set.")
        sys.exit(1)
    return token


def start_actor_run(token):
    url = f"{APIFY_BASE_URL}/acts/{ACTOR_ID}/runs?token={token}"
    print("[Step 1] Starting Apify Leads Finder actor...")
    try:
        resp = requests.post(url, json=DEFAULT_PAYLOAD, timeout=60)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to start actor run: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"  Response: {e.response.text[:3000]}")
        return None

    data = resp.json().get("data", {})
    run_id = data.get("id")
    if not run_id:
        print(f"ERROR: No run ID in response: {resp.text[:500]}")
        return None

    print(f"  Started actor run: {run_id}")
    return run_id


def poll_run_status(token, run_id):
    url = f"{APIFY_BASE_URL}/actor-runs/{run_id}?token={token}"
    print("\n[Step 2] Polling for completion...")
    start_time = time.time()

    while True:
        elapsed = int(time.time() - start_time)
        if elapsed > POLL_TIMEOUT:
            print(f"  TIMEOUT: Run did not complete within {POLL_TIMEOUT}s.")
            return False

        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            status = resp.json().get("data", {}).get("status", "UNKNOWN")
        except requests.exceptions.RequestException as e:
            print(f"  Warning: Poll request failed ({e}), will retry...")
            time.sleep(POLL_INTERVAL)
            continue

        print(f"  Status: {status} ({elapsed}s elapsed)")

        if status == "SUCCEEDED":
            return True
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"  Run ended with status: {status}")
            return False

        time.sleep(POLL_INTERVAL)


def fetch_results(token, run_id):
    url = f"{APIFY_BASE_URL}/actor-runs/{run_id}/dataset/items?token={token}"
    print("\n[Step 3] Retrieving results...")
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch results: {e}")
        return []

    results = resp.json()
    print(f"  Retrieved {len(results)} raw leads")

    if results:
        print(f"  Sample lead keys: {list(results[0].keys())}")

    return results


def _get_field(record, *keys, default=""):
    for key in keys:
        val = record.get(key)
        if val:
            return str(val).strip()
    return default


def format_leads(raw_leads):
    print("\n[Step 4] Formatting results...")
    seen_emails = set()
    leads = []

    for record in raw_leads:
        # Extract full name
        full_name = _get_field(record, "fullName", "full_name", "name")
        if not full_name:
            first = _get_field(record, "firstName", "first_name")
            last = _get_field(record, "lastName", "last_name")
            full_name = f"{first} {last}".strip()

        # Extract email and deduplicate
        email = _get_field(record, "email", "Email", "emailAddress").lower().strip()
        if not email or email in seen_emails:
            continue
        seen_emails.add(email)

        # Extract city and state
        city = _get_field(record, "city", "City", "companyCity", "company_city")
        state = _get_field(record, "state", "State", "companyState", "company_state")
        if not city or not state:
            location = _get_field(record, "location", "companyLocation", "company_location")
            if location and "," in location:
                parts = [p.strip() for p in location.rsplit(",", 1)]
                if len(parts) == 2:
                    if not city:
                        city = parts[0]
                    if not state:
                        state = parts[1]

        lead = {
            "Full Name": full_name,
            "Job Title": _get_field(record, "title", "jobTitle", "job_title", "Title"),
            "Email": email,
            "Company Name": _get_field(record, "companyName", "company_name", "company", "Company"),
            "Company Industry": _get_field(record, "companyIndustry", "company_industry", "industry", "Industry"),
            "City": city,
            "State": state,
            "LinkedIn URL": _get_field(record, "linkedinUrl", "linkedin_url", "profileUrl", "linkedin", "LinkedinUrl"),
        }
        leads.append(lead)

    # Sort by Company Name, then Job Title
    leads.sort(key=lambda x: (x["Company Name"].lower(), x["Job Title"].lower()))

    print(f"  After dedup and formatting: {len(leads)} leads")
    if 0 < len(leads) < 10:
        print("  WARNING: Fewer than 10 leads found. Filters may need adjustment.")

    return leads


def save_csv(leads):
    print("\n[Step 5] Saving CSV...")
    filename = f"leads_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
    print(f"  Saved {len(leads)} leads to {filename}")
    return filename


def main():
    print("=== Lead Scraper Routine ===")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    token = get_api_token()

    # Start the actor run — retry only the START call if it errors (network/4xx
    # on submit). Never retry after a run has actually started, because each
    # new run consumes Apify credits. A polling timeout does NOT trigger retry.
    run_id = start_actor_run(token)
    if not run_id:
        print("\n--- Retrying start (attempt 2 of 2) ---\n")
        run_id = start_actor_run(token)
    if not run_id:
        print("\nERROR: Could not start Apify run after 2 attempts.")
        sys.exit(1)

    success = poll_run_status(token, run_id)
    if not success:
        print(f"\nERROR: Apify run {run_id} did not succeed. Check the run in")
        print("the Apify console — it may still be running. Do NOT re-run this")
        print("script until you confirm the run ended, or you'll burn credits.")
        sys.exit(1)

    raw_leads = fetch_results(token, run_id)
    if not raw_leads:
        print("\nERROR: No leads returned from Apify.")
        sys.exit(1)

    leads = format_leads(raw_leads)
    if not leads:
        print("\nERROR: No valid leads after formatting.")
        sys.exit(1)

    filename = save_csv(leads)

    print("\n=== Summary ===")
    print(f"Leads found: {len(leads)}")
    print(f"Output file: {filename}")
    print("\nNOTE: Google Drive upload and email notification are handled outside this script.")


if __name__ == "__main__":
    main()
