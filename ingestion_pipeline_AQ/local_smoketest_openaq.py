#!/usr/bin/env python3
import argparse
import os
import sys
import time
from typing import Iterable, List, Tuple, Optional, Dict, Any
from dotenv import load_dotenv
import requests

load_dotenv()

OPENAQ_BASE = "https://api.openaq.org/v3"

def get_with_backoff(s: requests.Session, url: str, params=None, max_tries=6, base_delay=0.5):
    """Makes a GET request with exponential backoff for retries."""
    delay = base_delay
    for i in range(1, max_tries + 1):
        try:
            r = s.get(url, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                if i == max_tries:
                    r.raise_for_status()
                time.sleep(delay)
                delay = min(10.0, round(delay * 1.8, 2))
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if i == max_tries:
                print(f"   ERROR: Request for {url} failed after {max_tries} tries. {e}", file=sys.stderr)
            time.sleep(delay)
            delay = min(10.0, round(delay * 1.8, 2))
    return None

def list_locations_in_bbox(s: requests.Session, bbox: str, limit=100) -> List[dict]:
    """Lists locations within a bounding box that have PM2.5 sensors."""
    params = {
        "bbox": bbox,
        "parameters_id": 2,  # PM2.5 parameter ID
        "limit": limit,
    }
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations", params=params)
    if not r:
        return []
    return r.json().get("results", [])

def get_latest_for_location(s: requests.Session, location_id: int) -> Optional[Tuple]:
    """Fetches the latest PM2.5 measurement for a given location."""
    url = f"{OPENAQ_BASE}/locations/{location_id}/latest"
    r = get_with_backoff(s, url)
    if not r:
        return None
    
    latest_data = r.json().get("results", [])
    if not latest_data:
        return None
        
    for measurement in latest_data[0].get("measurements", []):
        if measurement.get("parameter") == "pm25":
            value = measurement.get("value")
            unit = measurement.get("unit", "µg/m³")
            when = measurement.get("lastUpdated")
            return (value, unit, when)
    return None

def main():
    p = argparse.ArgumentParser(description="Fetches latest PM2.5 data for locations in CA metropolitan areas.")
    p.add_argument("--api-key", default=os.getenv("OPENAQ_API_KEY"), help="OpenAQ API key.")
    p.add_argument("--per-bbox", type=int, default=15, help="Max locations to check per bounding box.")
    p.add_argument("--sleep-ms", type=int, default=300, help="Delay between API calls in milliseconds.")
    args = p.parse_args()

    if not args.api_key:
        print("API key is required. Use --api-key or set OPENAQ_API_KEY.", file=sys.stderr)
        sys.exit(2)

    s = requests.Session()
    s.headers.update({"X-API-Key": args.api_key})
    
    # Bounding boxes for major CA cities
    bboxes = [
        "-118.6681,33.7037,-117.6462,34.3373",  # Los Angeles
        "-122.5136,37.7080,-122.3569,37.8324",  # San Francisco
        "-117.282,32.615,-116.908,33.023",      # San Diego
    ]
    
    all_data = []
    for bbox in bboxes:
        print(f"\nFinding locations in bbox: {bbox}", flush=True)
        locations = list_locations_in_bbox(s, bbox, limit=args.per_bbox)
        
        for loc in locations:
            loc_id = loc["id"]
            loc_name = loc.get("name", "Unknown Location")
            print(f"  Fetching latest data for: {loc_name} (ID: {loc_id})", flush=True)
            
            latest_measurement = get_latest_for_location(s, loc_id)
            if latest_measurement:
                val, unit, when = latest_measurement
                all_data.append((loc_id, loc_name, val, unit, when))
            
            time.sleep(args.sleep_ms / 1000.0)
            
    if not all_data:
        print("\nNo recent PM2.5 data was found for any locations in the specified areas.")
        return

    all_data.sort(key=lambda r: (r[4] or "1970-01-01T00:00:00Z"), reverse=True)
    
    print("\n--- Latest PM2.5 Data from California Locations ---")
    print(f"{'Location ID':<12} {'Location Name':<35} {'Value':<10} {'Unit':<10} {'DateTime (UTC)':<30}")
    print("-" * 100)
    for loc_id, loc_name, val, unit, when in all_data:
        val_str = f"{val:.2f}" if val is not None else "N/A"
        print(f"{loc_id:<12} {loc_name[:35]:<35} {val_str:<10} {unit:<10} {when or 'N/A'}")

if __name__ == "__main__":
    main()
