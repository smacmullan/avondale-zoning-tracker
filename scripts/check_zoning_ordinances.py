import csv
from typing import List, Dict
import pandas as pd
from src.ordinance import (
    download_zoning_ordinances,
    extract_address_from_title,
    format_ordinance_data_for_geocoding,
)
from src.util.geocode import run_batch_geocode

# Configuration
INTRODUCTION_DATE = "2025-01-01T00:00:00.000Z"
ORDINANCE_DOWNLOAD_FILE = "data/out/ordinance_export.csv"
ORDINANCE_DOWNLOAD_WITH_ADDRESSES_FILE = "data/out/ordinance_export_with_address.csv"
ADDRESSES_FILE = "data/out/addresses_for_geocoding.csv"
GEOCODED_ADDRESSES_FILE = "data/out/geocode_results.csv"


def save_to_csv(records: List[Dict], fieldnames: List[str], filename: str) -> None:
    """Save records to a CSV file."""
    if not records:
        print("No records to save.")
        return

    try:
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        print(f"Successfully saved {len(records)} records to {filename}")
    except IOError as e:
        print(f"Error writing to CSV file: {e}")


def main():
    # download zoning ordinance data
    records, fieldnames = download_zoning_ordinances(INTRODUCTION_DATE)
    save_to_csv(records, fieldnames, ORDINANCE_DOWNLOAD_FILE)

    # load export CSV and extract addresses
    df = pd.read_csv(ORDINANCE_DOWNLOAD_FILE)
    df["address"] = df["title"].apply(extract_address_from_title)
    df.to_csv(ORDINANCE_DOWNLOAD_WITH_ADDRESSES_FILE, index=False)

    # geocode
    format_ordinance_data_for_geocoding(df, ADDRESSES_FILE)
    run_batch_geocode(ADDRESSES_FILE, GEOCODED_ADDRESSES_FILE)


if __name__ == "__main__":
    main()
