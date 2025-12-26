import re
import pandas as pd
import requests
import io
from typing import List, Dict


def _fetch_zoning_ordinance_data(change_date: str) -> List[Dict]:
    """Fetch zoning ordinance data from the Chicago City Clerk eLMS API."""
    API_BASE_URL = "https://api.chicityclerkelms.chicago.gov/matter"
    SEARCH_QUERY = "zoning"
    all_records = []
    skip = 0
    top = 500  # max allowed by API

    # API pagination loop
    while True:
        params = {
            "filter": f"lastPublicationDate gt {change_date} and type eq 'Ordinance'",
            "search": SEARCH_QUERY,
            "skip": skip,
            "top": top,
            "sort": "introductionDate",
        }

        headers = {"accept": "application/json; charset=utf-8"}

        try:
            response = requests.get(API_BASE_URL, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            records = data.get("data", [])
            all_records.extend(records)

            meta = data.get("meta", {})
            total_count = meta.get("count", 0)

            # Check if records remain to be fetched
            if skip + top >= total_count:
                break

            skip += top

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    return all_records


# Fields to extract from each record
FIELDS_TO_KEEP = [
    "matterId",
    "recordNumber",
    "status",
    "subStatus",
    "introductionDate",
    "finalActionDate",
    "title",
    "recordCreateDate",
    "matterCategory",
    "lastPublicationDate",
]


def _extract_fields(records: List[Dict]) -> List[Dict]:
    """Extract only a simplified list of fields from each record."""
    extracted = []
    for record in records:
        filtered_record = {field: record.get(field, "") for field in FIELDS_TO_KEEP}
        extracted.append(filtered_record)
    return extracted


def _filter_by_category(
    records: List[Dict], category: str = "ZONING RECLASSIFICATIONS"
) -> List[Dict]:
    """Filter records to only include those with the specified matterCategory."""
    return [r for r in records if (r.get("matterCategory", "") == category)]
    # Note: some categories have extra text (e.g., "ZONING RECLASSIFICATIONS | Opposition") but those usually aren't zoning reclassifications
    # return [record for record in records if category in record.get("matterCategory", "")]


def download_zoning_ordinances(change_date: str) -> tuple[List[Dict], List[str]]:
    """Download zoning ordinances from the Chicago City Clerk eLMS."""
    print("Fetching zoning ordinance data...")
    records = _fetch_zoning_ordinance_data(change_date)

    if records:
        print(f"Retrieved {len(records)} records. Filtering by category...")
        filtered_records = _filter_by_category(records)
        print(
            f"Found {len(filtered_records)} zoning reclassification records. Extracting fields..."
        )
        extracted_records = _extract_fields(filtered_records)
        return extracted_records, FIELDS_TO_KEEP
    else:
        print("No records retrieved from the API.")
        return [], FIELDS_TO_KEEP


# Address regex statements (compiled once)
stname_pattern = r"(?:\b[^\d\W_][\w.'-]*\b\s){1,4}"
sttype_pattern = (
    r"\b(?:ave|blvd|cres|ct|dr|hwy|ln|pkwy|pl|plz|rd|row|sq|st|ter|way)\.?\b"
)
st_pattern = stname_pattern + sttype_pattern

addr_pattern = rf"((?<!-)\b\d{{1,5}}(?:-\d{{1,5}})?\s{st_pattern})"
intersec_pattern = rf"((?<=\sat\s){st_pattern}\s?and\s?{st_pattern})"
pattern = rf"({addr_pattern}|{intersec_pattern})"

ADDRESS_RE = re.compile(pattern, re.IGNORECASE)
DASH_RE = re.compile(r"\b(\d{1,5})-\d{1,5}")


def extract_address_from_title(ordinance_title: str):
    """Extract street address from ordinance title using regex."""
    matches = ADDRESS_RE.findall(ordinance_title)
    if not matches:
        return None

    address = matches[0][0]  # get first address matched

    # Remove "-##" if text is a range of numbered street addresses (not an intersection)
    if " and " not in address.lower():
        address = DASH_RE.sub(r"\1", address)

    return address


def get_address_data_for_geocoding(df: pd.DataFrame) -> io.StringIO:
    """Parse an ordinance dataframe's address information for batching geocoding."""
    # keep only rows with an address
    df_filtered = df[df["address"].notna() & (df["address"] != "")]

    # build export dataframe
    address_data = pd.DataFrame(
        {
            "recordNumber": df_filtered["recordNumber"],
            "STREET": df_filtered["address"],
            "CITY": "Chicago",
            "STATE": "IL",
            "ZIP": "",
        }
    )

    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    address_data.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    return csv_buffer
