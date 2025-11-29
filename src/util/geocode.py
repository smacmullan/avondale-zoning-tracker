import requests

GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"


def run_batch_geocode(input_path: str, output_path: str):
    """Run batch geocoding using the Census Geocoder API."""

    print(f"Running batch geocode for {input_path}...")
    with open(input_path, "rb") as f:
        files = {"addressFile": (input_path, f, "text/csv")}
        data = {"benchmark": "4"}
        response = requests.post(GEOCODER_URL, files=files, data=data)
        response.raise_for_status()

    with open(output_path, "wb") as out:
        HEADER = """"record","address","match_type","match_level","matched_address","coordinates","place_id","side_of_street"\n"""
        out.write(HEADER.encode("utf-8"))  # prepend the header
        out.write(response.content)

    print(f"Saved results to {output_path}")
