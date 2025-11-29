import requests
import io

GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"


def batch_geocode(input_csv_buffer: io.StringIO) -> io.StringIO:
    """Run batch geocoding using the Census Geocoder API."""

    # send data to Census Geocoder API
    print("Running batch geocode...")
    files = {"addressFile": ("data.csv", input_csv_buffer, "text/csv")}
    data = {"benchmark": "4"}
    response = requests.post(GEOCODER_URL, files=files, data=data)
    response.raise_for_status()
    print("Geocoding complete.")

    # save results to output text buffer
    response_csv_buffer = io.StringIO()
    HEADER = """"record","address","match_type","match_level","matched_address","coordinates","place_id","side_of_street"\n"""
    response_csv_buffer.write(HEADER)  # prepend the header
    response_csv_buffer.write(response.text)
    response_csv_buffer.seek(0)
    return response_csv_buffer
