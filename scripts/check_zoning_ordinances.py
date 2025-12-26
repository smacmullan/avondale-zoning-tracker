import pandas as pd
import duckdb
from src.ordinance import (
    download_zoning_ordinances,
    extract_address_from_title,
    get_address_data_for_geocoding,
)
from src.util.geocode import batch_geocode
import os
import sys

# filepath configuration
ORDINANCE_EXPORT_CSV = "data/out/ordinance_export.csv"
AVONDALE_ZONING_CSV = "data/out/avondale_zoning.csv"
CITYWIDE_ZONING_CSV = "data/out/citywide_zoning.csv"

# create data/out directory if it doesn't exist
os.makedirs("data/out", exist_ok=True)

# connect to DuckDB and enable spatial extension
con = duckdb.connect("data/out/data.db")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
print("DuckDB started. ")

# check if ordinances table exists
result = con.execute("""
    SELECT COUNT(*) 
    FROM information_schema.tables 
    WHERE lower(table_name) = 'ordinances'
""").fetchone()
ordinance_table_exists = result is not None and result[0] > 0
# ordinance table existing serves as a proxy for geocode table existing
geocode_table_exists = ordinance_table_exists

# get last change date
last_change_date = "2025-01-01T00:00:00.000Z"  # default if no cached data
if ordinance_table_exists:
    result = con.execute("SELECT MAX(lastPublicationDate) FROM ordinances").fetchone()
    if result:
        last_change_date = result[0]
print(f"Getting changes since {last_change_date}")

# get and update ordinance data
records, fieldnames = download_zoning_ordinances(last_change_date)
df_ordinances = pd.DataFrame.from_records(records, columns=fieldnames)
df_ordinances["address"] = df_ordinances["title"].apply(extract_address_from_title)

if df_ordinances.empty:
    print("No new ordinance changes. Exiting script.")
    con.close()
    sys.exit()

if ordinance_table_exists:
    # upsert incoming records (update existing records, insert new records)
    con.execute("INSERT OR REPLACE INTO ordinances SELECT * FROM df_ordinances")
else:
    # first run, create new table
    con.execute("CREATE TABLE ordinances AS SELECT * FROM df_ordinances")
    con.execute("ALTER TABLE ordinances ADD PRIMARY KEY (recordNumber)")

# export ordinance data to CSV
con.execute(
    f"COPY (SELECT * from ordinances) TO '{ORDINANCE_EXPORT_CSV}' (HEADER TRUE);"
)
print(f"Wrote {ORDINANCE_EXPORT_CSV}.")


# geocode addresses
csv_buffer = get_address_data_for_geocoding(df_ordinances)
geocoded_csv_data = batch_geocode(csv_buffer)
df_geocode = pd.read_csv(geocoded_csv_data, skip_blank_lines=True)
con.register("geocode_raw", df_geocode)

# Extract geocode coordinates in a subquery
if geocode_table_exists:
    con.execute("""
    INSERT OR REPLACE INTO geocode
    SELECT
        TRIM(record) AS recordNumber,
        matched_address,
        coordinates,
        CAST(str_split(coordinates, ',')[1] AS DOUBLE) AS lon,
        CAST(str_split(coordinates, ',')[2] AS DOUBLE) AS lat,
        ST_Point(lon, lat) AS geom        
    FROM geocode_raw
    WHERE geocode_raw.coordinates IS NOT NULL AND geocode_raw.coordinates <> ''
    """)
else:
    con.execute("""
    CREATE OR REPLACE TABLE geocode AS
    SELECT
        TRIM(record) AS recordNumber,
        matched_address,
        coordinates,
        CAST(str_split(coordinates, ',')[1] AS DOUBLE) AS lon,
        CAST(str_split(coordinates, ',')[2] AS DOUBLE) AS lat,
        ST_Point(lon, lat) AS geom        
    FROM geocode_raw
    WHERE geocode_raw.coordinates IS NOT NULL AND geocode_raw.coordinates <> ''
    """)
    con.execute("ALTER TABLE geocode ADD PRIMARY KEY (recordNumber)")


# Read in community areas and wards geospatial data
COMMUNITIES_GEOJSON_FILE = "data/external_data/ChicagoCommunityAreas.geojson"
WARDS_GEO_CSV_FILE = "data/external_data/ChicagoWardBoundaries(2023-).csv"
con.execute(f"""
CREATE TABLE IF NOT EXISTS communities AS
SELECT * FROM ST_Read('{COMMUNITIES_GEOJSON_FILE}');
""")
con.execute(f"""
CREATE TABLE IF NOT EXISTS wards AS
SELECT * EXCLUDE the_geom,
    ST_GeomFromText(the_geom) AS geom
FROM read_csv_auto('{WARDS_GEO_CSV_FILE}');
""")
print("Starting spatial analysis...")


# Join geocode, community area name, and ward number to ordinances
# "isStale" denotes requests that are likely denied (not passed in 180 days; some requests that would include affordable housing have a 360 day window)
con.execute("""
CREATE OR REPLACE TABLE zoning_requests AS
    SELECT
        o.recordNumber,
        billAddress: o.address, 
        o.status,
        o.subStatus,
        o.introductionDate,
        passDate: o.finalActionDate,
        isStale: (subStatus = 'Referred' AND CAST(introductionDate AS TIMESTAMPTZ) < now() - INTERVAL '180 days'),
        g.lon, 
        g.lat,
        w.ward "ward",
        c.community,
        o.title, 
        o.matterId,
        CONCAT('https://chicityclerkelms.chicago.gov/matter/?matterId=', o.matterId) AS url,
        g.geom   
    FROM ordinances o
        LEFT JOIN geocode g USING (recordNumber)
        LEFT JOIN communities c
            ON ST_Contains(c.geom, g.geom)
        LEFT JOIN wards w
            ON ST_Contains(w.geom, g.geom)
""")

# Export citywide zoning requests to CSV
con.execute(
    f"COPY (SELECT * EXCLUDE geom from zoning_requests) TO '{CITYWIDE_ZONING_CSV}' (HEADER TRUE);"
)
print(f"Wrote {CITYWIDE_ZONING_CSV}.")


# create a buffered geometry around Avondale community area
# project to EPSG:26971 – NAD83 / Illinois East (meters)
# geojson uses [lon, lat] order, so set always_xy := true
BUFFER_DISTANCE_METERS = 300
con.execute(
    """
CREATE TABLE IF NOT EXISTS avondale_buffer AS
SELECT ST_Buffer(
            ST_Transform(geom, 'EPSG:4326', 'EPSG:26971', always_xy := true)
            ,?
        ) AS geom
FROM communities
WHERE UPPER(community) = 'AVONDALE';
""",
    [BUFFER_DISTANCE_METERS],
)

# filter on points on avondale buffer, removing points that are across the river (North Center)
con.execute("""
CREATE VIEW IF NOT EXISTS zoning_in_avondale AS
SELECT z.*
FROM zoning_requests z
WHERE ST_Contains(
        (SELECT geom FROM avondale_buffer LIMIT 1),
        ST_Transform(z.geom, 'EPSG:4326', 'EPSG:26971', always_xy := true)
    )
    AND community != 'NORTH CENTER';
""")

# export Avondale zoning to CSV
con.execute(
    f"COPY (SELECT * EXCLUDE geom FROM zoning_in_avondale) TO '{AVONDALE_ZONING_CSV}' (HEADER TRUE);"
)
row = con.execute("SELECT COUNT(*) FROM zoning_in_avondale").fetchone()
count = row[0] if row is not None else 0
print(f"Wrote {AVONDALE_ZONING_CSV}. {count} records identified.")

# get and print recent changes
recent_changes = con.execute(
    """
    SELECT *
    FROM zoning_in_avondale
    WHERE introductionDate >= $1
           OR passDate >= $1
    """,
    [last_change_date],
).fetchall()

if len(recent_changes) > 0:
    print("\nRecent Avondale zoning changes:")
    for record in recent_changes:
        address = record[1]
        ward = record[9]
        neighborhood = record[10]
        status = record[3]
        link = record[13]
        print(f"   {address} ({ward}, {neighborhood}) - {status}")
else:
    print("\nNo recent Avondale zoning changes.")

con.close()
