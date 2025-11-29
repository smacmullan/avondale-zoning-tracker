import pandas as pd
import duckdb
from src.ordinance import (
    download_zoning_ordinances,
    extract_address_from_title,
    get_address_data_for_geocoding,
)
from src.util.geocode import batch_geocode

# filepath configuration
INTRODUCTION_DATE = "2025-01-01T00:00:00.000Z"
ORDINANCE_EXPORT_CSV = "data/out/ordinance_export.csv"
AVONDALE_ZONING_CSV = "data/out/avondale_zoning.csv"
CITYWIDE_ZONING_CSV = "data/out/citywide_zoning.csv"

# get ordinance data
records, fieldnames = download_zoning_ordinances(INTRODUCTION_DATE)
df_ordinances = pd.DataFrame.from_records(records, columns=fieldnames)
df_ordinances["address"] = df_ordinances["title"].apply(extract_address_from_title)
df_ordinances.to_csv(ORDINANCE_EXPORT_CSV, index=False)

# geocode
csv_buffer = get_address_data_for_geocoding(df_ordinances)
geocoded_csv_data = batch_geocode(csv_buffer)
df_geocode = pd.read_csv(geocoded_csv_data, skip_blank_lines=True)

# Connect to DuckDB and enable spatial extension
con = duckdb.connect()
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
print("DuckDB started. Loading geodata...")

# Load ordinances and geocode into DuckDB
con.register("ordinances", df_ordinances)
con.register("geocode_raw", df_geocode)
# Extract geocode coordinates in a subquery
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


# Read in community areas and wards geospatial data
COMMUNITIES_GEOJSON_FILE = "data/external_data/ChicagoCommunityAreas.geojson"
WARDS_GEO_CSV_FILE = "data/external_data/ChicagoWardBoundaries(2023-).csv"
con.execute(f"""
CREATE OR REPLACE TABLE communities AS
SELECT * FROM ST_Read('{COMMUNITIES_GEOJSON_FILE}');
""")
con.execute(f"""
CREATE OR REPLACE TABLE wards AS
SELECT * EXCLUDE the_geom,
    ST_GeomFromText(the_geom) AS geom
FROM read_csv_auto('{WARDS_GEO_CSV_FILE}');
""")
print("Starting spatial analysis...")


# Join geocode, community area name, and ward number to ordinances
con.execute("""
CREATE OR REPLACE TABLE zoning_requests AS
    SELECT
        o.recordNumber,
        o.address AS bill_address, 
        o.status,
        o.subStatus,
        o.introductionDate,
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
CREATE OR REPLACE TABLE avondale_buffer AS
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
CREATE OR REPLACE TABLE points_within_avondale AS
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
    f"COPY (SELECT * EXCLUDE geom FROM points_within_avondale) TO '{AVONDALE_ZONING_CSV}' (HEADER TRUE);"
)
row = con.execute("SELECT COUNT(*) FROM points_within_avondale").fetchone()
count = row[0] if row is not None else 0
print(f"Wrote {AVONDALE_ZONING_CSV}. {count} records identified.")

con.close()
