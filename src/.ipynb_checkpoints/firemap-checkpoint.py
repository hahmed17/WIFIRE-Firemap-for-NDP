"""
WIFIRE Firemap data retrieval utilities.
https://firemap.sdsc.edu/

Provides two functions:
  - fetch_fire_perimeters()  : Historical fire perimeter polygons via WFS
  - fetch_weather()          : Weather observations via pylaski station API
"""

import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon, box, Point

import time
import json
import numpy as np

import geojson
from datetime import datetime
from pathlib import Path
import matplotlib.pyplot as plt
from pyproj import Transformer

import zipfile
from osgeo import gdal, osr
import io
import subprocess
import shapely
from io import StringIO

import contextily as ctx

from config import *
import warnings

# ============================================================================
# PERIMETER RETRIEVAL
# ============================================================================

def _multipolygon_to_polygon(geom):
    """Return the largest polygon from a MultiPolygon, or the polygon itself."""
    if isinstance(geom, Polygon):
        return geom
    elif isinstance(geom, MultiPolygon):
        return max(geom.geoms, key=lambda g: g.area)
    else:
        raise TypeError(f"Unsupported geometry type: {type(geom)}")


def fetch_fire_perimeters(fire_name, year=2025, geoserver_layer='WIFIRE:view_historical_fires', verbose=True):
    """
    Fetch all mapped perimeters for a fire from WIFIRE Firemap GeoServer (WFS).

    Args:
        fire_name: Fire name exactly as it appears in the database (e.g. "BORDER 2")
        year: Fire year (e.g. 2025)
        verbose: Print progress
        synthetic: Loads the synthetic fire from the local filesystem; no Firemap query

    Returns:
        GeoDataFrame with columns including 'datetime', 'acres', 'geometry',
        in EPSG:5070, sorted oldest to newest.
    """
    if verbose:
        print(f"Fetching perimeters for '{fire_name}' ({year})...")

    params = {
        "SERVICE":      "WFS",
        "VERSION":      "2.0.0",
        "REQUEST":      "GetFeature",
        "TYPENAMES":    f"{geoserver_layer}",
        "CQL_FILTER":   f"fire_name = '{fire_name}'",  # Add year filter after updating data
        "OUTPUTFORMAT": "application/json",
        "SRSNAME":      "EPSG:4326",
    }

    warnings.warn(
        "The 'year' parameter is currently not used for filtering perimeters. "
        "Ensure the fire name is unique or includes the year if needed."
    )

    response = requests.get(FIREMAP_WFS_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    features = data.get("features", [])
    if not features:
        raise ValueError(
            f"No perimeters found for fire_name='{fire_name}', year={year}.\n"
            f"Check the fire name is an exact case-sensitive match."
        )

    if verbose:
        print(f"  Retrieved {len(features)} perimeter(s)")

    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")

    # Parse datetime — format is "2025-01-24Z" or "2025-01-24T00:00:00Z"
    gdf['datetime'] = pd.to_datetime(
        gdf['perimeter_timestamp'].str.rstrip('Z'), utc=False
    )

    # MultiPolygon -> largest single Polygon
    gdf['geometry'] = gdf['geometry'].apply(_multipolygon_to_polygon)

    # Sort oldest to newest and reindex
    gdf = gdf.sort_values('datetime', ascending=True).reset_index(drop=True)

    # Reproject to FARSITE CRS
    gdf = gdf.to_crs(FARSITE_CRS)

    if verbose:
        print(f"\n✓ {len(gdf)} perimeters ready")
        print(f"  Oldest: {gdf['datetime'].iloc[0]}")
        print(f"  Newest: {gdf['datetime'].iloc[-1]}")
        print(f"  Area range: "
              f"{gdf.geometry.area.min()/1e6:.2f} – "
              f"{gdf.geometry.area.max()/1e6:.2f} km²")
        print(f"\n  Perimeter timeline:")
        for i, row in gdf.iterrows():
            print(f"    [{i}] {row['datetime'].date()}  —  "
                  f"{row.geometry.area/1e6:.2f} km²  "
                #   f"({row['acres']:.0f} acres)"
                    )
    return gdf


# ============================================================================
# WEATHER RETRIEVAL
# ============================================================================
def query_weather_for_timestep(lat, lon, start_time, end_time, verbose=False):
    """
    Query weather data for a specific timestep.
    
    Args:
        lat: Latitude (WGS84)
        lon: Longitude (WGS84)
        start_time: Start datetime (pandas Timestamp)
        end_time: End datetime (pandas Timestamp)
        verbose: Print query details
        synthetic: Loads the synthetic weather data from the local filesystem
        
    Returns:
        tuple: (wind_speed_list, wind_direction_list)
    """
    # Convert to ISO format
    start_iso = start_time.isoformat()
    end_iso = end_time.isoformat()
    
    if verbose:
        print(f"  Querying weather: {start_time} to {end_time}")


    # Query Firemap API
    timestamp = int(time.time() * 1000)
    wx_params = {
        'selection': 'closestTo',
        'lat': str(lat),
        'lon': str(lon),
        'observable': ['wind_speed', 'wind_direction'],
        'from': start_iso,
        'to': end_iso,
        'callback': 'wxData',
        '_': str(timestamp)
    }

    try:
        wx_response = requests.get(FIREMAP_WX_URL, params=wx_params, timeout=10)
        wx_text = wx_response.text.strip()
        
        # Remove JSONP wrapper
        if wx_text.startswith('wxData(') and wx_text.endswith(')'):
            wx_json = wx_text[len('wxData('):-1]
            wx_obs = json.loads(wx_json)
        else:
            wx_obs = wx_response.json()
        
        wind_speed_list = wx_obs["features"][0]["properties"]["wind_speed"]
        wind_direction_list = wx_obs["features"][0]["properties"]["wind_direction"]
        
        if verbose:
            print(f"  Retrieved {len(wind_speed_list)} observations")
            print(f"  Wind: {np.mean(wind_speed_list):.1f} mph @ {np.mean(wind_direction_list):.0f}°")
        
        return wind_speed_list, wind_direction_list
        
    except Exception as e:
        print(f"  WARNING: Weather query failed: {e}")
        print(f"  Using fallback values")
        # Return fallback values
        return [10.0], [225.0]  # Default 10 mph from SW


def fetch_weather(lat, lon, start_dt, end_dt, verbose=True):
    """
    Fetch weather observations from WIFIRE Firemap pylaski station API.

    Queries the nearest weather stations to the given location and returns
    wind speed and direction observations for the given time window.

    Args:
        lat: Latitude (WGS84)
        lon: Longitude (WGS84)
        start_dt: Start datetime (datetime object or ISO string)
        end_dt: End datetime (datetime object or ISO string)
        verbose: Print progress

    Returns:
        dict with keys:
            'windspeed'     : wind speed in mph (float)
            'winddirection' : wind direction in degrees (float)
            'observations'  : raw DataFrame of all observations
        Falls back to config defaults if no data is retrieved.
    """
    from config import DEFAULT_HUMIDITY, DEFAULT_TEMPERATURE

    if verbose:
        print(f"  Querying weather: {start_dt} to {end_dt}")

    # Convert datetimes to strings if needed
    if hasattr(start_dt, 'strftime'):
        start_str = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
        end_str   = end_dt.strftime('%Y-%m-%dT%H:%M:%S')
    else:
        start_str = str(start_dt)
        end_str   = str(end_dt)

    params = {
        'latitude':  lat,
        'longitude': lon,
        'start':     start_str,
        'end':       end_str,
        'features':  'wind',
    }

    try:
        response = requests.get(FIREMAP_WX_URL, params=params, timeout=15)
        response.raise_for_status()
        wx_data = response.json()

        features = wx_data.get('features', [])
        if not features:
            raise ValueError('features')

        # Parse observations into a flat DataFrame
        records = []
        for station in features:
            props = station.get('properties', {})
            obs_list = props.get('observations', [])
            for obs in obs_list:
                records.append({
                    'station':       props.get('stationName', ''),
                    'datetime':      pd.to_datetime(obs.get('date')),
                    'windspeed':     obs.get('windSpeed'),
                    'winddirection': obs.get('windDirection'),
                })

        if not records:
            raise ValueError('no observations parsed')

        obs_df = pd.DataFrame(records).dropna(subset=['windspeed', 'winddirection'])

        if obs_df.empty:
            raise ValueError('all observations are NaN')

        # Use the mean over the window
        ws = float(obs_df['windspeed'].mean())
        wd = float(obs_df['winddirection'].mean())

        if verbose:
            print(f"  Retrieved {len(obs_df)} observations")
            print(f"  Wind: {ws:.1f} mph @ {wd:.0f}°")

        return {
            'windspeed':     ws,
            'winddirection': wd,
            'observations':  obs_df,
        }

    except Exception as e:
        if verbose:
            print(f"  WARNING: Weather query failed: {e}")
            print(f"  Using fallback values")

        from config import DEFAULT_TEMPERATURE, DEFAULT_HUMIDITY
        return {
            'windspeed':     5.0,   # mph fallback
            'winddirection': 270.0, # degrees fallback (westerly)
            'observations':  pd.DataFrame(),
        }

def create_bbox_from_point(lon, lat, radius_km=10.0, write_geojson=False, output_path="initial_bbox.geojson"):
    """
    Create a bounding box (as a buffer polygon) around a geographic point.

    Args:
        lon (float): Longitude of center point (EPSG:4326)
        lat (float): Latitude of center point (EPSG:4326)
        radius_km (float): Radius of the buffer in kilometers
        write_geojson (bool): If True, write output GeoJSON
        output_path (str): Output path for GeoJSON

    Returns:
        GeoDataFrame containing the buffered area (in EPSG:4326)
    """
    # Create center point in WGS84
    center_point = Point(lon, lat)
    point_gdf = gpd.GeoSeries([center_point], crs="EPSG:4326")

    # Convert to a suitable UTM CRS for accurate distance buffering
    utm_crs = point_gdf.estimate_utm_crs()
    point_utm = point_gdf.to_crs(utm_crs)

    # Create buffer (convert km → meters)
    buffer_utm = point_utm.buffer(radius_km * 1000)

    # Convert buffer back to WGS84
    buffer_wgs84 = buffer_utm.to_crs("EPSG:4326")

    # Build GeoDataFrame
    bbox_gdf = gpd.GeoDataFrame(
        {
            "type": ["bounding_box"],
            "radius_km": [radius_km],
        },
        geometry=[buffer_wgs84.iloc[0]],
        crs="EPSG:4326"
    )

    # Optional: Write GeoJSON
    if write_geojson:
        bbox_feature = geojson.Feature(
            geometry=shapely.geometry.mapping(buffer_wgs84.iloc[0]),
            properties={
                "type": "bounding_box",
                "radius_km": radius_km,
                "center_lon": lon,
                "center_lat": lat,
            }
        )
        feature_collection = geojson.FeatureCollection([bbox_feature])
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(feature_collection, f, indent=2)
        print(f"✓ Bounding box saved to {output_path}")

    return bbox_gdf


def verify_landscape_file(lcp_path):
    """
    Verify that a landscape (.lcp) file exists.
    
    Args:
        lcp_path: Path to landscape file
        
    Returns:
        bool: True if file exists, False otherwise
    """
    exists = Path(lcp_path).exists()
    
    if exists:
        print(f"✓ Landscape file found: {lcp_path}")
    else:
        print(f"✗ Landscape file not found: {lcp_path}")
        print("You need to generate or download a .lcp file for your domain.")
        print("See FARSITE documentation for landscape file creation.")
    
    return exists

def create_prj_file(epsg_code, filename):
    """
    Generates a .prj file for a given EPSG code.
    """
    spatial_ref = osr.SpatialReference()
    # Import the coordinate system from the EPSG code
    if spatial_ref.ImportFromEPSG(epsg_code) == 0:
        # Morph to ESRI WKT format for compatibility
        spatial_ref.MorphToESRI()
        # Export to WKT string
        wkt_string = spatial_ref.ExportToWkt()

        # Write the WKT string to the .prj file
        with open(filename, 'w') as f:
            f.write(wkt_string)
        print(f"Successfully created {filename} for EPSG:{epsg_code}")
    else:
        print(f"Error: Could not import EPSG code {epsg_code}")

def download_landfire_data(
    poly,
    output_dir,
    email,
    verbose=True
):
    """
    Download LANDFIRE landscape data for a fire location.
    
    Args:
        poly: Polygon object (EPSG:4326)
        radius_miles: Radius around center point (miles)
        output_dir: Directory to save downloaded rasters
        email: Valid email address (required by LANDFIRE)
        verbose: Print progress
        
    Returns:
        dict with paths to ASCII rasters:
        {
            'elevation': Path,
            'slope': Path,
            'aspect': Path,
            'fuel': Path,
            'canopy_cover': Path,
            'canopy_height': Path,
            'canopy_base': Path,
            'canopy_density': Path
        }
    """
    """Download LANDFIRE landscape data for a fire location."""
    print("Generating new .lcp file for FARSITE...")
    print("To use an existing .lcp file instead, call: verify_landscape_file(LCP_PATH)")
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Add buffer to polygon to avoid size error
    buffered_poly = poly.buffer(0.5, cap_style='flat', join_style='bevel')
    
    if verbose:
        print(f"Area of Interest: ({buffered_poly.bounds})")

    minx, miny, maxx, maxy = buffered_poly.bounds
        
    if verbose:
        print(f"Downloading LANDFIRE data in bounding box: [{minx:.4f}, {miny:.4f}, {maxx:.4f}, {maxy:.4f}]")

    
    # Submit LANDFIRE request
    LFPS_URL = "https://lfps.usgs.gov/api/job/submit"
    
    params = {
        "Email": email,
        "Layer_List": "250CBD;250CBH;250CC;250CH;250FBFM40;ASP2020;ELEV2020;SLPP2020",
        "Area_of_Interest": f"{minx} {miny} {maxx} {maxy}",
        "Output_Projection": "5070",  # NAD83 Albers
        "Resample_Resolution": "90",
        "Priority_Code": "K3LS9F"
    }
    
    if verbose:
        print("\nSubmitting LANDFIRE request...")
    
    response = requests.get(LFPS_URL, params=params, timeout=30)
    response.raise_for_status()
    
    job_id = response.json()["jobId"]
    
    if verbose:
        print(f"✓ Job ID: {job_id}")
        print(f"  Notification will be sent to {email}")
    
    # Wait for processing
    status_url = f"https://lfps.usgs.gov/api/job/status?JobId={job_id}"
    print(f"\nLFPS Job Status URL: {status_url}")

    if verbose:
        print("\nWaiting for LANDFIRE processing (takes a couple minutes)...")
    
    start_time = time.time()
    while True:
        response = requests.get(status_url, timeout=30)
        
        status_data = response.json()
        status = status_data.get("status", "").lower()
        
        elapsed = int(time.time() - start_time)
        
        if verbose:
            print(f"  [{elapsed}s] {status}")
        
        if status == "succeeded":
            if verbose:
                print(f"\n✓ Completed in {elapsed}s")
            download_url = status_data["outputFile"]
            break
        elif status in ("failed", "canceled"):
            raise RuntimeError(f"LANDFIRE job {status}: {status_data.get('message', '')}")
        
        time.sleep(10)
    
    # Download and extract
    if verbose:
        print("Downloading...")
    
    zip_response = requests.get(download_url, stream=True, timeout=60)
    zip_response.raise_for_status()
    
    if verbose:
        print("Extracting...")
    
    with zipfile.ZipFile(io.BytesIO(zip_response.content)) as zf:
        zf.extractall(output_dir)
    
    # Convert multi-band TIFF to ASCII rasters
    multi_tif = next(output_dir.glob("*.tif"))
    layer_names = ["250CBD", "250CBH", "250CC", "250CH", "250FBFM40", "ASP2020", "ELEV2020", "SLPP2020"]
    
    if verbose:
        print(f"\nConverting {multi_tif.name} to ASCII rasters...")
    
    for band_idx, layer_name in enumerate(layer_names, start=1):
        asc_path = output_dir / f"{layer_name}.asc"
        gdal.Translate(str(asc_path), str(multi_tif), format="AAIGrid", bandList=[band_idx])
        if verbose:
            print(f"  ✓ {layer_name}.asc")
    
    # Return paths in friendly names
    result = {
        'elevation': output_dir / "ELEV2020.asc",
        'slope': output_dir / "SLPP2020.asc",
        'aspect': output_dir / "ASP2020.asc",
        'fuel': output_dir / "250FBFM40.asc",
        'canopy_cover': output_dir / "250CC.asc",
        'canopy_height': output_dir / "250CH.asc",
        'canopy_base': output_dir / "250CBH.asc",
        'canopy_density': output_dir / "250CBD.asc"
    }
    
    if verbose:
        print(f"\n✓ LANDFIRE data downloaded to {output_dir}/")
    
    return result


def generate_lcp_from_rasters(
    output_path,
    elevation_asc,
    slope_asc,
    aspect_asc,
    fuel_asc,
    canopy_cover_asc,
    canopy_height_asc,
    canopy_base_asc,
    canopy_density_asc,
    latitude=None,
    fuel_model="fb40",
    verbose=True
):
    """
    Generate FARSITE landscape (.lcp) file from ASCII rasters using lcpmake.
    
    Args:
        output_path: Output .lcp file path
        elevation_asc: Elevation raster (.asc) in meters
        slope_asc: Slope raster (.asc) in percent
        aspect_asc: Aspect raster (.asc) in degrees (0-360)
        fuel_asc: Fuel model raster (.asc) - integers matching fuel_model
        canopy_cover_asc: Canopy cover (.asc) in percent (0-100)
        canopy_height_asc: Canopy height (.asc) in meters * 10
        canopy_base_asc: Canopy base height (.asc) in meters * 10
        canopy_density_asc: Canopy bulk density (.asc) in kg/m³ * 100
        latitude: Center latitude in decimal degrees (auto-detected if None)
        fuel_model: Fuel model type - "fb40" (FBFM40) or "fb13" (FBFM13)
        verbose: Print lcpmake command
        
    Returns:
        Path to generated .lcp file
    """
    output_path = Path(output_path)
    lcpmake_exe = Path(LCPMAKE_EXECUTABLE)
    
    if not lcpmake_exe.exists():
        raise FileNotFoundError(
            f"lcpmake executable not found at {lcpmake_exe}\n"
            # f"Place lcpmake in {SCRIPT_DIR}/"
        )
    
    # Auto-detect latitude from elevation raster if not provided
    if latitude is None:
        ds = gdal.Open(str(elevation_asc))
        if ds:
            gt = ds.GetGeoTransform()
            proj = ds.GetProjection()
            x_center = gt[0] + (ds.RasterXSize / 2) * gt[1]
            y_center = gt[3] + (ds.RasterYSize / 2) * gt[5]
            
            src_srs = osr.SpatialReference()
            src_srs.ImportFromWkt(proj)
            dst_srs = osr.SpatialReference()
            dst_srs.ImportFromEPSG(4326)
            transform = osr.CoordinateTransformation(src_srs, dst_srs)
            lon, lat, _ = transform.TransformPoint(x_center, y_center)
            latitude = lat
            ds = None
            if verbose:
                print(f"Auto-detected latitude: {latitude:.4f}")
    
    # Build lcpmake command
    cmd = [
        str(lcpmake_exe),
        "-latitude", str(latitude),
        "-landscape", str(output_path.with_suffix('')),
        "-elevation", str(elevation_asc),
        "-slope", str(slope_asc),
        "-aspect", str(aspect_asc),
        "-fuel", str(fuel_asc),
        "-cover", str(canopy_cover_asc),
        "-height", str(canopy_height_asc),
        "-base", str(canopy_base_asc),
        "-density", str(canopy_density_asc),
    ]
    
    if fuel_model.lower() in ["fb40", "fbfm40", "40"]:
        cmd.append("-fb40")
    elif fuel_model.lower() in ["fb13", "fbfm13", "13"]:
        cmd.append("-fb13")
    else:
        raise ValueError(f"Unknown fuel model: {fuel_model}")
    
    if verbose:
        print("\nRunning lcpmake command:")
        print(" ".join(cmd))
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(
            f"lcpmake failed with return code {result.returncode}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    
    final_path = output_path.with_suffix('.lcp')
    
    if not final_path.exists():
        raise RuntimeError(f"lcpmake succeeded but output file not found: {final_path}")
    
    if verbose:
        print(f"\n✓ Landscape file created: {final_path}")
        print(f"  Size: {final_path.stat().st_size / 1024:.1f} KB")
    
    return final_path




def verify_landscape_file(lcp_path):
    """
    Verify that a landscape (.lcp) file exists.
    
    Args:
        lcp_path: Path to landscape file
        
    Returns:
        bool: True if file exists, False otherwise
    """
    exists = Path(lcp_path).exists()
    
    if exists:
        print(f"✓ Landscape file found: {lcp_path}")
    else:
        print(f"✗ Landscape file not found: {lcp_path}")
        print("You need to generate or download a .lcp file for your domain.")
        print("See FARSITE documentation for landscape file creation.")
    
    return exists



def get_fire_detections(bbox, start_date, satellite_source="LANDSAT_NRT", day_range=5, firms_map_key="b38da98e9b7e9389fd05a00c32f99783"):
    """
    Fetch active fire detections from NASA FIRMS API. 
    
    Args:
        firms_map_key (str): Access key to query NASA FIRMS API.
        bbox (str): "minLon, minLat, maxLon, maxLat" (WGS84)
        start_time (str): Start datetime ("%Y-%m-%d")
        satellite_source (str): satelite source name (options: https://firms.modaps.eosdis.nasa.gov/api/area/)
            default: "LANDSAT_NRT" (US/Canada only)
        day_range (int): number between 1-5 of days to query
            default: 5
        
    Returns:
        dict
    """
    minx, miny, maxx, maxy = bbox
    bbox = f"{minx},{miny},{maxx},{maxy}"
    
    # Set up base URL
    FIRMS_API_URL = f"https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/{firms_map_key}/{satellite_source}/{bbox}/{day_range}/{start_date}"

    try:
        response = requests.get(FIRMS_API_URL, timeout=30)
        response.raise_for_status()
        print(f"\nNASA FIRMS Satellite Response: {response.url}")
        
        # Parse CSV response
        csv_data = StringIO(response.text)
        
        # Read into DataFrame
        hotspots_df = pd.read_csv(csv_data)
        
        print(f"\n✓ Retrieved {len(hotspots_df)} fire detections")
        
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            print("\n⚠ No fire detections found in this area and time range")
            hotspots_df = pd.DataFrame()
        else:
            print(f"\n❌ API Error: {e}")
            print("Check your MAP_KEY and try again")
            raise

    # Display dataframe keys
    col_names = list(hotspots_df.columns.values)
    print(f"\nData columns:\n{col_names}")


    # Convert to GeoDataFrame with hotspot points
    geometry = [Point(lon, lat) for lon, lat in zip(hotspots_df['longitude'], hotspots_df['latitude'])]
    hotspots_gdf = gpd.GeoDataFrame(
        hotspots_df,
        geometry=geometry,
        crs="EPSG:4326"
    )
    
    return hotspots_gdf





def fetch_weather_data(lat, lon, start_time, end_time, verbose=True):
    """
    Fetch weather observations from Firemap API.
    
    Args:
        lat: Latitude (WGS84)
        lon: Longitude (WGS84)
        start_time: Start datetime (ISO format string or datetime)
        end_time: End datetime (ISO format string or datetime)
        verbose: Print progress messages
        
    Returns:
        dict with keys: 'location', 'time_range', 'wind_speed', 'wind_direction'
    """
    # Convert datetimes to ISO format if needed
    if isinstance(start_time, pd.Timestamp):
        start_time = start_time.isoformat()
    if isinstance(end_time, pd.Timestamp):
        end_time = end_time.isoformat()
    
    if verbose:
        print(f"Querying weather data...")
        print(f"  Location: {lat:.4f}, {lon:.4f}")
        print(f"  From: {start_time}")
        print(f"  To: {end_time}")
    
    # Query Firemap API
    timestamp = int(time.time() * 1000)
    wx_params = {
        'selection': 'closestTo',
        'lat': str(lat),
        'lon': str(lon),
        'observable': ['wind_speed', 'wind_direction'],
        'from': start_time,
        'to': end_time,
        'callback': 'wxData',
        '_': str(timestamp)
    }
    
    wx_response = requests.get(FIREMAP_WX_URL, params=wx_params)
    wx_text = wx_response.text.strip()
    
    # Remove JSONP wrapper
    if wx_text.startswith('wxData(') and wx_text.endswith(')'):
        wx_json = wx_text[len('wxData('):-1]
        wx_obs = json.loads(wx_json)
    else:
        wx_obs = wx_response.json()
    
    # Extract wind data
    wind_speed_list = wx_obs["features"][0]["properties"]["wind_speed"]
    wind_direction_list = wx_obs["features"][0]["properties"]["wind_direction"]
    
    weather_data = {
        "location": {"lat": lat, "lon": lon},
        "time_range": {"start": start_time, "end": end_time},
        "wind_speed": wind_speed_list,
        "wind_direction": wind_direction_list
    }
    
    if verbose:
        print(f"\n✓ Retrieved {len(wind_speed_list)} weather observations")
        print(f"  Wind speed: {min(wind_speed_list):.1f} - {max(wind_speed_list):.1f} mph (mean: {np.mean(wind_speed_list):.1f})")
        print(f"  Wind direction: {min(wind_direction_list):.0f} - {max(wind_direction_list):.0f}° (mean: {np.mean(wind_direction_list):.0f})")
    
    return weather_data


def get_weather_location_from_fire(perimeters_gdf, to_wgs84=True):
    """
    Get weather query location from fire perimeter centroid.
    
    Args:
        perimeters_gdf: GeoDataFrame with fire perimeters
        to_wgs84: Convert to WGS84 coordinates (required for weather API)
        
    Returns:
        tuple: (lat, lon) in WGS84 if to_wgs84=True, else in original CRS
    """
    # Use first perimeter centroid
    fire_centroid = perimeters_gdf.geometry.iloc[0].centroid
    
    if to_wgs84:
        # Convert to WGS84 for weather API
        transformer = Transformer.from_crs(perimeters_gdf.crs, "EPSG:4326", always_xy=True)
        lon_wgs, lat_wgs = transformer.transform(fire_centroid.x, fire_centroid.y)
        return lat_wgs, lon_wgs
    else:
        return fire_centroid.y, fire_centroid.x



def extract_fire_timeline(perimeters_gdf, verbose=True):
    """
    Extract ignition and containment dates from perimeter GeoDataFrame.
    
    Args:
        perimeters_gdf: GeoDataFrame with perimeter updates (must have 'datetime' column)
        verbose: Print timeline information
        
    Returns:
        dict with keys: 'ignition_date', 'containment_date', 'duration', 'n_updates'
    """
    ignition_date = perimeters_gdf['datetime'].iloc[0]
    containment_date = perimeters_gdf['datetime'].iloc[-1]
    duration = containment_date - ignition_date
    n_updates = len(perimeters_gdf)
    
    timeline = {
        'ignition_date': ignition_date,
        'containment_date': containment_date,
        'duration': duration,
        'n_updates': n_updates
    }
    
    if verbose:
        print(f"\nFire Timeline:")
        print(f"  First observation (ignition): {ignition_date}")
        print(f"  Last observation (containment): {containment_date}")
        print(f"  Total duration: {duration}")
        print(f"  Number of updates: {n_updates}")
    
    return timeline
    




### Visualization

def plot_perimeter_evolution(perimeters_gdf, fire_name="Fire", add_basemap=True):
    """
    Plot fire perimeter evolution over time with OpenStreetMap basemap.
    
    Args:
        perimeters_gdf: GeoDataFrame with perimeter updates (must have 'datetime' column)
        fire_name: Name of fire for plot title
        add_basemap: Add OpenStreetMap basemap (default: True)
    """
    fig, ax = plt.subplots(1, 1, figsize=(14, 14))
    
    n_perims = len(perimeters_gdf)
    colors = plt.cm.Reds(np.linspace(0.3, 1, n_perims))
    
    # Plot perimeters
    for idx, (_, row) in enumerate(perimeters_gdf.iterrows()):
        boundary = row.geometry.boundary
        if boundary.geom_type == 'LineString':
            x, y = boundary.xy
            ax.plot(x, y, color=colors[idx], linewidth=2.5, zorder=2)
        elif boundary.geom_type == 'MultiLineString':
            for line in boundary.geoms:
                x, y = line.xy
        ax.plot(x, y, color=colors[idx], linewidth=2.5, zorder=2)
    
    # Add basemap
    if add_basemap:
        try:
            ctx.add_basemap(
                ax=ax, 
                source=ctx.providers.OpenStreetMap.Mapnik, 
                crs=FARSITE_CRS,
                alpha=0.6,
                zorder=1
            )
        except Exception as e:
            print(f"Could not add basemap: {e}")
    
    ax.set_aspect('equal')
    
    start_date = perimeters_gdf['datetime'].iloc[0].date()
    end_date = perimeters_gdf['datetime'].iloc[-1].date()
    ax.set_title(f"{fire_name} - Perimeter Evolution\n{start_date} to {end_date}", fontsize=16, weight='bold')
    
    # Add colorbar
    sm = plt.cm.ScalarMappable(cmap=plt.cm.Reds, norm=plt.Normalize(vmin=0, vmax=n_perims-1))
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, fraction=0.03, pad=0.04)
    cbar.set_label('Time progression (older → newer)', fontsize=12)
    
    plt.tight_layout()
    plt.show()


def plot_weather_data(weather_data):
    """
    Plot weather observations (wind speed and direction).
    
    Args:
        weather_data: Dictionary returned by fetch_weather_data()
    """
    wind_speed = weather_data['wind_speed']
    wind_direction = weather_data['wind_direction']
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6))
    
    ax1.plot(wind_speed, 'b-', linewidth=1)
    ax1.axhline(np.mean(wind_speed), color='r', linestyle='--', 
               label=f'Mean: {np.mean(wind_speed):.1f} mph')
    ax1.set_ylabel('Wind Speed (mph)')
    ax1.set_title('Weather Observations')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    ax2.plot(wind_direction, 'g-', linewidth=1)
    ax2.axhline(np.mean(wind_direction), color='r', linestyle='--', 
               label=f'Mean: {np.mean(wind_direction):.0f}°')
    ax2.set_ylabel('Wind Direction (degrees)')
    ax2.set_xlabel('Observation Index')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()


def plot_active_hotspots(hotspots_gdf):
    # --- Basic info ---
    earliest_date = hotspots_gdf['acq_date'].iloc[0]
    cen_lat = hotspots_gdf['latitude'].iloc[0]
    cen_lon = hotspots_gdf['longitude'].iloc[0]
    cen_point = Point(cen_lat, cen_lon)

    # --- Create figure ---
    fig, ax = plt.subplots(1, 1, figsize=(12, 10))

    # --- Initial invisible plot to determine extent ---
    hotspots_gdf.plot(
        ax=ax,
        color="none",
        markersize=1,
        legend=False
    )

    # --- Expand view ---
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    x_margin = (xlim[1] - xlim[0]) * 2.0
    y_margin = (ylim[1] - ylim[0]) * 2.0
    ax.set_xlim(xlim[0] - x_margin, xlim[1] + x_margin)
    ax.set_ylim(ylim[0] - y_margin, ylim[1] + y_margin)

    # --- Basemap ---
    try:
        ctx.add_basemap(
            ax,
            crs=hotspots_gdf.crs.to_string(),
            source=ctx.providers.OpenStreetMap.Mapnik,
            zoom='auto',
            alpha=0.6,
            zorder=0
        )
        print("✓ Basemap added")
    except Exception as e:
        print(f"Note: Could not add basemap: {e}")

    # --- Plot hotspots manually (Matplotlib scatter) ---
    ax.scatter(
        hotspots_gdf.geometry.x,
        hotspots_gdf.geometry.y,
        color='orange',       # uniform hotspot color
        s=200,
        alpha=0.9,
        edgecolors='white',
        linewidths=1.5,
        zorder=10,
        label="Fire Detections"
    )

    # --- Convex hull ---
    hull = hotspots_gdf.unary_union.convex_hull
    gpd.GeoSeries([hull], crs=hotspots_gdf.crs).boundary.plot(
        ax=ax,
        color='red',
        linewidth=3,
        linestyle='--',
        zorder=11,
        label='Fire Extent'
    )

    # --- First and last detections ---
    first = hotspots_gdf.iloc[0]
    last = hotspots_gdf.iloc[-1]

    ax.plot(
        first.geometry.x, first.geometry.y,
        marker='*', markersize=25,
        color='lime', markeredgecolor='black',
        markeredgewidth=2, zorder=15,
        label='First Detection'
    )

    ax.plot(
        last.geometry.x, last.geometry.y,
        marker='*', markersize=25,
        color='red', markeredgecolor='black',
        markeredgewidth=2, zorder=15,
        label='Last Detection'
    )

    # --- Formatting ---
    ax.set_title(
        f"Active Fire Detections near {cen_point} starting {earliest_date}",
        fontsize=14, fontweight='bold'
    )
    ax.set_xlabel("X (m)", fontsize=11)
    ax.set_ylabel("Y (m)", fontsize=11)
    ax.set_aspect("equal")
    ax.grid(True, alpha=0.3, linestyle=":", linewidth=0.5)
    ax.legend(loc='upper right', fontsize=10, framealpha=0.95)

    plt.tight_layout()
    plt.show()




### Saving final workflow configuration settings
def save_workflow_config(fire_name, lcp_path, timeline, weather_location, 
                        domain_bounds=None, output_path=None):
    """
    Save complete workflow configuration.
    
    Args:
        fire_name: Name of fire
        lcp_path: Path to landscape file
        timeline: Dictionary from extract_fire_timeline()
        weather_location: (lat, lon) tuple
        domain_bounds: Optional domain bounds
        output_path: Output path (default: DATA_DIR/workflow_config.json)
        
    Returns:
        Path to saved configuration file
    """
    config_data = {
        # Static data
        "lcp_path": str(lcp_path),
        "domain_bounds": domain_bounds,
        "crs": FARSITE_CRS,
        
        # Fire information
        "fire_name": fire_name,
        "ignition_date": timeline['ignition_date'].isoformat(),
        "containment_date": timeline['containment_date'].isoformat(),
        "n_perimeters": timeline['n_updates'],
        
        # Weather location
        "weather_location": {"lat": weather_location[0], "lon": weather_location[1]},
    }
    
    if output_path is None:
        output_path = DATA_DIR / "workflow_config.json"
    
    with open(output_path, 'w') as f:
        json.dump(config_data, f, indent=2)
    
    print(f"✓ Configuration saved to {output_path}")
    return output_path


def save_perimeters(perimeters_gdf, fire_name, output_dir=None):
    """
    Save perimeter GeoDataFrame to file.
    
    Args:
        perimeters_gdf: GeoDataFrame with perimeters
        fire_name: Name of fire
        output_dir: Output directory (default: OUTPUT_DIR)
        
    Returns:
        Path to saved file
    """
    if output_dir is None:
        output_dir = OUTPUT_DIR
    
    filename = f"{fire_name.lower().replace(' ', '_')}_perimeters.geojson"
    output_path = output_dir / filename
    
    perimeters_gdf.to_file(output_path, driver="GeoJSON")
    
    print(f"✓ Perimeters saved to {output_path}")
    print(f"  Order: Index 0 = oldest (ignition), Index {len(perimeters_gdf)-1} = newest (containment)")
    
    return output_path


def save_weather_data(weather_data, output_dir=None):
    """
    Save weather data to JSON file.
    
    Args:
        weather_data: Dictionary from fetch_weather_data()
        output_dir: Output directory (default: DATA_DIR)
        
    Returns:
        Path to saved file
    """
    if output_dir is None:
        output_dir = DATA_DIR
    
    output_path = output_dir / "weather_observations.json"
    
    with open(output_path, 'w') as f:
        json.dump(weather_data, f, indent=2)
    
    print(f"✓ Weather data saved to {output_path}")
    
    return output_path