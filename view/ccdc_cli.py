import os
import re
import ee
import json
import geedim
import argparse
import rioxarray as rxr
from shapely import box
from shapely.ops import transform
from pyproj import Transformer, CRS
from datetime import datetime as dt
from pyCCDC.model.ccdcUtil import \
    buildCcdImage, toYearFraction, getMultiSynthetic

"""
CCDC (Continuous Change Detection and Classification) Image Generation Script

This script generates synthetic images using the CCDC
(Continuous Change Detection and Classification)
algorithm implemented in Google Earth Engine (GEE).
It takes a footprint file as input, extracts the
coordinates and date from it, and uses this information
to create a synthetic image for the specified
area and date.

The script performs the following main tasks:
1. Extracts coordinates from a given raster file and
converts them to EPSG:4326 projection.
2. Authenticates with Google Earth Engine using provided
or default credentials.
3. Generates a synthetic image using CCDC algorithm for
the specified date and area.
4. Saves the generated image to a specified output location.

Usage:
    python ccdc_cli.py \
        --gee_config <GEE_CONFIG_FILE> \
        --output_path <OUTPUT_DIRECTORY> \
        --footprint_file <PATH_TO_FOOTPRINT_FILE>

Note: If GEE credentials are not provided, the script will attempt
to use default credentials.
"""


def _getCoords(file):
    """
    Extract coordinates from a raster file and convert
    them to EPSG:4326 projection.

    Args:
        file (str): Path to the raster file.

    Returns:
        list: A list of coordinate pairs [longitude, latitude]
        defining the bounding box of the raster.

    Note: This function assumes the input file is a valid
    raster file readable by rioxarray.
    """
    # Open the raster file
    raster = rxr.open_rasterio(file)
    raster_crs = raster.rio.crs.to_epsg()

    # Create a bounding box from the raster's extent
    poly = box(*raster.rio.bounds())

    # Define the source and target CRS
    source_crs = CRS.from_epsg(raster_crs)  # WGS 84
    target_crs = CRS.from_epsg(4326)  # Web Mercator

    # Create a transformer
    transformer = Transformer.from_crs(
        source_crs, target_crs, always_xy=True)

    # Reproject the raster to EPSG:4326 (WGS84) coordinate system
    poly_reproj = transform(transformer.transform, poly)

    # Extract the coordinates of the bounding box
    # Convert each coordinate pair to [longitude, latitude] format
    coords = [[i[0], i[1]] for i in list(poly_reproj.exterior.coords)]

    return coords


def _get_gee_credential(account, key):
    try:
        return ee.ServiceAccountCredentials(account, key)
    except Exception as e:
        print(f"Error creating GEE credentials: {str(e)}")
        raise


def genSingleImage(
            dateStr,
            coords,
            gee_account=None,
            gee_key=None,
            outfile='',
            scale=30,
            max_tile_size=0.5,
            image_dtype='int16'
        ):

    # TODO: Implement user-specified GEE account authentication.
    # Currently using local credentials for testing purposes.
    try:
        credentials = _get_gee_credential(gee_account, gee_key)
        ee.Initialize(credentials)
        print("GEE initialized successfully")
    except Exception as e:
        print(f"Error initializing GEE: {str(e)}")
        raise

    date_object = dt.strptime(dateStr, '%Y-%m-%d').date()
    formattedDate = toYearFraction(date_object)

    roi = ee.Geometry.Polygon(coords=[coords])
    segments = (
        ee.ImageCollection('projects/CCDC/v3').filterBounds(roi).mosaic())

    BANDS = ['BLUE', 'GREEN', 'RED', 'NIR']
    SEGS = ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10"]
    nSegments = len(SEGS)
    ccdImage = buildCcdImage(segments, nSegments, BANDS)
    outImage = getMultiSynthetic(ccdImage, formattedDate, BANDS, SEGS, 1)

    proj = "EPSG:4326"
    im = geedim.MaskedImage(outImage, mask=False)
    im.download(
        outfile,
        crs=proj,
        scale=scale,
        region=roi,
        max_tile_size=max_tile_size,
        dtype=image_dtype,
        overwrite=True
    )
    return


def main():

    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Generate synthetic image using CCDC')
    parser.add_argument(
        '--gee_config',
        type=str,
        default=None,
        help='Path to GEE config file that contains account name and key'
    )

    parser.add_argument(
        '--footprint_file',
        type=str,
        default=None,
        help='Path to the file containing the scene footprint ' +
        'coordinates for CCDC generation'
    )

    parser.add_argument(
        '--output_path',
        type=str,
        default='./',
        help='Directory path to save the generated synthetic image'
    )

    # Parse arguments
    args = parser.parse_args()

    # Load GEE acount information
    with open(args.gee_config, 'r') as fh:
        config = json.load(fh)
    gee_account = config.get('gee_account')
    gee_key_path = config.get('gee_key_path')

    # Extract footprint coordinates
    coords = _getCoords(args.footprint_file)

    # Extract date
    tiff_file = os.path.basename(args.footprint_file)
    date_str = tiff_file.split('_')[1]
    # Validate date string format
    if not re.match(r'^\d{8}$', date_str):
        raise ValueError("Date string must be in the format YYYYMMDD")
    # Convert date string to YYYY-MM-DD format for genSingleImage function
    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    outfn = tiff_file.split('.')[0]+'_ccdc'+'.tiff'
    outfile = os.path.join(args.output_path, outfn)

    # Create output directory
    os.makedirs(args.output_path, exist_ok=True)

    # download single CCDC scene
    genSingleImage(
        date_str,
        coords,
        gee_account=gee_account,
        gee_key=gee_key_path,
        outfile=outfile
    )

    return


if __name__ == "__main__":
    main()
