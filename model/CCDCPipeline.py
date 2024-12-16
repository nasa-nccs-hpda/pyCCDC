import glob
import re
import json
from datetime import datetime as dt
import ee
import rioxarray as rxr
import geedim
from shapely import box
from pathlib import Path
from pyCCDC.model.ccdcUtil import toYearFraction, buildCcdImage, getMultiSynthetic


class CCDCPipeline:
    """
    A class to process and generate CCDC (Continuous Change Detection and Classification) images.

    This class provides functionality to:
    1. Initialize the pipeline with input and output directories.
    2. Authenticate with Google Earth Engine (GEE).
    3. Extract coordinates from raster files.
    4. Generate single CCDC images based on input data and coordinates.
    5. Run the pipeline to process multiple scenes.

    Attributes:
        input_dir (str): Directory containing input raster files.
                         Raster file names should follow the pattern: QB02_YYYYMMDD_?1BS_*.tif
        output_dir (str): Directory to save output CCDC images.

    Methods:
        __init__(self, input_dir, output_dir): Initialize the CCDCPipeline.
        _get_gee_credential(account, key): Static method to get GEE credentials.
        _get_coords(self, file): Extract coordinates from a raster file.
        gen_single_image(self, date_str, coords, outfile=None): Generate a single CCDC image.
        run(self): Run the pipeline to process all input scenes.
    """
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir

    @staticmethod
    def _get_gee_credential(account, key):
        try:
            return ee.ServiceAccountCredentials(account, key)
        except Exception as e:
            print(f"Error creating GEE credentials: {str(e)}")
            raise  

    def _get_coords(self, file):
        """
        Extract coordinates from a raster file and convert them to EPSG:4326 projection.

        Args:
            file (str): Path to the raster file.

        Returns:
            list: A list of coordinate pairs [longitude, latitude] defining the bounding box of the raster.

        Note: This function assumes the input file is a valid raster file readable by rioxarray.
        """
        # Open the raster file
        raster = rxr.open_rasterio(file)

        # Reproject the raster to EPSG:4326 (WGS84) coordinate system
        raster_reproj = raster.rio.reproject("EPSG:4326")

        # Get the projection of the raster
        epsg = raster.rio.crs.to_epsg()

        # Create a bounding box from the raster's extent
        poly = box(*raster_reproj.rio.bounds())

        # Extract the coordinates of the bounding box
        # Convert each coordinate pair to [longitude, latitude] format
        coords = [[i[0], i[1]] for i in list(poly.exterior.coords)]

        return coords, epsg

    def gen_single_image(self, date_str, coords, outfile=None):
    # TODO: Implement user-specified GEE account authentication. 
    # Currently using local credentials for testing purposes.
        with open("/explore/nobackup/projects/ilab/gee/gee_config.json") as fh:
            config = json.load(fh)
        gee_account = config.get('gee_account')
        gee_key = config.get('gee_key_path')

        try:
            credentials = self._get_gee_credential(gee_account, gee_key)
            ee.Initialize(credentials)
            print("GEE initialized successfully")
        except Exception as e:
            print(f"Error initializing GEE: {str(e)}")
            raise
    
        date_object = dt.strptime(date_str, '%Y-%m-%d').date()
        formatted_date = toYearFraction(date_object)

        roi = ee.Geometry.Polygon(coords=[coords])
        segments = (ee.ImageCollection('projects/CCDC/v3').filterBounds(roi).mosaic())

        bands = ['BLUE', 'GREEN', 'RED', 'NIR']
        segs = ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10"]
        n_segments = len(segs)
        ccd_image = buildCcdImage(segments, n_segments, bands)
        out_image = getMultiSynthetic(ccd_image, formatted_date, bands, segs, 1)

        proj = "EPSG:4326"
        im = geedim.MaskedImage(out_image, mask=False)
        im.download(outfile, crs=proj, scale=30, region=roi, max_tile_size=2, overwrite=True)

    def post_proc(self, file, crs):
        raster = rxr.open_rasterio(file)
        raster = raster.rio.write_nodata(-9999.)

        raster = raster.rio.reproject(f"EPSG:{crs}")
        raster.rio.to_raster(file)

    def run(self, toa_file=None):
        # Process a specific ToA file if provided, otherwise process all .tif files in the input directory
        if toa_file:
            toa_path = Path(toa_file)
            if toa_path.exists():
                wv_list = [toa_path]
            else:
                raise FileNotFoundError(f"The specified file '{toa_path}' does not exist.")
        else:
            dir_path = Path(self.input_dir)
            wv_list = list(dir_path.glob("*.tif"))
            if not wv_list:
                raise FileNotFoundError(f"No .tif files found in the input directory: {dir_path}")

        out_list = []
        for fpath in wv_list: 
            #base_fn = fpath.name

            # Assuming raster file name follows the pattern: QB02_YYYYMMDD_?1BS_*.tif
            date_str = fpath.stem.split('_')[1]

            # Validate date string format
            if not re.match(r'^\d{8}$', date_str):
                raise ValueError("Date string must be in the format YYYYMMDD")
            # Convert date string to YYYY-MM-DD format for gen_single_image function
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

            #Extract bounding box
            coords, epsg = self._get_coords(str(fpath))

            #Output file path
            out_fn = fpath.stem + '_ccdc' + fpath.suffix
            out_fpath = Path(self.output_dir) / out_fn
            out_list.append(out_fpath)
            self.gen_single_image(date_str, coords, outfile=str(out_fpath))
            self.post_proc(out_fpath, epsg)
        return out_list
