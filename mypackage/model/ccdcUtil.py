"""
CCDC (Continuous Change Detection and Classification) Utility Functions

This module provides a set of functions for working with Earth Engine (ee) 
and performing time series analysis using the CCDC algorithm. It includes 
utilities for date handling, coefficient extraction, and synthetic imagery 
generation based on CCDC model results.

Key components:
- Date conversion and handling
- Tag building for segments and bands
- CCDC result processing (coefficients, temporal information)
- Coefficient filtering and extraction
- Synthetic imagery generation

These functions are designed to work with Earth Engine and require the ee library.
"""
import ee
import math
import geedim
import rioxarray as rxr
from datetime import datetime as dt
import time

def toYearFraction(date):
    """
    Convert a datetime object to a fractional year representation.
    
    Args:
        date (datetime): The date to convert
    
    Returns:
        float: The fractional year representation of the input date
    """
    def sinceEpoch(date): # returns seconds since epoch
        return time.mktime(date.timetuple())
    s = sinceEpoch

    year = date.year
    startOfThisYear = dt(year=year, month=1, day=1)
    startOfNextYear = dt(year=year+1, month=1, day=1)

    yearElapsed = s(date) - s(startOfThisYear)
    yearDuration = s(startOfNextYear) - s(startOfThisYear)
    fraction = yearElapsed/yearDuration

    return date.year + fraction

def buildSegmentTag(nSegments):
    """
    Build segment tags (S1, S2, ...) for a given number of segments.
    
    Args:
        nSegments (int): Number of segments
    
    Returns:
        ee.List: List of segment tags
    """
    def proc(i):
        return ee.String('S').cat(ee.Number(i).int().format())
    
    return ee.List.sequence(1, nSegments).map(lambda i:proc(i))

def buildBandTag(tag, bandList):
    """
    Build band tags by appending a tag to each band name.
    
    Args:
        tag (str): Tag to append to band names
        bandList (list): List of band names
    
    Returns:
        ee.List: List of band tags
    """
    bands = ee.List(bandList)
    def proc(s):
        return ee.String(s).cat('_'+tag)
    return bands.map(lambda s:proc(s))

def buildStartEndBreakProb(fit, nSegments, tag):
    """
    Build an image with start, end, break, or probability information for each segment.
    
    Args:
        fit (ee.Image): The CCDC model fit image
        nSegments (int): Number of segments
        tag (str): Type of information to extract ('tStart', 'tEnd', 'tBreak', or 'changeProb')
    
    Returns:
        ee.Image: An image with the requested information for each segment
    """
    def proc(s):
        return ee.String(s).cat('_').cat(tag)
    segmentTag=buildSegmentTag(nSegments).map(lambda s:proc(s))
    zeros = ee.Array(0).repeat(0, nSegments)
    magImg = fit.select(tag).arrayCat(zeros, 0).float().arraySlice(0, 0, nSegments)
    return magImg.arrayFlatten([segmentTag])
    

def buildCoefs(fit, nSegments, bandList):
    """
    Build coefficient images for each band and segment from CCDC fit results.

    Args:
        fit (ee.Image): The CCDC model fit image
        nSegments (int): Number of segments
        bandList (list): List of band names

    Returns:
        ee.Image: An image containing coefficient bands for each segment and spectral band
    """
    nBands = len(bandList)
    bandTag = buildBandTag('coef', bandList)
    segmentTag = buildSegmentTag(nSegments)
    harmonicTag = ['INTP','SLP','COS','SIN','COS2','SIN2','COS3','SIN3']
    zeros = ee.Image(ee.Array([ee.List.repeat(0, len(harmonicTag))])).arrayRepeat(0, nSegments)
    
    def retrieveCoefs(band):
        """
        Retrieve coefficients for a single band and format them into an image.
        
        Args:
            band (str): The name of the band
        
        Returns:
            ee.Image: An image with coefficient bands for the given spectral band
        """
        b = ee.String(band)
        cname = b.cat('_coefs')
        coefImg = fit.select(cname).arrayCat(zeros, 0).float().arraySlice(0, 0, nSegments)
        
        # Create tags for each segment and coefficient
        def proc(x):
            return ee.String(x).cat('_').cat(b).cat('_coef')
        tags = segmentTag.map(lambda x: proc(x))
        
        # Flatten the array into an image with named bands
        return coefImg.arrayFlatten([tags, harmonicTag])
    
    # Apply retrieveCoefs to each band
    tmp = ee.List(bandList).map(lambda band:retrieveCoefs(band))
    
    # Collect results into a list
    ll = []
    for i in range(nBands):
        ll.append(tmp.get(i))
    
    # Combine all coefficient images into a single multi-band image
    return ee.Image.cat(ll)

def buildCcdImage(fit, nSegments, bandList):
    """
    Build a comprehensive CCDC image by combining coefficients and temporal information.

    Args:
        fit (ee.Image): The CCDC model fit image
        nSegments (int): Number of segments
        bandList (list): List of band names

    Returns:
        ee.Image: A combined image with coefficients and temporal information for each segment

    """
    # Build coefficient images for each band and segment
    coef = buildCoefs(fit, nSegments, bandList)
    
    # Extract temporal information for each segment
    tStart = buildStartEndBreakProb(fit, nSegments, 'tStart')  # Start time of each segment
    tEnd = buildStartEndBreakProb(fit, nSegments, 'tEnd')      # End time of each segment
    tBreak = buildStartEndBreakProb(fit, nSegments, 'tBreak')  # Break time of each segment
    probs = buildStartEndBreakProb(fit, nSegments, 'changeProb')  # Change probability for each segment
    nobs = buildStartEndBreakProb(fit, nSegments, 'numObs')    # Number of observations for each segment
    
    # Combine coefficient and temporal information into a single image
    # Note: Only coef, tStart, and tEnd are included in the final image
    return ee.Image.cat([coef, tStart, tEnd])

def filterCoefs(ccdResults, date, band, coef, segNames, behavior):
    """
    Filter coefficients based on date and behavior.

    Args:
        ccdResults (ee.Image): CCDC results image
        date (float): Date to filter by (in year fraction format)
        band (str): Spectral band name
        coef (str): Coefficient name
        segNames (list): List of segment names
        behavior (str): 'before' or 'after' to determine filtering behavior

    Returns:
        ee.Image: Filtered coefficient image
    """
    # Extract start and end dates for each segment
    startBands = ccdResults.select(".*_tStart").rename(segNames)
    endBands = ccdResults.select(".*_tEnd").rename(segNames)

    # Create a selection string for the coefficient bands
    selStr = ee.String(".*").cat(band).cat("_.*").cat(coef)
    coef_bands = ccdResults.select(selStr)
    
    if (behavior == "before"):
        # For 'before' behavior, select segments that started before the given date
        segmentMatch = startBands.selfMask().lt(date).selfMask()
        # Get the last non-null coefficient (most recent segment before the date)
        outCoef =  coef_bands.updateMask(segmentMatch).reduce(ee.Reducer.lastNonNull())
    elif (behavior == "after"):
        # For 'after' behavior, select segments that end after the given date
        segmentMatch = endBands.gt(date)
        # Get the first non-null coefficient (earliest segment after the date)
        outCoef = coef_bands.updateMask(segmentMatch).reduce(ee.Reducer.firstNonNull())
    
    return outCoef

def getCoef(ccdResults, date, bandList, coef, segNames, behavior):
    """
    Extract coefficients for multiple bands from CCDC results.

    Args:
        ccdResults (ee.Image): CCDC results image
        date (float): Date to filter by (in year fraction format)
        bandList (list): List of spectral band names
        coef (str): Coefficient name to extract
        segNames (list): List of segment names
        behavior (str): 'before' or 'after' to determine filtering behavior

    Returns:
        ee.Image: Image with extracted coefficients for each band
    """
    nBand = len(bandList)

    def inner(band):
        """
        Extract coefficient for a single band.

        Args:
            band (str): Spectral band name

        Returns:
            ee.Image: Image with extracted coefficient for the band
        """
        # Filter coefficients for the specific band
        band_coef = filterCoefs(ccdResults, date, band, coef, segNames, behavior)
        # Rename the resulting band
        return band_coef.rename(ee.String(band).cat("_").cat(coef))

    # Apply the inner function to each band
    tmp = ee.List(bandList).map(lambda b: inner(b))

    # Collect results into a list
    ll = []
    for i in range(nBand):
        ll.append(tmp.get(i))

    # Combine all coefficient images into a single multi-band image
    return ee.Image.cat(ll)

def getMultiCoefs(ccdResults, date, bandList, coef_list, segNames, behavior):
    """
    Extract multiple coefficients for multiple bands from CCDC results.
    
    Args:
        ccdResults (ee.Image): CCDC results image
        date (float): Date to filter by (in year fraction format)
        bandList (list): List of spectral band names
        coef_list (list): List of coefficient names to extract
        segNames (list): List of segment names
        behavior (str): 'before' or 'after' to determine filtering behavior

    Returns:
        ee.Image: Image with extracted coefficients for each band and coefficient type
    """
    
    nCoef = len(coef_list)
    
    def inner(coef):
        """
        Extract a single coefficient type for all bands.

        Args:
            coef (str): Coefficient name to extract

        Returns:
            ee.Image: Image with extracted coefficient for all bands
        """
        inner_coef = getCoef(ccdResults, date, bandList, coef, segNames, behavior)
        return inner_coef
    
    # Apply the inner function to each coefficient type
    tmp = ee.List(coef_list).map(lambda c: inner(c))
    
    # Collect results into a list
    ll = []
    for i in range(nCoef):
        ll.append(tmp.get(i))
    
    # Combine all coefficient images into a single multi-band image
    return ee.Image.cat(ll)

def getSyntheticForYear(image, date, band, segs, dateFormat=1):
    """
    Generate synthetic imagery for a specific date based on CCDC coefficients.

    Args:
        image (ee.Image): CCDC results image
        date (float): Date to generate synthetic imagery for (in year fraction format)
        band (list): List of spectral band names
        segs (list): List of segment names
        dateFormat (int): Date format indicator (default: 1)

    Returns:
        ee.Image: Synthetic image for the specified date and bands
    """
    # Convert input date to ee.Number
    tfit = ee.Number(date)

    # Define constants
    PI2 = 2.0 * math.pi
    OMEGAS = [PI2 / 365.25, PI2, PI2 / (1000 * 60 * 60 * 24 * 365.25)]
    omega = OMEGAS[dateFormat]

    # Create an image with harmonic components for the given date
    imageT = ee.Image.constant([1, tfit,
                                tfit.multiply(omega).cos(),
                                tfit.multiply(omega).sin(),
                                tfit.multiply(omega * 2).cos(),
                                tfit.multiply(omega * 2).sin(),
                                tfit.multiply(omega * 3).cos(),
                                tfit.multiply(omega * 3).sin()]).float()

    # Define coefficient names
    COEFS = ["INTP", "SLP", "COS", "SIN", "COS2", "SIN2", "COS3", "SIN3"]

    # Get the coefficients for the specified date and bands
    newParams = getMultiCoefs(image, date, band, COEFS, segs, 'before')

    # Calculate synthetic values by multiplying harmonic components with coefficients
    # and summing the results
    return imageT.multiply(newParams).reduce('sum').rename(band)

def getMultiSynthetic(image, date, bandList, segs, dateFormat=1, scale=10000):
    """
    Generate synthetic imagery for multiple bands based on CCDC coefficients.

    Args:
        image (ee.Image): CCDC results image
        date (float): Date to generate synthetic imagery for (in year fraction format)
        bandList (list): List of spectral band names
        segs (list): List of segment names
        dateFormat (int): Date format indicator (default: 1)
        scale (int): Scaling factor for the output values (default: 10000)

    Returns:
        ee.Image: Synthetic image for the specified date and bands
    """
    ll = []
    for b in bandList:
        # Generate synthetic imagery for each band
        tmp = getSyntheticForYear(image, date, [b], segs, dateFormat=1)
        
        # Scale the values, convert to int16, and set masked values to -9999
        ll.append(tmp.multiply(scale).int16().unmask(-9999))
 
    # Combine all band images into a single multi-band image
    return ee.Image.cat(ll)

def main():
    # input to download data for
    gee_account = 'id-sl-senegal-service-account@ee-3sl-senegal.iam.gserviceaccount.com'
    gee_key = '/home/jli30/gee/ee-3sl-senegal-8fa70fe1c565.json'

    # get credentials
    credentials = ee.ServiceAccountCredentials(gee_account, gee_key)
    ee.Initialize(credentials)  # gd initialize does not take service account
    print("Initialized")

    date_str = '2001-08-12'
    date_object = dt.strptime(date_str, '%Y-%m-%d').date()

    formattedDate = toYearFraction(date_object)

    coords = [[-149.1833576743, 61.5406563632], [-148.9069567464, 61.5447976303],[-148.8810273161, 61.1153798873], [-149.1536766218, 61.1113113479], [-149.1833576743, 61.5406563632]]

    roi = ee.Geometry.Polygon(coords=[coords])

    segments = (ee.ImageCollection('projects/CCDC/v3').filterBounds(roi).mosaic())

    BANDS = ['BLUE', 'GREEN', 'RED', 'NIR']
    SEGS = ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10"]
    nSegments = len(SEGS)

    ccdImage=buildCcdImage(segments, nSegments, BANDS)

    outImage=getMultiSynthetic(ccdImage, formattedDate, BANDS, SEGS, 1)

    proj="EPSG:4326"
    im = geedim.MaskedImage(outImage, mask=False)
    im.download('scene_tst.tif', crs=proj, scale=30, region=roi, max_tile_size=4)

if __name__ == "__main__":
    main()



