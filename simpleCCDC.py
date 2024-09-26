from datetime import datetime as dt
import geedim
from model.ccdcUtil import toYearFraction, buildCcdImage, getMultiSynthetic


def main():
    with open('/home/jli30/gee/gee_config.json') as fh:
        config = json.load(fh)
    gee_account = config.get('gee_account')
    gee_key = config.get('gee_key_path')

    # Get credentials
    credentials = ee.ServiceAccountCredentials(gee_account, gee_key)
    ee.Initialize(credentials)  # gd initialize does not take service account
    print("Initialized")

    date_str = '2001-08-12'
    date_object = dt.strptime(date_str, '%Y-%m-%d').date()

    formatted_date = toYearFraction(date_object)

    coords = [
        [-149.1833576743, 61.5406563632],
        [-148.9069567464, 61.5447976303],
        [-148.8810273161, 61.1153798873],
        [-149.1536766218, 61.1113113479],
        [-149.1833576743, 61.5406563632]
    ]
    roi = ee.Geometry.Polygon(coords=[coords])

    segments = (ee.ImageCollection('projects/CCDC/v3').filterBounds(roi).mosaic())

    BANDS = ['BLUE', 'GREEN', 'RED', 'NIR']
    SEGS = ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10"]
    n_segments = len(SEGS)

    ccd_image = buildCcdImage(segments, n_segments, BANDS)

    out_image = getMultiSynthetic(ccd_image, formatted_date, BANDS, SEGS, 1)

    proj = "EPSG:4326"
    im = geedim.MaskedImage(out_image, mask=False)
    im.download(
        'scene_tst.tif',
        crs=proj,
        scale=30,
        region=roi,
        max_tile_size=2,
        overwrite=True
    )
if __name__ == "__main__":
    main()