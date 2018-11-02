#!/usr/bin/env python
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import datetime, os, sys, re, requests, json, logging, traceback, argparse, shutil, glob
import zipfile
from urlparse import urlparse
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
import boto
import osaka.main
import numpy as np
import scipy.spatial
from osgeo import gdal

import ConfigParser
import StringIO

from hysds.orchestrator import submit_job
import hysds.orchestrator
from hysds.celery import app
from hysds.dataset_ingest import ingest
from hysds_commons.job_rest_utils import single_process_and_submission

from subprocess import check_call

# disable warnings for SSL verification
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# all file types
ALL_TYPES = []

# zip types
ZIP_TYPE = ["zip"]
ALL_TYPES.extend(ZIP_TYPE)

# scale range
SCALE_RANGE=[0, 7500]



def verify(path, file_type):
    """Verify downloaded file is okay by checking that it can
       be unzipped/untarred."""

    test_dir = "./extract_test"
    if file_type in ZIP_TYPE:
        if not zipfile.is_zipfile(path):
            raise RuntimeError("%s is not a zipfile." % path)
        with zipfile.ZipFile(path, 'r') as f:
            f.extractall(test_dir)
        shutil.rmtree(test_dir, ignore_errors=True)
    else:
        raise NotImplementedError("Failed to verify %s is file type %s." % \
                                  (path, file_type))


def extract(zip_file):
    """Extract the zipfile."""

    with zipfile.ZipFile(zip_file, 'r') as zf:
        prod_dir = zip_file.replace(".zip", "")
        zf.extractall(prod_dir)
    return prod_dir


def download(download_url, dest, oauth_url):
    # download
    logging.info("Downloading %s to %s." % (download_url, dest))
    try:
        osaka.main.get(download_url, dest, params={"oauth": oauth_url}, measure=True, output="./pge_metrics.json")
    except Exception, e:
        tb = traceback.format_exc()
        logging.error("Failed to download %s to %s: %s" % (download_url,
                                                           dest, tb))
        raise


def create_metadata(alos2_md_file, download_url):
    # TODO: Some of these are hardcoded! Do we need them?
    metadata = {}
    # open summary.txt to extract metadata
    # extract information from summary see: https://www.eorc.jaxa.jp/ALOS-2/en/doc/fdata/PALSAR-2_xx_Format_GeoTIFF_E_r.pdf
    logging.info("Extracting metadata from %s" % alos2_md_file)
    dummy_section = "summary"
    with open(alos2_md_file, 'r') as f:
        # need to add dummy section for config parse to read .properties file
        summary_string = '[%s]\n' % dummy_section + f.read()
    summary_string = summary_string.replace('"', '')
    buf = StringIO.StringIO(summary_string)
    config = ConfigParser.ConfigParser()
    config.readfp(buf)

    # parse the metadata from summary.txt
    alos2md = {}
    for name, value in config.items(dummy_section):
        alos2md[name] = value

    metadata['alos2md'] = alos2md

    # facetview filters
    dataset_name = metadata['alos2md']['scs_sceneid'] + "_" + metadata['alos2md']['pds_productid']
    metadata['prod_name'] = dataset_name
    metadata['spacecraftName'] = dataset_name[0:5]
    metadata['dataset_type'] = dataset_name[0:5]
    metadata['orbitNumber'] = int(dataset_name[5:10])
    metadata['frameID'] = int(dataset_name[10:14])
    # this emprical forumla to get path/track number is derived from Eric Lindsey's modeling and fits for all L1.1 data
    metadata['trackNumber'] = int((14*metadata['orbitNumber']+24) % 207)
    prod_datetime = datetime.datetime.strptime(dataset_name[15:21], '%y%m%d')
    prod_date = prod_datetime.strftime("%Y-%m-%d")
    metadata['prod_date'] = prod_date

    # TODO: not sure if this is the right way to expose this in Facet Filters, using CSK's metadata structure
    dfdn = {"AcquistionMode": dataset_name[22:25],
            "LookSide": dataset_name[25]}
    metadata['dfdn'] = dfdn

    metadata['lookDirection'] = "right" if dataset_name[25] is "R" else "left"
    metadata['level'] = "L" + dataset_name[26:29]
    metadata['processingOption'] = dataset_name[29]
    metadata['mapProjection'] = dataset_name[30]
    metadata['direction'] = "ascending" if dataset_name[31] is "A" else "descending"

    # others
    metadata['dataset'] = "ALOS2_GeoTIFF"
    metadata['source'] = "jaxa"
    metadata['download_url'] = download_url
    location = {}
    location['type'] = 'Polygon'
    location['coordinates'] = [[
        [float(metadata['alos2md']['img_imagescenelefttoplongitude']), float(metadata['alos2md']['img_imagescenelefttoplatitude'])],
        [float(metadata['alos2md']['img_imagescenerighttoplongitude']), float(metadata['alos2md']['img_imagescenerighttoplatitude'])],
        [float(metadata['alos2md']['img_imagescenerightbottomlongitude']), float(metadata['alos2md']['img_imagescenerightbottomlatitude'])],
        [float(metadata['alos2md']['img_imagesceneleftbottomlongitude']), float(metadata['alos2md']['img_imagesceneleftbottomlatitude'])],
        [float(metadata['alos2md']['img_imagescenelefttoplongitude']), float(metadata['alos2md']['img_imagescenelefttoplatitude'])]

    ]]
    metadata['location'] = location

    # # Add metadata from context.json
    # # copy _context.json if it exists
    # ctx = {}
    # ctx_file = "_context.json"
    # if os.path.exists(ctx_file):
    #     with open(ctx_file) as f:
    #         ctx = json.load(f)

    return metadata


def create_dataset(metadata):
    logging.info("Extracting datasets from metadata")
    # get settings for dataset version
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'settings.json')
    if not os.path.exists(settings_file):
        settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                     'settings.json.tmpl')
    settings = json.load(open(settings_file))

    # datasets.json
    # extract metadata for datasets
    dataset = {
        'version': settings['ALOS2_INGEST_VERSION'],
        'label': metadata['prod_name'],
        'starttime': datetime.datetime.strptime(metadata['alos2md']['img_scenestartdatetime'], '%Y%m%d %H:%M:%S.%f').strftime("%Y-%m-%dT%H:%M:%S.%f"),
        'endtime': datetime.datetime.strptime(metadata['alos2md']['img_sceneenddatetime'], '%Y%m%d %H:%M:%S.%f').strftime("%Y-%m-%dT%H:%M:%S.%f")
    }
    dataset['location'] = metadata['location']

    return dataset


def gdal_translate(outfile, infile, options_string):
    cmd = "gdal_translate {} {} {}".format(options_string, infile, outfile)
    logging.info("cmd: %s" % cmd)
    return check_call(cmd,  shell=True)

def get_bounding_polygon(vrt_file):
    '''
    Get the minimum bounding region
    @param path - path to h5 file from which to read TS data
    '''
    ds = gdal.Open(vrt_file)
    #Read out the first data frame, lats vector and lons vector.
    data = np.array(ds.GetRasterBand(1).ReadAsArray())
    logging.info("Array size of data {}".format(data.shape))
    lats, lons = get_geocoded_coords(vrt_file)
    logging.info("Array size of lats {}".format(lats.shape))
    logging.info("Array size of lons {}".format(lons.shape))

    #Create a grid of lon, lat pairs
    coords = np.dstack(np.meshgrid(lons,lats))
    #Calculate any point in the data that is not 0, and grab the coordinates
    inx = np.nonzero(data)
    points = coords[inx]
    #Calculate the convex-hull of the data points.  This will be a mimimum
    #bounding convex-polygon.
    hull = scipy.spatial.ConvexHull(points)
    #Harvest the points and make it a loop
    pts = [list(pt) for pt in hull.points[hull.vertices]]
    logging.info("Number of vertices: {}".format(len(pts)))
    pts.append(pts[0])
    return pts


def get_geocoded_coords(vrt_file):
    """Return geocoded coordinates of radar pixels."""

    # extract geo-coded corner coordinates
    ds = gdal.Open(vrt_file)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    lon_arr = list(range(0, cols))
    lat_arr = list(range(0, rows))
    lons = np.empty((cols,))
    lats = np.empty((rows,))
    for py in lat_arr:
        lats[py] = gt[3] + (py * gt[5])
    for px in lon_arr:
        lons[px] = gt[0] + (px * gt[1])
    return lats, lons

# ONLY FOR L2.1 which is Geo-coded (Map projection based on north-oriented map direction)
def get_swath_polygon_coords(processed_tif):
    # create vrt file with wgs84 coordinates
    file_basename = os.path.splitext(processed_tif)[0]
    cmd = "gdalwarp -dstnodata 0 -dstalpha -of vrt -t_srs EPSG:4326 {} {}.vrt".format(processed_tif, file_basename)
    logging.info("cmd: %s" % cmd)
    check_call(cmd, shell=True)

    logging.info('Getting polygon of satellite footprint swath.')
    polygon_coords = get_bounding_polygon("{}.vrt".format(file_basename))
    logging.info("Coordinates of subswath polygon: {}".format(polygon_coords))

    return polygon_coords

def process_geotiff_disp(infile):
    # removes nodata value from original geotiff file from jaxa
    outfile = os.path.splitext(infile)[0] + "_disp.tif"
    logging.info("Removing nodata and scaling intensity from %s to %s. Scale intensity at %s"
                 % (infile, outfile, SCALE_RANGE))
    options_string = '-of GTiff -ot Byte -scale {} {} 0 255 -a_nodata 0'.format(SCALE_RANGE[0], SCALE_RANGE[1])
    gdal_translate(outfile, infile, options_string)
    return outfile


def create_tiled_layer(output_dir, tiff_file, zoom=[0, 8]):
    # create tiles from geotiff for facetView dispaly
    logging.info("Generating tiles.")
    zoom_i = zoom[0]
    zoom_f = zoom[1]

    while zoom_f > zoom_i:
        try:
            cmd = "gdal2tiles.py -z {}-{} -p mercator -a 0,0,0 {} {}".format(zoom_i, zoom_f, tiff_file, output_dir)
            logging.info("cmd: %s" % cmd)
            check_call(cmd, shell=True)
            break
        except Exception as e:
            logging.warn("Got exception running {}: {}".format(cmd, str(e)))
            logging.warn("Traceback: {}".format(traceback.format_exc()))
            zoom_f -= 1


def create_product_browse(tiff_file):
    logging.info("Creating browse png from %s" % tiff_file)
    options_string = '-of PNG -outsize 10% 10%'
    out_file = os.path.splitext(tiff_file)[0] + '.browse.png'
    out_file_small = os.path.splitext(tiff_file)[0] + '.browse_small.png'
    gdal_translate(out_file, tiff_file, options_string)
    os.system("convert -resize 250x250 %s %s" % (out_file, out_file_small))
    return


def create_product_kmz(tiff_file):
    logging.info("Creating KMZ from %s" % tiff_file)
    out_kmz = os.path.splitext(tiff_file)[0] + ".kmz"
    options_string = '-of KMLSUPEROVERLAY -ot Byte'
    gdal_translate(out_kmz, tiff_file, options_string)
    return


def ingest_alos2(download_url, file_type, path_number=None, oauth_url=None):
    """Download file, push to repo and submit job for extraction."""

    # get filename
    pri_zip_path = os.path.basename(download_url)
    download(download_url, pri_zip_path, oauth_url)

    # verify downloaded file was not corrupted
    logging.info("Verifying %s is file type %s." % (pri_zip_path, file_type))
    try:
        verify(pri_zip_path, file_type)
        sec_zip_dir = extract(pri_zip_path)

        # unzip the second layer to gather metadata
        sec_zip_file = glob.glob(os.path.join(sec_zip_dir,'*.zip'))
        if not len(sec_zip_file) == 1:
            raise RuntimeError("Unable to find second zipfile under %s" % sec_zip_dir)

        logging.info("Verifying %s is file type %s." % (sec_zip_file[0], file_type))
        verify(sec_zip_file[0], file_type)
        product_dir = extract(sec_zip_file[0])

    except Exception, e:
        tb = traceback.format_exc()
        logging.error("Failed to verify and extract files of type %s: %s" % \
                      (file_type, tb))
        raise

    # create met.json
    alos2_md_file = os.path.join(product_dir, "summary.txt")
    metadata = create_metadata(alos2_md_file, download_url)

    #checks path number formulation:
    if path_number:
        path_num = int(float(path_number))
        logging.info("Checking manual input path number {} against formulated path number {}"
                     .format(path_num, metadata['trackNumber']))
        if path_num != metadata['trackNumber']:
            raise RuntimeError("There might be an error in the formulation of path number. "
                               "Formulated path_number: {} | Manual input path_number: {}"
                               .format(metadata['trackNumber'], path_num))


    # create dataset.json
    dataset = create_dataset(metadata)

    # create the product directory
    dataset_name = metadata['prod_name']
    proddir = os.path.join(".", dataset_name)
    os.makedirs(proddir)
    # move all files forward
    files = os.listdir(product_dir)
    for f in files:
        shutil.move(os.path.join(product_dir, f), proddir)


    # create post products
    tiff_regex = re.compile("IMG-([A-Z]{2})-ALOS2(.{27}).tif")
    tiff_files = [f for f in os.listdir(proddir) if tiff_regex.match(f)]

    tile_md = {"tiles": True, "tile_layers": []}

    # we need to override the coordinates bbox to cover actula swath if dataset is Level2.1
    # L2.1 is Geo-coded (Map projection based on north-oriented map direction)
    need_swath_poly = "2.1" in dataset_name
    tile_output_dir = "{}/tiles/".format(proddir)

    for tf in tiff_files:
        tif_file_path = os.path.join(proddir, tf)
        # process the geotiff to remove nodata
        processed_tif_disp = process_geotiff_disp(tif_file_path)

        # create the layer for facet view (only one layer created)
        if not os.path.isdir(tile_output_dir):
            layer = tiff_regex.match(tf).group(1)
            create_tiled_layer(os.path.join(tile_output_dir, layer), processed_tif_disp, zoom=[0, 14])
            tile_md["tile_layers"].append(layer)

        # create the browse pngs
        create_product_browse(processed_tif_disp)

        create_product_kmz(processed_tif_disp)

        if need_swath_poly:
            coordinates = get_swath_polygon_coords(processed_tif_disp)
            # Override cooirdinates from summary.txt
            metadata['location']['coordinates'] = [coordinates]
            dataset['location']['coordinates'] = [coordinates]
            # do this once only
            need_swath_poly = False


    #udpate the tiles
    metadata.update(tile_md)

    # dump metadata
    with open(os.path.join(proddir, dataset_name + ".met.json"), "w") as f:
        json.dump(metadata, f, indent=2)
        f.close()

    # dump dataset
    with open(os.path.join(proddir, dataset_name + ".dataset.json"), "w") as f:
        json.dump(dataset, f, indent=2)
        f.close()

    # remove unwanted zips
    shutil.rmtree(sec_zip_dir, ignore_errors=True)
    # retaining primary zip, we can delete it if we want
    # os.remove(pri_zip_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("download_url", help="download file URL " +
                                             "(credentials stored " +
                                             "in .netrc)")
    parser.add_argument("file_type", help="download file type to verify",
                        choices=ALL_TYPES)
    parser.add_argument("--path_number_to_check", help="Path number provided from ALOS2 Ordering system to "
                                                     "check against empirical formulation.", required=False)
    parser.add_argument("--oauth_url", help="OAuth authentication URL " +
                                            "(credentials stored in " +
                                            ".netrc)", required=False)

    args = parser.parse_args()

    try:
        ingest_alos2(args.download_url, args.file_type, path_number=args.path_number_to_check, oauth_url=args.oauth_url)
    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
