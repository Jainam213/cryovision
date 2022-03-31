import os
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt, make_path_filter
import geopandas as gpd
import rasterio as rio
import rasterio.mask
from tqdm import tqdm

from utils.constants import (
    BUFFER_RADIUS,
    GEOJSON_PATH,
    IMAGE_FILTER,
    PLATFORM_NAME,
    RGB_FILENAME,
    RGB_MASKED_FILENAME,
    SENTINEL_HOST,
    SENTINEL_PASSWORD,
    SENTINEL_USERNAME,
    SHAPEFILE,
    TILE_HEIGHT,
    TILE_WIDTH,
)
from utils.helpers import get_tiles

# Init Sentinel API

api = SentinelAPI(SENTINEL_USERNAME, SENTINEL_PASSWORD, SENTINEL_HOST)

# Import Shapefile

pingo = gpd.read_file(SHAPEFILE).to_crs("epsg:4326")
pingo.to_file(GEOJSON_PATH, driver="GeoJSON")

# Choose a pingo to track
pingo_number = 1

geojson = read_geojson(GEOJSON_PATH)["features"][pingo_number]
footprint = geojson_to_wkt(geojson)


# Get Image Data

images = api.query(
    footprint,
    platformname=PLATFORM_NAME,
    processinglevel="Level-2A",
    cloudcoverpercentage=(0, 10),
    limit=100,
)
dataframe = api.to_geodataframe(images)
dataframe_sorted = dataframe.sort_values(
    ["ingestiondate", "cloudcoverpercentage"], ascending=[False, True]
)

# Download Image Data set using Id
path_filter = make_path_filter(IMAGE_FILTER)
image_id = dataframe_sorted.index[0]
date = dataframe_sorted.generationdate[0].strftime("%d_%m_%Y")

download_data = api.download(image_id, nodefilter=path_filter)


# Process Image
main_path = download_data["node_path"][2:]
image_paths = list(download_data["nodes"].keys())[1:]

b2 = rio.open(main_path + list(download_data["nodes"].keys())[1:][0][1:])
b3 = rio.open(main_path + list(download_data["nodes"].keys())[2:][0][1:])
b4 = rio.open(main_path + list(download_data["nodes"].keys())[3:][0][1:])

os.mkdir(f"images/{image_id}_{date}")
os.mkdir(f"images/{image_id}_{date}/tiles")

in_path = f"images/{image_id}_{date}/"
out_path = f"images/{image_id}_{date}/tiles/"
output_filename = "tile_{}-{}.tif"

pingo_projection = pingo.loc[pingo_number:pingo_number].to_crs(b4.crs.data.get("init"))

with rio.open(
    os.path.join(in_path, RGB_FILENAME),
    "w",
    driver="GTiff",
    width=b4.width,
    height=b4.height,
    count=3,
    crs=b4.crs,
    transform=b4.transform,
    dtype=b4.dtypes[0],
) as rgb:
    rgb.write(b2.read(1), 1)
    rgb.write(b3.read(1), 2)
    rgb.write(b4.read(1), 3)
    rgb.close()

# Create Masked Image

with rio.open(os.path.join(in_path, RGB_FILENAME)) as src:
    out_image, out_transform = rasterio.mask.mask(
        src, pingo_projection.geometry.buffer(BUFFER_RADIUS), crop=True
    )
    out_meta = src.meta.copy()
    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
        }
    )

with rio.open(os.path.join(in_path, RGB_MASKED_FILENAME), "w", **out_meta) as dest:
    dest.write(out_image)

# Create Image tiles

with rio.open(os.path.join(in_path, RGB_MASKED_FILENAME)) as inds:
    tile_width, tile_height = TILE_WIDTH, TILE_HEIGHT
    meta = inds.meta.copy()
    tiles = list(get_tiles(inds))

    for window, transform in tqdm(tiles):
        meta["transform"] = transform
        meta["width"], meta["height"] = window.width, window.height
        outpath = os.path.join(
            out_path, output_filename.format(int(window.col_off), int(window.row_off))
        )
        patch = inds.read(window=window)
        if inds.read(window=window).min() > 0 and patch.shape == (
            3,
            TILE_WIDTH,
            TILE_HEIGHT,
        ):
            with rio.open(outpath, "w", **meta) as outds:
                outds.write(inds.read(window=window))
