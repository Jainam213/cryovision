from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt, make_path_filter
import geopandas as gpd
import rasterio as rio
import rasterio.mask

from utils.constants import (
    GEOJSON_PATH,
    IMAGE_FILTER,
    SENTINEL_HOST,
    SENTINEL_PASSWORD,
    SENTINEL_USERNAME,
    SHAPEFILE,
)

# Init Sentinel API

api = SentinelAPI(SENTINEL_USERNAME, SENTINEL_PASSWORD, SENTINEL_HOST)

# Import Shapefile

pingo = gpd.read_file(SHAPEFILE).to_crs("epsg:4326")
pingo.to_file(GEOJSON_PATH, driver="GeoJSON")

pingo_number = 1
geojson = read_geojson(GEOJSON_PATH)["features"][pingo_number]
footprint = geojson_to_wkt(geojson)


# Get Image Data

images = api.query(
    footprint,
    platformname="Sentinel-2",
    processinglevel="Level-2A",
    cloudcoverpercentage=(0, 10),
    limit=10,
)
dataframe = api.to_geodataframe(images)
dataframe_sorted = dataframe.sort_values(
    ["ingestiondate", "cloudcoverpercentage"], ascending=[False, True]
)

# Download Image Data set using Id
path_filter = make_path_filter(IMAGE_FILTER)
download_data = api.download(dataframe_sorted.index[0], nodefilter=path_filter)


# Process Image
main_path = download_data["node_path"][2:]
image_paths = list(download_data["nodes"].keys())[1:]

b2 = rio.open(main_path + list(download_data["nodes"].keys())[1:][0][1:])
b3 = rio.open(main_path + list(download_data["nodes"].keys())[2:][0][1:])
b4 = rio.open(main_path + list(download_data["nodes"].keys())[3:][0][1:])

pingo_projection = pingo.to_crs(b4.crs.data.get("init"))

with rio.open(
    "RGB.tiff",
    "w",
    driver="GTiff",
    width=b4.width,
    height=b4.height,
    count=3,
    crs=b4.crs,
    transform=b4.transform,
    dtype=b4.dtypes[0],
) as rgb:
    rgb.write(b2.read(1), 3)
    rgb.write(b3.read(1), 2)
    rgb.write(b4.read(1), 1)
    rgb.close()

# Create Masked Image

with rio.open("RGB.tiff") as src:
    out_image, out_transform = rasterio.mask.mask(
        src, pingo_projection.geometry.buffer(40000.0), crop=True
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

with rasterio.open("RGB_masked.tiff", "w", **out_meta) as dest:
    dest.write(out_image)

