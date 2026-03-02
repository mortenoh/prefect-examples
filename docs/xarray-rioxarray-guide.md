# xarray and rioxarray Guide

## Overview

**xarray** is a Python library for working with labelled multi-dimensional
arrays. It builds on NumPy and integrates with pandas, providing a
`DataArray` (single variable with labelled dimensions) and `Dataset`
(collection of DataArrays sharing dimensions).

**rioxarray** is an xarray extension that adds geospatial raster operations
via the `.rio` accessor. It wraps `rasterio` (Python bindings for GDAL) to
provide CRS handling, reprojection, clipping, and GeoTIFF I/O directly on
xarray objects.

Together, they let you treat raster geospatial data as labelled arrays with
geographic coordinates, CRS awareness, and nodata handling.

## Installation

```bash
uv add rioxarray
```

This pulls in `rasterio` and `xarray` as transitive dependencies. GDAL
(the C library under rasterio) is included as a binary wheel.

## Reading a GeoTIFF

```python
import rioxarray

# Open a GeoTIFF as an xarray DataArray
da = rioxarray.open_rasterio("population.tif")

# da is an xarray.DataArray with dimensions (band, y, x)
print(da.dims)    # ('band', 'y', 'x')
print(da.shape)   # e.g. (1, 4800, 5400)
print(da.rio.crs) # CRS.from_epsg(4326)
```

### Key attributes

| Attribute | Description |
|-----------|-------------|
| `da.dims` | Dimension names, typically `('band', 'y', 'x')` |
| `da.coords` | Coordinate labels -- `y` = latitude, `x` = longitude |
| `da.rio.crs` | Coordinate Reference System |
| `da.rio.nodata` | NoData sentinel value |
| `da.rio.resolution()` | Pixel size as `(x_res, y_res)` tuple |
| `da.rio.bounds()` | Geographic bounds `(left, bottom, right, top)` |

## Common operations

### Clipping to a polygon

```python
import rioxarray

da = rioxarray.open_rasterio("population.tif")

# GeoJSON geometry dict
geometry = {
    "type": "Polygon",
    "coordinates": [[[100.0, 14.0], [108.0, 14.0], [108.0, 23.0], [100.0, 23.0], [100.0, 14.0]]]
}

# Clip the raster to the polygon boundary
clipped = da.rio.clip([geometry], all_touched=True)
```

The `all_touched=True` parameter includes any pixel that touches the polygon
boundary (not just those whose centre falls inside). This avoids edge effects
with small polygons.

The CRS of the geometry is assumed to match the raster CRS. If they differ,
pass `crs=` explicitly.

### Handling nodata

```python
# Check for nodata value
nodata = clipped.rio.nodata  # e.g. -99999.0

# Mask nodata pixels to NaN for safe aggregation
if nodata is not None:
    clipped = clipped.where(clipped != nodata)
```

### Summing pixel values (zonal statistics)

```python
import numpy as np

# Sum all valid pixel values -- gives total population in the polygon
total = float(np.nansum(clipped.values))
```

`np.nansum` ignores `NaN` values from the nodata mask.

### Reprojecting

```python
# Reproject to a different CRS
da_utm = da.rio.reproject("EPSG:32648")  # UTM zone 48N
```

### Writing a GeoTIFF

```python
# Write a DataArray back to disk
da.rio.to_raster("output.tif")
```

## The .rio accessor

Importing `rioxarray` registers the `.rio` accessor on all xarray DataArrays
and Datasets. You must import it even if you do not call it directly:

```python
import rioxarray  # noqa: F401  -- registers .rio accessor
```

Without this import, calling `.rio.clip()` or `.rio.crs` will raise an
`AttributeError`.

### Commonly used .rio methods

| Method | Description |
|--------|-------------|
| `.rio.clip(geometries)` | Clip raster to polygon boundaries |
| `.rio.reproject(crs)` | Reproject to a different CRS |
| `.rio.to_raster(path)` | Write to GeoTIFF |
| `.rio.set_crs(crs)` | Set or override the CRS |
| `.rio.set_nodata(value)` | Set or override the nodata value |
| `.rio.pad_box(minx, miny, maxx, maxy)` | Pad raster to bounding box |

### Commonly used .rio properties

| Property | Description |
|----------|-------------|
| `.rio.crs` | The CRS of the raster |
| `.rio.nodata` | The nodata sentinel value |
| `.rio.bounds()` | Geographic bounding box |
| `.rio.resolution()` | Pixel size |
| `.rio.width` | Number of columns |
| `.rio.height` | Number of rows |

## Dimensions and coordinates

A GeoTIFF opened with `rioxarray.open_rasterio()` has three dimensions:

- **band** -- raster band index (usually just `1` for single-band files)
- **y** -- latitude values (descending order for north-up images)
- **x** -- longitude values (ascending order)

```python
da = rioxarray.open_rasterio("population.tif")

# Select a single band (most GeoTIFFs have one band)
da_band1 = da.sel(band=1)

# Slice by geographic coordinates
subset = da.sel(x=slice(100, 105), y=slice(20, 15))  # note: y is descending
```

## Memory considerations

For large rasters, `rioxarray.open_rasterio()` uses lazy loading by default
(backed by `rasterio`'s windowed reading). Data is read into memory only when
you access `.values` or perform computations. For explicit chunked/lazy
processing, use the `chunks` parameter:

```python
# Lazy loading with dask chunks
da = rioxarray.open_rasterio("large_file.tif", chunks={"x": 1024, "y": 1024})
```

For our WorldPop use case, each file is ~15 MB and fits comfortably in memory,
so chunking is not needed.

## Usage in this project

The `src/prefect_examples/worldpop_geotiff.py` module uses rioxarray for
zonal population extraction:

```python
import numpy as np
import rioxarray

def zonal_population(tiff_path, geometry):
    da = rioxarray.open_rasterio(tiff_path)
    try:
        clipped = da.rio.clip([geometry], all_touched=True)
        nodata = clipped.rio.nodata
        if nodata is not None:
            clipped = clipped.where(clipped != nodata)
        total = float(np.nansum(clipped.values))
    finally:
        da.close()
    return total
```

This opens the raster, clips it to an org unit polygon, masks nodata, and
sums the remaining pixel values to get the population inside that boundary.

## Further reading

- [xarray documentation](https://docs.xarray.dev/)
- [rioxarray documentation](https://corteva.github.io/rioxarray/)
- [rasterio documentation](https://rasterio.readthedocs.io/)
