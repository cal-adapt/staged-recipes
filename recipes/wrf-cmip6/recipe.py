import pandas as pd
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

path = ('s3://wrf-cmip6-noversioning/downscaled_products/era/6hourly/'
        '{time.year}/d01/wrfout_d01_{time:%Y-%m-%d_%X}')

def preproc(ds, filename):
    ds = ds.rename({'XLAT': 'lat', 'XLONG': 'lon',
                    'LANDMASK': 'landmask', 'LAKEMASK': 'lakemask'})
    ds = ds.assign_coords(
        lat=ds.coords['lat'].squeeze('Time'),
        lon=ds.coords['lon'].squeeze('Time'),
        landmask=ds.landmask.squeeze('Time'),
        lakemask=ds.lakemask.squeeze('Time'))
    ds = ds.rename({'south_north': 'y', 'west_east': 'x'})
    ds = ds.drop('XTIME')
    t = ds['Times']
    t_strs = [s.decode().replace('_', ' ') for s in t.data]
    t_dt = pd.to_datetime(t_strs)
    ds = ds.rename({'Time': 'time'})
    ds = ds.assign(time=t_dt)
    ds = ds.drop('Times')
    return ds

dates = pd.date_range('1950-09-01', '1950-12-31', freq='6H')
tdim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(path.format, tdim)
recipe = XarrayZarrRecipe(pattern, process_input=preproc, inputs_per_chunk=2500)
