import xarray as _xr
import zarr as _zarr
import subprocess
from .site_specific import get_from_tape


def create_zarr_zipstore(ds, rootdir, ignore_vars=[]):
    """
    Write each variable from a xarray Dataset ds into a new zarr ZipStore
    under the root directory rootdir, excluding optional variables from
    ignore_vars.

    PARAMETERS:
    ===========

    ds: xarray.Dataset

    rootdir: str

    ignore_vars: list

    RETURNS:
    ========

    None
    """

    for variable in ds.variables:
        if variable not in ignore_vars:
            # update output directory with variable name
            outputdir = rootdir.replace('<VARNAME>', variable)
            # create the output directory
            check = subprocess.check_call(f'mkdir -p {outputdir}', shell=True)
            # create a zarr store in write mode
            store = _zarr.ZipStore(f'{outputdir}/{variable}.zip', mode='w')
            # create a bogus dataset to copy a single variable
            tmp = _xr.Dataset()
            tmp[variable] = ds[variable]
            # then copy to zarr
            tmp.to_zarr(store)
            # and close store
            store.close()
    return None


def append_to_zarr_zipstore(ds, rootdir, ignore_vars=[], concat_dim='time', site='gfdl'):
    """
    Write each variable from a xarray Dataset ds into an existing zarr ZipStore
    under the root directory rootdir, excluding optional variables from
    ignore_vars and concatenate along dimension concat_dim.

    PARAMETERS:
    ===========

    ds: xarray.Dataset

    rootdir: str

    ignore_vars: list

    concat_dim: str

    RETURNS:
    ========

    None
    """

    for variable in ds.variables:
        if variable not in ignore_vars:
            # update output directory with variable name
            outputdir = rootdir.replace('<VARNAME>', variable)
            # create the output directory
            check = subprocess.check_call(f'mkdir -p {outputdir}', shell=True)
            # get from tape if needed
            check = get_from_tape(f'{outputdir}', f'{variable}.zip', site=site)
            # open a zarr store in append mode
            store = _zarr.ZipStore(f'{outputdir}/{variable}.zip', mode='a')
            # create a bogus dataset to copy a single variable
            tmp = _xr.Dataset()
            tmp[variable] = ds[variable]
            # then append to zarr
            #print(f'appending variable {variable} to store {store}')
            #print(tmp)
            #verif = _xr.open_zarr(store)
            #print(verif)
            tmp.to_zarr(store, mode='a', append_dim=concat_dim)
            # and close store
            store.close()
    return None
