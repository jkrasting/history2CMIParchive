import xarray as _xr
import zarr as _zarr


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
            # create a zarr store in write mode
            store = _zarr.ZipStore(f'{rootdir}/{variable}.zip', mode='w')
            # create a bogus dataset to copy a single variable
            tmp = _xr.Dataset()
            tmp[variable] = ds[variable]
            # then copy to zarr
            tmp.to_zarr(store)
            # and close store
            store.close()
    return None


def append_to_zarr_zipstore(ds, rootdir, ignore_vars=[], concat_dim='time'):
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
            # open a zarr store in append mode
            store = _zarr.ZipStore(f'{rootdir}/{variable}.zip', mode='a')
            # create a bogus dataset to copy a single variable
            tmp = _xr.Dataset()
            tmp[variable] = ds[variable]
            # then append to zarr
            tmp.to_zarr(store, mode='a', append_dim=concat_dim)
            # and close store
            store.close()
    return None
