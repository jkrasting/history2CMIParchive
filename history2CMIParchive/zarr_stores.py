import xarray as _xr
import zarr as _zarr
import subprocess
from .site_specific import get_from_tape
import os


def create_zarr_store(ds, rootdir, ignore_vars=[],
                      storetype='directory',
                      consolidated=True):
    """
    Write each variable from a xarray Dataset ds into a new zarr ZipStore
    under the root directory rootdir, excluding optional variables from
    ignore_vars.

    PARAMETERS:
    ===========

    ds: xarray.Dataset
        input dataset
    rootdir: str
        root path to the zarr stores
    ignore_vars: list
        variables to ignore
    storetype: str
        zarr store type (directory, zip)
    consolidated: logical
        zarr option to store (default = True)

    RETURNS:
    ========

    None
    """

    for variable in ds.variables:
        if variable not in ignore_vars:
            print(f'writing {variable}')
            # create a bogus dataset to copy a single variable
            tmp = _xr.Dataset()
            tmp[variable] = ds[variable]
            # update output directory with variable name
            outputdir = rootdir.replace('<VARNAME>', variable)
            # create the output directory
            check = subprocess.check_call(f'mkdir -p {outputdir}', shell=True)
            exit_code(check)
            # create a zarr store in write mode
            store_exists = os.path.exists(f'{outputdir}/{variable}')
            if storetype == 'directory' and not store_exists:
                store = _zarr.DirectoryStore(f'{outputdir}/{variable}')
                # then copy to zarr
                tmp.to_zarr(store, consolidated=consolidated)
            elif storetype == 'zip':
                store = _zarr.ZipStore(f'{outputdir}/{variable}.zip', mode='w')
                # then copy to zarr
                tmp.to_zarr(store, mode='w', consolidated=consolidated)
            # and close store
            if storetype == 'zip':
                store.close()
            tmp.close()
    return None


def append_to_zarr_store(ds, rootdir, ignore_vars=[], concat_dim='time',
                         storetype='directory',
                         consolidated=True, site='gfdl'):
    """
    Write each variable from a xarray Dataset ds into an existing zarr ZipStore
    under the root directory rootdir, excluding optional variables from
    ignore_vars and concatenate along dimension concat_dim.

    PARAMETERS:
    ===========

    ds: xarray.Dataset
        input dataset
    rootdir: str
        root path to the zarr stores
    ignore_vars: list
        variables to ignore
    concat_dim: str
        dimension along which to append to store
    storetype: str
        zarr store type (directory, zip)
    consolidated: logical
        zarr option to store (default = True)
    site: str
        control archive retrieval from tape (default = 'gfdl')

    RETURNS:
    ========

    None
    """

    for variable in ds.variables:
        if concat_dim not in ds[variable].dims:
            ignore_vars.append(variable)
        if variable not in ignore_vars:
            assert concat_dim in ds[variable].dims
            print(f'writing {variable}')
            # update output directory with variable name
            outputdir = rootdir.replace('<VARNAME>', variable)
            # create the output directory
            check = subprocess.check_call(f'mkdir -p {outputdir}', shell=True)
            # get from tape if needed
            if storetype == 'zip':
                check = get_from_tape(f'{outputdir}', f'{variable}.zip',
                                      site=site)
                exit_code(check)
            # open current store
            if storetype == 'directory':
                current = _xr.open_zarr(f'{outputdir}/{variable}',
                                        decode_times=False,
                                        consolidated=consolidated)
            elif storetype == 'zip':
                current = _xr.open_zarr(f'{outputdir}/{variable}.zip',
                                        decode_times=False,
                                        consolidated=consolidated)
            last_current_frame = current[concat_dim].values[-1]
            new_frame = ds[concat_dim].values[0]
            current.close()
            if new_frame > last_current_frame:
                print('new data available, appending to store')
                # create a bogus dataset to copy a single variable
                tmp = _xr.Dataset()
                tmp[variable] = ds[variable]
                # open a zarr store in append mode
                if storetype == 'directory':
                    store = _zarr.DirectoryStore(f'{outputdir}/{variable}')
                elif storetype == 'zip':
                    store = _zarr.ZipStore(f'{outputdir}/{variable}.zip',
                                           mode='a')
                # then append to zarr
                tmp.to_zarr(store, mode='a',
                            append_dim=concat_dim,
                            consolidated=consolidated)
                # and close store
                if storetype == 'zip':
                    store.close()
                tmp.close()
            else:
                print('data already present, skipping')
    return None


def write_to_zarr_store(da, storepath, concat_dim='time',
                        storetype='directory', consolidated=True,
                        overwrite=False, site='gfdl'):
    """ create/append to a zarr store """

    # create temp dataset with new data
    varname = da.name
    ds = _xr.Dataset()
    ds[varname] = da

    # zarr store full name/path depends on type
    if storetype == 'directory':
        fstore = f'{storepath}/{varname}'
    elif storetype == 'zip':
        fstore = f'{storepath}/{varname}.zip'

    # check if file exists
    store_exists = True if (os.path.exists(fstore)) else False

    # by default, set write to true
    write_store = True

    # set zarr write/append mode
    if store_exists and not overwrite:
        zarrmode = 'a'
        zarr_kwargs = {'mode': zarrmode, 'append_dim': concat_dim}
        # edge case: if concat_dim not in dataarray, abort write
        if concat_dim not in ds[varname].dims:
            write_store = False
    else:
        zarrmode = 'w'
        zarr_kwargs = {'mode': zarrmode}

    # reload the store, if present
    if storetype == 'zip':
        check = get_from_tape(f'{storepath}', f'{varname}.zip',
                              site=site)
        exit_code(check)

    # check if append is the right thing to do
    # would return updated value of write_store

    if write_store:
        # open the store
        if storetype == 'directory':
            store = _zarr.DirectoryStore(fstore)
        elif storetype == 'zip':
            store = _zarr.ZipStore(fstore, mode=zarrmode)
        # write to store
        ds.to_zarr(store, consolidated=consolidated, **zarr_kwargs)
        # and close store
        if storetype == 'zip':
            store.close()
    ds.close()

    return None


def exit_code(return_code):
    import sys
    """exit with return code """
    if return_code != 0:
        sys.exit(return_code)
        return None
