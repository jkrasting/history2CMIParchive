import xarray as _xr
import zarr as _zarr
import subprocess
from .site_specific import get_from_tape
import os


def create_zarr_store(ds, rootdir, ignore_vars=[], storetype='directory'):
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
            print(f'writing {variable}')
            # create a bogus dataset to copy a single variable
            tmp = _xr.Dataset()
            tmp[variable] = ds[variable]
            # update output directory with variable name
            outputdir = rootdir.replace('<VARNAME>', variable)
            # create the output directory
            check = subprocess.check_call(f'mkdir -p {outputdir}', shell=True)
            # create a zarr store in write mode
            if storetype == 'directory' and not os.path.exists(f'{outputdir}/{variable}'):
                store = _zarr.DirectoryStore(f'{outputdir}/{variable}')
                # then copy to zarr
                tmp.to_zarr(store)
            elif storetype == 'zip':
                store = _zarr.ZipStore(f'{outputdir}/{variable}.zip', mode='w')
                # then copy to zarr
                tmp.to_zarr(store)
            # and close store
            if storetype == 'zip':
                store.close()
            tmp.close()
            # fix permissions (only possible for DirectoryStore)
            if storetype == 'directory':
                check = subprocess.check_call(f'chmod -R go+rX {outputdir}/{variable}', shell=True)
            else:
                pass
    return None


def append_to_zarr_store(ds, rootdir, ignore_vars=[], concat_dim='time',
                         storetype='directory', site='gfdl'):
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
            print(f'writing {variable}')
            # update output directory with variable name
            outputdir = rootdir.replace('<VARNAME>', variable)
            # create the output directory
            check = subprocess.check_call(f'mkdir -p {outputdir}', shell=True)
            # get from tape if needed
            if storetype == 'zip':
                check = get_from_tape(f'{outputdir}', f'{variable}.zip',
                                      site=site)
            # open current store
            if storetype == 'directory':
                current = _xr.open_zarr(f'{outputdir}/{variable}',
                                        decode_times=False)
            elif storetype == 'zip':
                current = _xr.open_zarr(f'{outputdir}/{variable}.zip',
                                        decode_times=False)
            last_current_frame = current[concat_dim].values[-1]
            new_frame = ds[concat_dim].values[0]
            current.close()
            if new_frame > last_current_frame:
                print('new data available, appending to store')
                # check = subprocess.check_call(f'cp {outputdir}/{variable}.zip {outputdir}/{variable}.zip.bkup', shell=True)
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
                tmp.to_zarr(store, mode='a', append_dim=concat_dim)
                # and close store
                if storetype == 'zip':
                    store.close()
                tmp.close()
                # fix permissions (only possible for DirectoryStore)
                if storetype == 'directory':
                    check = subprocess.check_call(f'chmod -R go+rX {outputdir}/{variable}', shell=True)
                else:
                    pass
            else:
                print('data already present, skipping')
    return None
