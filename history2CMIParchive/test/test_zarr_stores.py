import xarray as xr
import numpy as np
import subprocess as sp
import pytest
import os


# test datasets
lon1 = np.arange(0, 360)
lat1 = np.arange(-90, 90)
z = np.arange(15)
lon, lat = np.meshgrid(lon1, lat1)
temp = 20 * np.cos(np.pi * lat / 180) * np.ones((12, 15,) + lon.shape)
salt = 34 * np.ones((12, 15,) + lon.shape)
for k in np.arange(15):
    temp[:, k, :, :] = temp[:, k, :, :] * np.exp(-k/15)
    salt[:, k, :, :] = salt[:, k, :, :] + (k / 15.)

# create
ds_ref = xr.Dataset({'thetao': (['time', 'z', 'y', 'x'], temp),
                     'so': (['time', 'z', 'y', 'x'], salt)},
                    coords={'xh': (['xh'], lon1),
                            'yh': (['yh'], lat1),
                            'z_l': (['z_l'], z),
                            'time': (['time'], np.arange(1, 13))})

# append
ds_add = ds_ref.copy(deep=True)
ds_add['time'] = np.arange(13, 25)

# bad appends
# ds_bad1 = ds_ref.copy(deep=True)
# ds_bad1['time'] = pd.date_range(start='1899-1-1', periods=12, freq='1M')

# ds_bad2 = ds_ref.copy(deep=True)
# ds_bad2['time'] = pd.date_range(start='1903-1-1', periods=12, freq='1M')


@pytest.mark.parametrize("storetype", ['directory', 'zip'])
@pytest.mark.parametrize("consolidated", [True, False])
def test_create_zarr_store(tmpdir, storetype, consolidated):
    from history2CMIParchive.zarr_stores import create_zarr_store
    rootdir = f'{tmpdir}/<VARNAME>'
    create_zarr_store(ds_ref, rootdir,
                      storetype=storetype,
                      consolidated=consolidated)

    if storetype == 'directory':
        # check permission for a random chunk
        fstat = os.stat(f'{tmpdir}/thetao/thetao/thetao/0.0.0.0')
        assert oct(fstat.st_mode)[-3:] == '644'
        # open variable from zarr
        temp_from_zarr = xr.open_zarr(f'{tmpdir}/thetao/thetao',
                                      consolidated=consolidated)
    elif storetype == 'zip':
        # check permission for a random chunk in zip
        _ = sp.check_call(f'cd {tmpdir}/thetao/; unzip thetao.zip', shell=True)
        fstat = os.stat(f'{tmpdir}/thetao/thetao/0.0.0.0')
        assert oct(fstat.st_mode)[-3:] == '644'
        # open variable from zarr
        temp_from_zarr = xr.open_zarr(f'{tmpdir}/thetao/thetao.zip',
                                      consolidated=consolidated)
    assert ds_ref['thetao'] == temp_from_zarr
    return None


@pytest.mark.parametrize("storetype", ['directory', 'zip'])
@pytest.mark.parametrize("consolidated", [True, False])
def test_append_zarr_store(tmpdir, storetype, consolidated):
    from history2CMIParchive.zarr_stores import create_zarr_store
    from history2CMIParchive.zarr_stores import append_to_zarr_store

    rootdir = f'{tmpdir}/<VARNAME>'
    create_zarr_store(ds_ref, rootdir, storetype=storetype)
    append_to_zarr_store(ds_add, rootdir,
                         ignore_vars=['xh', 'yh', 'z_l'],
                         storetype=storetype,
                         consolidated=consolidated,
                         site='')

    # test function filters out variables without concat dim
    append_to_zarr_store(ds_add, rootdir,
                         ignore_vars=[],
                         storetype=storetype,
                         consolidated=consolidated,
                         site='')

    if storetype == 'directory':
        # check permission for a random chunk
        fstat = os.stat(f'{tmpdir}/thetao/thetao/thetao/0.0.0.0')
        assert oct(fstat.st_mode)[-3:] == '644'
        # open variable from zarr
        temp_from_zarr = xr.open_zarr(f'{tmpdir}/thetao/thetao',
                                      consolidated=consolidated)
    elif storetype == 'zip':
        # open variable from zarr
        temp_from_zarr = xr.open_zarr(f'{tmpdir}/thetao/thetao.zip',
                                      consolidated=consolidated)
    ds_update = xr.concat([ds_ref, ds_add], dim='time')
    assert ds_update['thetao'] == temp_from_zarr
