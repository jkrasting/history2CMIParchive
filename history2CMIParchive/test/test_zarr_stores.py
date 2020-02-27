import xarray as xr
import numpy as np
import subprocess as sp
import pytest
import os
import yaml


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
ds_bad1 = ds_ref.copy(deep=True)
ds_bad1['time'] = np.arange(6, 18)

ds_bad2 = ds_ref.copy(deep=True)
ds_bad2['time'] = np.arange(1, 13)

ds_bad3 = ds_ref.copy(deep=True)
ds_bad3['time'] = np.arange(14, 26)


def read_store(tmpdir, varname, storetype, consolidated):
    """ helper function """
    if storetype == 'directory':
        storename = f'{tmpdir}/{varname}'
    elif storetype == 'zip':
        storename = f'{tmpdir}/{varname}.zip'
    # open variable from zarr
    temp_from_zarr = xr.open_zarr(storename,
                                  consolidated=consolidated)
    return temp_from_zarr


@pytest.mark.skip(reason="obsoleted function")
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


@pytest.mark.skip(reason="obsoleted function")
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


@pytest.mark.parametrize("storetype", ['directory', 'zip'])
@pytest.mark.parametrize("consolidated", [True, False])
@pytest.mark.parametrize("write_yaml", [True, False])
def test_write_to_zarr_store(tmpdir, storetype, consolidated, write_yaml):
    from history2CMIParchive.zarr_stores import write_to_zarr_store

    # ---------------------------------------------------------------
    # test the write mode

    rebuild_dict = {'path': '',
                    'options': '',
                    'varname': 'thetao',
                    'files': [{'tar1': 'file1'}]}

    write_to_zarr_store(ds_ref['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated,
                        write_yaml=write_yaml, rebuild_dict=rebuild_dict)

    if storetype == 'directory':
        # check permission for a random chunk
        fstat = os.stat(f'{tmpdir}/thetao/thetao/0.0.0.0')
        assert oct(fstat.st_mode)[-3:] == '644'
    elif storetype == 'zip':
        # check permission for a random chunk in zip
        _ = sp.check_call(f'cd {tmpdir} ; unzip thetao.zip', shell=True)
        fstat = os.stat(f'{tmpdir}/thetao/0.0.0.0')
        assert oct(fstat.st_mode)[-3:] == '644'

    temp_from_zarr = read_store(tmpdir, 'thetao', storetype, consolidated)
    assert ds_ref['thetao'] == temp_from_zarr

    if write_yaml:
        assert os.path.exists(f'{tmpdir}/thetao.yml')
        with open(f'{tmpdir}/thetao.yml', 'r') as f:
            store_history = yaml.load(f, Loader=yaml.FullLoader)
            assert len(store_history['files']) == 1
    # ---------------------------------------------------------------
    # try to overwrite
    write_to_zarr_store(ds_ref['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated,
                        overwrite=True, write_yaml=write_yaml,
                        rebuild_dict=rebuild_dict)

    if write_yaml:
        assert os.path.exists(f'{tmpdir}/thetao.yml')
        with open(f'{tmpdir}/thetao.yml', 'r') as f:
            store_history = yaml.load(f, Loader=yaml.FullLoader)
            assert len(store_history['files']) == 1
    # ---------------------------------------------------------------
    # test append mode with good data

    rebuild_dict2 = {'path': '',
                     'options': '',
                     'varname': 'thetao',
                     'files': [{'tar2': 'file2'}]}

    write_to_zarr_store(ds_add['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated,
                        write_yaml=write_yaml, rebuild_dict=rebuild_dict2)

    ds_update = xr.concat([ds_ref, ds_add], dim='time')
    temp_from_zarr = read_store(tmpdir, 'thetao', storetype, consolidated)
    assert ds_update['thetao'] == temp_from_zarr

    if write_yaml:
        assert os.path.exists(f'{tmpdir}/thetao.yml')
        with open(f'{tmpdir}/thetao.yml', 'r') as f:
            store_history = yaml.load(f, Loader=yaml.FullLoader)
            assert len(store_history['files']) == 2

    # ----------------------------------------------------------------
    # now with bad data, first we overwrite:
    write_to_zarr_store(ds_ref['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated,
                        overwrite=True)

    write_to_zarr_store(ds_bad1['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated)

    # check the store have not been updated
    temp_from_zarr = read_store(tmpdir, 'thetao', storetype, consolidated)
    assert ds_ref['thetao'] == temp_from_zarr

    write_to_zarr_store(ds_bad2['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated)

    # check the store have not been updated
    temp_from_zarr = read_store(tmpdir, 'thetao', storetype, consolidated)
    assert ds_ref['thetao'] == temp_from_zarr

    write_to_zarr_store(ds_bad3['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated)

    # check the store have not been updated
    temp_from_zarr = read_store(tmpdir, 'thetao', storetype, consolidated)
    assert ds_ref['thetao'] == temp_from_zarr

    # ---------------------------------------------------------------
    # overwrite a second time
    write_to_zarr_store(ds_ref['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated,
                        overwrite=True)

    temp_from_zarr = read_store(tmpdir, 'thetao', storetype, consolidated)
    assert ds_ref['thetao'] == temp_from_zarr

    # ---------------------------------------------------------------
    # test to write/append coordinates without concat dimension
    write_to_zarr_store(ds_ref['xh'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated)

    write_to_zarr_store(ds_ref['xh'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated)

    # ---------------------------------------------------------------
    # test behavior with incomplete file

    # write dataset
    write_to_zarr_store(ds_ref['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated,
                        overwrite=True)

    # mess up the store
    if storetype == 'directory':
        sp.check_call(f'mv {tmpdir}/thetao {tmpdir}/thetao_tmp', shell=True)
    elif storetype == 'zip':
        sp.check_call(f'mv {tmpdir}/thetao.zip {tmpdir}/thetao.zip_tmp',
                      shell=True)

    # try adding
    write_to_zarr_store(ds_add['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated)
    return None


@pytest.mark.parametrize("storetype", ['directory', 'zip'])
@pytest.mark.parametrize("consolidated", [True, False])
def test_appending_needed(tmpdir, storetype, consolidated):
    from history2CMIParchive.zarr_stores import write_to_zarr_store
    from history2CMIParchive.zarr_stores import appending_needed

    # ---------------------------------------------------------------
    # write a first store
    write_to_zarr_store(ds_ref['thetao'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated)

    # ----------------------------------------------------------------
    # test good store
    check = appending_needed(f'{tmpdir}', 'thetao',
                             storetype, ds_add['thetao'],
                             concat_dim='time', consolidated=consolidated)
    assert check

    # ----------------------------------------------------------------
    # test bad stores
    check = appending_needed(f'{tmpdir}', 'thetao',
                             storetype, ds_bad1['thetao'],
                             concat_dim='time', consolidated=consolidated)
    assert not check

    check = appending_needed(f'{tmpdir}', 'thetao',
                             storetype, ds_bad2['thetao'],
                             concat_dim='time', consolidated=consolidated)
    assert not check

    check = appending_needed(f'{tmpdir}', 'thetao',
                             storetype, ds_bad3['thetao'],
                             concat_dim='time', consolidated=consolidated)
    assert not check

    # ---------------------------------------------------------------
    # test it works on coord too
    write_to_zarr_store(ds_ref['xh'], f'{tmpdir}', site='',
                        storetype=storetype, consolidated=consolidated)
    check = appending_needed(f'{tmpdir}', 'xh',
                             storetype, ds_add['xh'],
                             concat_dim='time', consolidated=consolidated)
    assert not check
