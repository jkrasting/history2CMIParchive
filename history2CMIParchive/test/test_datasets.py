import xarray as xr
import numpy as np
import pytest
import subprocess as sp
import os
import yaml


def define_test_dataset(resolution=1, nz=15, nt=12):
    """ create test datasets with variable size """
    lon1 = np.arange(0, 360, resolution)
    lat1 = np.arange(-90, 90, resolution)
    z = np.arange(nz)
    lon, lat = np.meshgrid(lon1, lat1)
    temp = 20 * np.cos(np.pi * lat / 180) * np.ones((nt, nz,) + lon.shape)
    salt = 34 * np.ones((nt, nz,) + lon.shape)
    for k in np.arange(nz):
        temp[:, k, :, :] = temp[:, k, :, :] * np.exp(-k/nz)
        salt[:, k, :, :] = salt[:, k, :, :] + (k / nz)

    # write
    ds = xr.Dataset({'thetao': (['time', 'z', 'y', 'x'], temp),
                     'so': (['time', 'z', 'y', 'x'], salt)},
                    coords={'xh': (['xh'], lon1),
                            'yh': (['yh'], lat1),
                            'z_l': (['z_l'], z),
                            'time': (['time'], np.arange(nt))})
    return ds


def test_define_component_code(tmpdir):
    from history2CMIParchive.datasets import define_component_code

    ds_1 = define_test_dataset(resolution=1, nt=12)
    ds_1.to_netcdf(f'{tmpdir}/ocean_monthly.nc')
    code = define_component_code(f'{tmpdir}/ocean_monthly.nc')
    assert code == 'Omon'
    ds_1.to_netcdf(f'{tmpdir}/ocean_unknown_freq.nc')
    code = define_component_code(f'{tmpdir}/ocean_unknown_freq.nc')
    assert code == 'Omon'

    ds_2 = define_test_dataset(resolution=1, nt=1)
    ds_2.to_netcdf(f'{tmpdir}/ocean_annual.nc')
    code = define_component_code(f'{tmpdir}/ocean_annual.nc')
    assert code == 'Oyr'
    ds_2.to_netcdf(f'{tmpdir}/ocean_unknown_freq.nc')
    code = define_component_code(f'{tmpdir}/ocean_unknown_freq.nc')
    assert code == 'Oyr'


def test_define_store_path(tmpdir):
    from history2CMIParchive.datasets import define_store_path

    ds_1 = define_test_dataset()
    ds_1.to_netcdf(f'{tmpdir}/ocean_monthly.nc')
    ds_1.to_netcdf(f'{tmpdir}/ocean_unknown.nc')

    ppdir = f'{tmpdir}/pp/'
    grid = 'gn'
    tag = 'v1'
    timedim = 'time'

    storepath, code = define_store_path(f'{tmpdir}/ocean_monthly.nc',
                                        ppdir, grid=grid,
                                        tag=tag, timedim=timedim)
    assert code == 'Omon'
    assert storepath == f'{ppdir}/{code}/<VARNAME>/{grid}/{tag}'

    storepath, code = define_store_path(f'{tmpdir}/ocean_unknown.nc',
                                        ppdir, grid=grid,
                                        tag=tag, timedim=timedim)
    assert code == 'Omon'
    assert storepath == f'{ppdir}/{code}/<VARNAME>/{grid}/{tag}'


def test_infer_store_path(tmpdir):
    from history2CMIParchive.datasets import infer_store_path
    from history2CMIParchive.datasets import define_component_code

    ds_1 = define_test_dataset()
    ds_1.to_netcdf(f'{tmpdir}/ocean_monthly.nc')
    ds_1.to_netcdf(f'{tmpdir}/ocean_unknown.nc')

    code = define_component_code(f'{tmpdir}/ocean_monthly.nc',
                                 timedim='time')
    ppdir = f'{tmpdir}/pp/'
    grid = 'gn'
    tag = 'v1'

    storepath = infer_store_path(f'{tmpdir}/ocean_monthly.nc',
                                 'thetao', ppdir, code, grid=grid,
                                 tag=tag)
    assert storepath == f'{ppdir}/{code}/thetao/{grid}/{tag}'

    storepath = infer_store_path(f'{tmpdir}/ocean_unknown.nc',
                                 'so', ppdir, code, grid=grid,
                                 tag=tag)
    assert storepath == f'{ppdir}/{code}/so/{grid}/{tag}'


@pytest.mark.parametrize("code", ['Omon', 'Oday', 'Oyr', 'Ofx',
                                  'SImon', 'SIday', 'SIyr', 'SIfx'])
def test_chunk_choice_default(code):
    from history2CMIParchive.datasets import chunk_choice_default
    chunk = chunk_choice_default(code)
    assert isinstance(chunk, dict)


@pytest.mark.parametrize("code", ['Omon', 'Oday', 'Oyr', 'Ofx',
                                  'SImon', 'SIday', 'SIyr', 'SIfx'])
def test_chunk_choice_OM4p25(code):
    from history2CMIParchive.datasets import chunk_choice_OM4p25
    chunk = chunk_choice_OM4p25(code)
    assert isinstance(chunk, dict)


@pytest.mark.parametrize("code", ['Omon', 'Oday', 'Oyr', 'Ofx',
                                  'SImon', 'SIday', 'SIyr', 'SIfx'])
def test_chunk_choice_OM4p125(code):
    from history2CMIParchive.datasets import chunk_choice_OM4p125
    chunk = chunk_choice_OM4p125(code)
    assert isinstance(chunk, dict)


@pytest.mark.parametrize("code", ['Omon', 'Oday', 'Oyr', 'Ofx',
                                  'SImon', 'SIday', 'SIyr', 'SIfx'])
@pytest.mark.parametrize("domain", ['OM4p25', 'OM4p125', 'OM4'])
def test_chunk_choice(code, domain):
    from history2CMIParchive.datasets import chunk_choice
    chunk = chunk_choice(code, domain=domain)
    assert isinstance(chunk, dict)


@pytest.mark.parametrize("storetype", ['directory', 'zip'])
@pytest.mark.parametrize("consolidated", [True, False])
@pytest.mark.parametrize("write_yaml", [True, False])
def test_export_nc_out_to_zarr_stores(tmpdir, storetype, consolidated,
                                      write_yaml):
    from history2CMIParchive.datasets import export_nc_out_to_zarr_stores
    from history2CMIParchive.datasets import define_component_code
    from history2CMIParchive.datasets import infer_store_path

    ds_1 = define_test_dataset(resolution=1, nt=12)
    ds_1.to_netcdf(f'{tmpdir}/ocean_monthly.nc')

    ppdir = f'{tmpdir}/pp/'

    export_nc_out_to_zarr_stores(ncfile=f'{tmpdir}/ocean_monthly.nc',
                                 outputdir=ppdir,
                                 overwrite=False,
                                 consolidated=consolidated,
                                 timedim='time',
                                 chunks=None,
                                 storetype=storetype,
                                 grid='gn', tag='v1',
                                 domain='OM4', site='',
                                 write_yaml=write_yaml)

    comp_code = define_component_code(f'{tmpdir}/ocean_monthly.nc')
    storepath = infer_store_path(f'{tmpdir}/ocean_monthly.nc',
                                 'thetao', ppdir, comp_code,
                                 grid='gn', tag='v1')

    if storetype == 'directory':
        check_ds = xr.open_zarr(f'{storepath}/thetao')
    elif storetype == 'zip':
        check_ds = xr.open_zarr(f'{storepath}/thetao.zip')

    assert check_ds['thetao'].equals(ds_1['thetao'])

    if write_yaml:
        assert os.path.exists(f'{storepath}/thetao.yml')
        with open(f'{storepath}/thetao.yml') as f:
            store_history = yaml.load(f, Loader=yaml.FullLoader)
            f.close()
        assert len(store_history['files']) == 1

    # ---------------------------------------------------------
    # test appending

    ds_2 = ds_1.copy(deep=True)
    ds_2['time'] = ds_1['time'] + 12

    ds_2.to_netcdf(f'{tmpdir}/ocean_monthly_to_append.nc')

    export_nc_out_to_zarr_stores(ncfile=f'{tmpdir}/ocean_monthly_to_append.nc',
                                 outputdir=ppdir,
                                 overwrite=False,
                                 consolidated=consolidated,
                                 timedim='time',
                                 chunks=None,
                                 storetype=storetype,
                                 grid='gn', tag='v1',
                                 domain='OM4', site='')

    if storetype == 'directory':
        check_ds = xr.open_zarr(f'{storepath}/thetao')
    elif storetype == 'zip':
        check_ds = xr.open_zarr(f'{storepath}/thetao.zip')

    assert len(check_ds['time']) == 24

    if write_yaml:
        assert os.path.exists(f'{storepath}/thetao.yml')
        with open(f'{storepath}/thetao.yml') as f:
            store_history = yaml.load(f, Loader=yaml.FullLoader)
            f.close()
        assert len(store_history['files']) == 2

    # ---------------------------------------------------------
    # test overwrite

    export_nc_out_to_zarr_stores(ncfile=f'{tmpdir}/ocean_monthly.nc',
                                 outputdir=ppdir,
                                 overwrite=True,
                                 consolidated=consolidated,
                                 timedim='time',
                                 chunks=None,
                                 storetype=storetype,
                                 grid='gn', tag='v1',
                                 domain='OM4', site='')

    if storetype == 'directory':
        check_ds = xr.open_zarr(f'{storepath}/thetao')
    elif storetype == 'zip':
        check_ds = xr.open_zarr(f'{storepath}/thetao.zip')

    assert len(check_ds['time']) == 12

    if write_yaml:
        assert os.path.exists(f'{storepath}/thetao.yml')
        with open(f'{storepath}/thetao.yml') as f:
            store_history = yaml.load(f, Loader=yaml.FullLoader)
            f.close()
        assert len(store_history['files']) == 1


@pytest.mark.parametrize("storetype", ['directory', 'zip'])
@pytest.mark.parametrize("consolidated", [True, False])
@pytest.mark.parametrize("write_yaml", [True, False])
def test_convert_archive_to_zarr_store(tmpdir, storetype, consolidated,
                                       write_yaml):
    from history2CMIParchive.datasets import convert_archive_to_zarr_store

    # set up the directory structure
    hisdir = f'{tmpdir}/history'
    workdir = f'{tmpdir}/tmp'
    ppdir = f'{tmpdir}/pp'

    _ = sp.check_call(f'mkdir -p {hisdir}', shell=True)
    _ = sp.check_call(f'mkdir -p {workdir}', shell=True)
    _ = sp.check_call(f'mkdir -p {ppdir}', shell=True)

    ds_1 = define_test_dataset(resolution=1, nt=12)
    ds_1.to_netcdf(f'{hisdir}/ocean_monthly.nc')
    ds_2 = define_test_dataset(resolution=1, nt=1)
    ds_2.to_netcdf(f'{hisdir}/ocean_annual.nc')

    _ = sp.check_call(f'touch {hisdir}/ocean_static.nc', shell=True)
    _ = sp.check_call(f'tar -cf {hisdir}/1900101.tar {hisdir}/*.nc',
                      shell=True)
    _ = sp.check_call(f'rm {hisdir}/*.nc', shell=True)

    assert os.path.exists(f'{hisdir}/1900101.tar')

    # run the conversion
    convert_archive_to_zarr_store(archive=f'{hisdir}/1900101.tar',
                                  outputdir=ppdir, workdir=workdir,
                                  ignore_types=['static'],
                                  overwrite=False, consolidated=consolidated,
                                  timedim='time', chunks=None,
                                  storetype=storetype, grid='gn', tag='v1',
                                  domain='OM4', site='', debug=True,
                                  write_yaml=write_yaml)

    # test the data is produced
    assert os.path.exists(f'{workdir}/{hisdir}/ocean_annual.nc')
    assert os.path.exists(f'{workdir}/{hisdir}/ocean_monthly.nc')
    assert not os.path.exists(f'{workdir}/{hisdir}/ocean_static.nc')

    assert os.path.exists(f'{ppdir}/Omon/thetao/gn/v1/')
    assert os.path.exists(f'{ppdir}/Omon/so/gn/v1/')
    assert os.path.exists(f'{ppdir}/Oyr/thetao/gn/v1/')
    assert os.path.exists(f'{ppdir}/Oyr/so/gn/v1/')

    if write_yaml:
        assert os.path.exists(f'{ppdir}/Omon/thetao/gn/v1/thetao.yml')
        with open(f'{ppdir}/Omon/thetao/gn/v1/thetao.yml') as f:
            store_history = yaml.load(f, Loader=yaml.FullLoader)
            f.close()
        assert len(store_history['files']) == 1
