import xarray as _xr
from .tar_utilities import list_files_archive
from .tar_utilities import extract_ncfile_from_archive
from .zarr_stores import write_to_zarr_store
import subprocess as sp
from .yaml_utils import create_build_history


# list of straits used in the MOM model
list_straits = ['Agulhas_section', 'Barents_opening', 'Bering_Strait',
                'Davis_Strait', 'Denmark_Strait', 'Drake_Passage',
                'English_Channel', 'Faroe_Scotland', 'Florida_Bahamas',
                'Fram_Strait', 'Gibraltar_Strait', 'Iceland_Faroe_U',
                'Iceland_Faroe_V', 'Iceland_Norway', 'Indonesian_Throughflow',
                'Mozambique_Channel', 'Pacific_undercurrent', 'Taiwan_Luzon',
                'Windward_Passage']


def convert_archive_to_zarr_store(archive='', outputdir='', workdir='',
                                  ignore_types=[],
                                  overwrite=False, consolidated=True,
                                  timedim='time', chunks=None,
                                  storetype='directory', grid='gn', tag='v1',
                                  domain='OM4p25', site=None, debug=False,
                                  write_yaml=True):
    '''extract files from tar archive and convert to zarr stores '''

    # figure out what files are in the archive
    ncfiles = list_files_archive(archive)

    if debug:
        print(ncfiles)

    # build a list of acceptable files
    files_to_convert = []
    for ncfile in ncfiles:
        addfile = True  # first guess: add file to list
        for filetype in ignore_types:
            if filetype in ncfile:
                addfile = False
            else:
                pass
        if addfile:
            files_to_convert.append(ncfile)

    if debug:
        print(files_to_convert)

    for ncfile in files_to_convert:
        # extract the file
        extract_ncfile_from_archive(archive, ncfile, workdir)
        # and process it
        export_nc_out_to_zarr_stores(ncfile=f'{workdir}/{ncfile}',
                                     outputdir=outputdir,
                                     archive=archive,
                                     overwrite=overwrite,
                                     consolidated=consolidated,
                                     timedim=timedim,
                                     chunks=chunks,
                                     storetype=storetype,
                                     grid=grid, tag=tag,
                                     domain=domain, site=site,
                                     debug=debug, write_yaml=write_yaml)

    return None


def export_nc_out_to_zarr_stores(ncfile='',
                                 outputdir='',
                                 archive='',
                                 overwrite=False,
                                 consolidated=True,
                                 timedim='time',
                                 chunks=None,
                                 storetype='directory',
                                 grid='gn', tag='v1',
                                 domain='OM4p25', site=None,
                                 debug=False, write_yaml=True):

    """convert all variables form netcdf file and distribute into
    zarr stores"""

    # the name of ncfile and its time is used to create a code
    component_code = define_component_code(ncfile, timedim=timedim)
    # decide chunking if none provided
    if chunks is None:
        chunks = chunk_choice(component_code, domain=domain)
    if debug:
        print(f'component_code is {component_code}')
        print(f'domain is {domain}')
        print(f'chunks are {chunks}')
    # open dataset
    ds = open_dataset(ncfile, chunks, decode_times=False)

    for variable in ds.variables:
        # define path to zarr store
        storepath = infer_store_path(ncfile, variable, outputdir,
                                     component_code, grid=grid,
                                     tag=tag)
        # and create path
        check = sp.check_call(f'mkdir -p {storepath}', shell=True)
        exit_code(check)
        # build dict for yaml file
        if len(archive) > 0:
            tarfile = archive.replace('/', ' ').split()[-1]
            historydir = archive.replace(tarfile, '')
        else:
            tarfile = 'unknown'
            historydir = 'unknown'
        files = [{tarfile: ncfile.replace('/', ' ').split()[-1]}]
        rebuild_dict = create_build_history(historydir, outputdir,
                                            storetype, consolidated,
                                            timedim, chunks, grid, tag,
                                            domain, site, variable, files)
        # write the store
        if debug:
            print(f'writing {variable} into {storepath}')
        write_to_zarr_store(ds[variable], storepath,
                            concat_dim=timedim, storetype=storetype,
                            consolidated=consolidated,
                            overwrite=overwrite, site=site, debug=debug,
                            write_yaml=write_yaml, rebuild_dict=rebuild_dict)
    return None


def chunk_choice(component_code, domain='OM4p25'):
    """ default chunking for standard domains """
    if domain == 'OM4p25':
        chunks = chunk_choice_OM4p25(component_code)
    elif domain == 'OM4p125':
        chunks = chunk_choice_OM4p125(component_code)
    else:
        print('Unknown domain, defaulting to time-based')
        chunks = chunk_choice_default(component_code)

    return chunks


def chunk_choice_OM4p25(component_code):
    """ default chunking for OM4p25 """
    # set to one vertical level
    chunks = {'z_i': 1, 'z_l': 1, 'rho2_l': 1, 'rho2_i': 1, 'zi': 1, 'zl': 1}
    if 'mon' in component_code:
        chunks.update({'time': 12})
    elif 'day' in component_code:
        chunks.update({'time': 12})
    elif 'yr' in component_code:
        chunks.update({'time': 1})
    else:
        chunks.update({'time': 1})
    return chunks


def chunk_choice_OM4p125(component_code):
    """ default chunking for OM4p125 """
    # set to one vertical level
    chunks = {'z_i': 1, 'z_l': 1, 'rho2_l': 1, 'rho2_i': 1, 'zi': 1, 'zl': 1}
    if 'mon' in component_code:
        chunks['time'] = 1
    elif 'day' in component_code:
        chunks['time'] = 1
    elif 'yr' in component_code:
        chunks['time'] = 1
    else:
        chunks['time'] = 1
    return chunks


def chunk_choice_default(component_code):
    """ default chunking for unknown domain """
    # set to one vertical level
    chunks = {'z_i': 1, 'z_l': 1, 'rho2_l': 1, 'rho2_i': 1, 'zi': 1, 'zl': 1}
    if 'mon' in component_code:
        chunks.update({'time': 1})
    elif 'day' in component_code:
        chunks.update({'time': 1})
    elif 'yr' in component_code:
        chunks.update({'time': 1})
    return chunks


def define_store_path(ncfile, ppdir, grid='gn', tag='v1', timedim='time'):
    """ OBSOLETE: define template path where content of ncfile should be copied,
        <VARNAME> will be updated by create_store.
    """
    cvarname = '<VARNAME>'

    # make separate tree for straits
    for strait in list_straits:
        if strait in ncfile:
            grid += f'_{strait}'

    # also separate downsampled datasets
    if '_d2' in ncfile:
        grid += f'_d2'

    # also separate density space datasets
    if 'rho2' in ncfile:
        grid += f'_rho2'

    # also separate datasets on woa z-levels
    if '_z.' in ncfile:
        grid += f'_z'

    component_code = define_component_code(ncfile, timedim=timedim)
    store_path = f'{ppdir}/{component_code}/{cvarname}/{grid}/{tag}'
    return store_path, component_code


def infer_store_path(ncfile, varname, ppdir, component_code,
                     grid='gn', tag='v1'):
    """ create the name of the store based on data specs.
    """

    # make separate tree for straits
    for strait in list_straits:
        if strait in ncfile:
            grid += f'_{strait}'

    # also separate downsampled datasets
    if '_d2' in ncfile:
        grid += f'_d2'

    # also separate density space datasets
    if 'rho2' in ncfile:
        grid += f'_rho2'

    # also separate datasets on woa z-levels
    if '_z.' in ncfile:
        grid += f'_z'

    store_path = f'{ppdir}/{component_code}/{varname}/{grid}/{tag}'
    return store_path


def define_component_code(ncfile, timedim='time'):
    """ based on filename, infer what component it belongs to

    PARAMETERS:
    -----------

    ncfile: str
        input netcdf file
    timedim: str
        time dimension in file

    RETURNS:
    --------

    code: str

    """

    code = ''
    # ESM component
    if 'ice' in ncfile:
        code = 'SI'
    elif 'ocean' in ncfile:
        code = 'O'
    elif 'Vertical_coordinate' in ncfile:
        code = 'O'
    else:
        print('Unknown component for file', ncfile)
    # Frequency
    if 'static' in ncfile:
        code += 'fx'
    elif 'month' in ncfile:
        code += 'mon'
    elif 'annual' in ncfile:
        code += 'yr'
    elif 'daily' in ncfile:
        code += 'day'
    else:
        # infer from the file time variable
        print('Infer frequency from time counter (slower)')
        tmp = _xr.open_dataset(ncfile, decode_times=False)
        if timedim in tmp.dims:
            ntimes = len(tmp[timedim])
        else:
            ntimes = 0
        tmp.close()
        if ntimes == 0:
            code += 'fx'
        elif ntimes == 1:
            code += 'yr'
        elif ntimes == 12:
            code += 'mon'
        elif (ntimes > 359) and (ntimes < 367):
            code += 'day'
        else:
            print('Cannot infer frequency of file', ncfile)

    return code


def open_dataset(ncfile, chunks, decode_times=False):
    """ A wrapper around xarray.open_dataset, in case some
    custom code is needed.

    PARAMETERS:
    ===========

    ncfile: str

    chunks: list

    decode_times: bool

    RETURNS:
    ========

    ds: xarray.Dataset
    """
    # first open without chunks to get dimensions:
    tmp = _xr.open_dataset(ncfile, decode_times=decode_times)
    # check if dimensions in chunk exist in dataset (else xarray returns error)
    useable_chunks = {}
    for k in chunks.keys():
        if k in tmp.dims:
            useable_chunks[k] = chunks[k]
    # chunks on other dimensions should be size of dims
    # and not infered from input netcdf file
    for d in tmp.dims:
        if d not in useable_chunks:
            useable_chunks[d] = len(tmp[d])
    tmp.close()
    # re-open with correct chunking
    if len(useable_chunks) > 1:
        ds = _xr.open_dataset(ncfile, chunks=useable_chunks,
                              decode_times=decode_times)
    else:
        ds = _xr.open_dataset(ncfile, decode_times=decode_times)
    return ds


def exit_code(return_code):
    import sys
    """exit with return code """
    if return_code != 0:
        sys.exit(return_code)
        return None
