import xarray as _xr
from .tar_utilities import list_files_archive
from .tar_utilities import extract_ncfile_from_archive
from .zarr_stores import create_zarr_zipstore
from .zarr_stores import append_to_zarr_zipstore
import os

list_straits = ['Agulhas_section', 'Barents_opening', 'Bering_Strait', 
                'Davis_Strait', 'Denmark_Strait', 'Drake_Passage', 
                'English_Channel', 'Faroe_Scotland', 'Florida_Bahamas',
                'Fram_Strait', 'Gibraltar_Strait', 'Iceland_Faroe_U', 
                'Iceland_Faroe_V', 'Iceland_Norway', 'Indonesian_Throughflow',
                'Mozambique_Channel', 'Pacific_undercurrent', 'Taiwan_Luzon',
                'Windward_Passage' ]

coords_need_be_ignored = ['xq', 'yq', 'xh', 'yh', 'zl', 'zi', 'nv', 'z_l', 'z_i',
                          'xh_sub01', 'yh_sub01', 'xq_sub01', 'yq_sub01',
                          'xh_sub02', 'yh_sub02', 'xq_sub02', 'yq_sub02',
                          'xh_sub03', 'yh_sub03', 'xq_sub03', 'yq_sub03',
                          'xh_sub04', 'yh_sub04', 'xq_sub04', 'yq_sub04',
                          'xT', 'xTe', 'yT', 'yTe',
                          'Layer', 'Interface']

datasets_need_be_ignored = ['static', 'Vertical_coordinate']

def convert_archive_to_zarr_zipstore(archive, ppdir, workdir, ignore_types=[],
                                     ignore_vars=[], newstore=False,
                                     grid='gn', tag='v1', time='time',
                                     domain='OM4p25', chunks=None):

    # figure out what files are in the archive
    ncfiles = list_files_archive(archive)

    # do not append coordinates,...
    if not newstore:
        ignore_types += datasets_need_be_ignored

    # build a list of acceptable files
    files_to_convert = []
    for ncfile in ncfiles:
        addfile = True # first guess: add file to list
        for filetype in ignore_types:
            if filetype in ncfile:
                addfile=False
            else:
                pass
        if addfile:
            files_to_convert.append(ncfile)

    print(files_to_convert)
    for ncfile in files_to_convert:
        # extract the file
        extract_ncfile_from_archive(archive, ncfile, workdir)
        # define path to zarr store
        print(f'Extract {ncfile} to {workdir}/{ncfile}')
        rootdir, component_code = define_store_path(workdir + os.sep + ncfile,
                                                    ppdir, grid=grid, tag=tag,
                                                    timedim=time)
        print(f'Copying into zarr store at {rootdir}')
        # decide chunking if none provided
        if chunks is None:
            chunks = chunk_choice(component_code, domain=domain)
        # open dataset
        ds = open_dataset(workdir + os.sep + ncfile, chunks, decode_times=False)
        # create store
        if newstore:
            create_zarr_zipstore(ds, rootdir, ignore_vars=ignore_vars)
        else:
            # coordinates don't like to be appended
            ignore_vars += coords_need_be_ignored
            append_to_zarr_zipstore(ds, rootdir, 
                                    ignore_vars=ignore_vars, concat_dim=time)

    return None


def chunk_choice(component_code, domain='OM4p25'):
    """ default chunking for standard domains """ 
    if domain == 'OM4p25':
        chunks = chunk_choice_OM4p25(component_code)
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
    """ define template path where content of ncfile should be copied,
        <VARNAME> will be updated by create_store.
    """
    cvarname = '<VARNAME>'

    for strait in list_straits:
        if strait in ncfile:
            cvarname += f'_{strait}'

    component_code = define_component_code(ncfile, timedim=timedim)
    store_path = f'{ppdir}/{component_code}/{cvarname}/{grid}/{tag}'
    return store_path, component_code


def define_component_code(ncfile, timedim='time'):
    """ based on filename, infer what component it belongs to """

    code = ''
    # ESM component
    if 'ice' in ncfile:
        code = 'SI'
    elif 'ocean' in ncfile:
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
    tmp.close()
    # re-open with correct chunking
    if len(useable_chunks) > 1:
        ds = _xr.open_dataset(ncfile, chunks=useable_chunks, decode_times=decode_times)
    else:
        ds = _xr.open_dataset(ncfile, decode_times=decode_times)
    return ds
