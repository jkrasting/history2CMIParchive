import xarray as _xr


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
    ds = _xr.open_dataset(ncfile, chunks=chunks, decode_times=decode_times)
    return ds
