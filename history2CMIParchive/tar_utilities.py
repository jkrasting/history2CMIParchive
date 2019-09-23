import tarfile as _tarfile


def list_files_archive(archivefile):
    """ returns list of netcdf files contained in a tar file """
    assert _tarfile.is_tarfile(archivefile)
    arch = _tarfile.open(name=archivefile, mode='r')
    allfiles = arch.getnames()
    ncfiles = []
    for f in allfiles:
        if f.endswith('.nc'):
            ncfiles.append(f)
    arch.close()
    return ncfiles


def extract_ncfile_from_archive(archivefile, ncfile, destination):
    """ extract a single netcdf file from archivefile into destination """
    assert _tarfile.is_tarfile(archivefile)
    arch = _tarfile.open(name=archivefile, mode='r')
    arch.extract(ncfile, path=destination)
    arch.close()
    return None
