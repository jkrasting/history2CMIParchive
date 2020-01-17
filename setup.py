''' setup for history2CMIParchive '''
import setuptools

setuptools.setup(
    name="history2CMIParchive",
    version="0.0.1",
    author="Raphael Dussin",
    author_email="raphael.dussin@gmail.com",
    description=("A package to create CMIP-like dataset from history files"),
    license="GPLv3",
    keywords="",
    url="https://github.com/raphaeldussin/history2CMIParchive",
    packages=['history2CMIParchive'],
    scripts=['history2CMIParchive/exe/history_nc_to_zarr.py']
)
