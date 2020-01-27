#!/usr/bin/env python

from history2CMIParchive.datasets import convert_archive_to_zarr_store
import warnings
import argparse

parser = argparse.ArgumentParser(description='convert history tar file to \
                                              zarr stores')

parser.add_argument('-i', '--archive', type=str, required=True,
                    help="input tar file")

parser.add_argument('-o', '--outputdir', type=str, required=True,
                    help="path to output zarr store")

parser.add_argument('-w', '--workdir', type=str, required=True,
                    help="path to temp dir for extract nc")

parser.add_argument('-s', '--storetype', type=str, required=True,
                    default='directory', help="zarr store type")

parser.add_argument('-d', '--domain', type=str, required=True,
                    default='OM4', help="model domain")

parser.add_argument('-O', '--overwrite', type=bool, required=False,
                    default=False, help="overwrite stores")

parser.add_argument('-C', '--consolidated', type=bool, required=False,
                    default=True, help="use consolidated zarr metadata")

parser.add_argument('-T', '--timedim', type=str, required=False,
                    default='time', help="name of time dimension")

parser.add_argument('-K', '--chunks', type=dict, required=False,
                    default=None, help="dict of chunks size")

parser.add_argument('-G', '--grid', type=str, required=False,
                    default='gn', help="grid type (gn/gr)")

parser.add_argument('-t', '--tag', type=str, required=False,
                    default='v1', help="model version tag")

parser.add_argument('-S', '--site', type=str, required=False,
                    default='gfdl', help="site specific")

parser.add_argument('-I', '--ignore', nargs='+', required=False,
                    help="types to be ignored")

parser.add_argument('-D', '--debug', type=bool, required=False,
                    help="print debug information")

parser.add_argument("--Wall", help='show warnings')

args = parser.parse_args()

if not args.Wall:
    warnings.filterwarnings("ignore")

# build kwargs from args
kwargs = vars(args)
kwargs.pop('ignore', None)
kwargs.pop('Wall', None)

convert_archive_to_zarr_store(**kwargs)
