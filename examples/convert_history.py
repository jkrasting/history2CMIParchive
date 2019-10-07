#!/usr/bin/env python

import history2CMIParchive
import subprocess as sp
import uuid
import warnings
import argparse
import os

parser = argparse.ArgumentParser(description='convert history tar to \
                                              zarr zipstore')

parser.add_argument('-H', '--historydir', type=str, required=True,
                    help="path to history tar files")

parser.add_argument('-o', '--outputdir', type=str, required=True,
                    help="path to output zarr store")

parser.add_argument('-y', '--year', type=int, required=True,
                    help="year to process")

parser.add_argument('-s', '--startyear', type=int, required=True,
                    help="starting year of run")

parser.add_argument('-d', '--domain', type=str, required=False,
                    help="domain (e.g. OM4p25,...)")

parser.add_argument('-i', '--ignore', nargs='+', required=False,
                    help="types to be ignored")

parser.add_argument("--Wall", help='show warnings')

args = parser.parse_args()

if not args.Wall:
    warnings.filterwarnings("ignore")

# create tmp directory (GFDL specific)
user = sp.check_output('whoami', shell=True).decode('utf-8').replace('\n','')
uniqueid = uuid.uuid4()
tmpdir = f'/work/{user}/{uniqueid}'
check = sp.check_call(f'mkdir {tmpdir}', shell=True)

# setting up options
kwargs = {}
# check if this is a new store
newstore = True if (args.year == args.startyear) else False
kwargs['newstore'] = newstore
# check for types to be ignored
if args.ignore is not None:
    kwargs['ignore_types'] = args.ignore

if args.domain is not None:
    kwargs['domain'] = args.domain

print(kwargs)

ctarfile = f'{args.year}0101.nc.tar'
check = history2CMIParchive.get_from_tape(args.historydir, ctarfile,
                                          site='gfdl')

history2CMIParchive.convert_archive_to_zarr_zipstore(args.historydir + os.sep + ctarfile,
                                                     args.outputdir, tmpdir,
                                                     **kwargs)
# clean up this year
check = sp.check_call(f'rm {tmpdir}/*.nc', shell=True)
# clean up tmp directory
#check = sp.check_call(f'rmdir {tmpdir}', shell=True)
