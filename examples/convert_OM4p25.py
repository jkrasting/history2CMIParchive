#!/usr/bin/env python

import history2CMIParchive
import subprocess as sp
import uuid
import warnings

warnings.filterwarnings("ignore")

historydir = '/archive/Raphael.Dussin/xanadu_esm4_20190304_mom6_2019.08.08/OM4p25_JRA55do1.4_0netfw_cycle5/gfdl.ncrc4-intel16-prod/history/'
outputdir = '/work/Raphael.Dussin/zarr_stores/OM4p25_JRA55do1.4_0netfw_cycle5/'
tmpdir = '/work/Raphael.Dussin/' + str(uuid.uuid4())
firstyear = 1958
lastyear = 2018

#ignore_types_debug = ['ice', 'daily', 'month' ]
#ignore_types_debug = ['ice', 'daily', 'month', 'annual', 'Vertical' ]

# create tmp directory
check = sp.check_call(f'mkdir {tmpdir}', shell=True)
# this is a new store
newstore = True

# loop on years
for year in range(firstyear, lastyear+1):
    ctarfile = f'{year}0101.nc.tar'
    check = sp.check_call(f'dmget -v -d {historydir} {ctarfile}', shell=True)
    history2CMIParchive.convert_archive_to_zarr_store(historydir + ctarfile,
                                                      outputdir, tmpdir,
                                                      newstore=newstore)
    # after first iteration, no longer a new store
    # switch to append mode
    newstore = False
    # clean up this year
    check = sp.check_call(f'rm {tmpdir}/*.nc', shell=True)

# clean up tmp directory
check = sp.check_call(f'rmdir {tmpdir}', shell=True)
