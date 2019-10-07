import subprocess as sp


def get_from_tape(mydir, myfile, site='gfdl'):
    """ wrapper around tape utilities that are site specific """
    if site == 'gfdl':
        command = f'dmget -v -d {mydir} {myfile}'
    else:
        command = ''
        pass  # if nothing else, assuming no tape system 

    check = sp.check_call(command, shell=True)
    return check
    
