import yaml
import os


def create_build_history(historydir, storedir, storetype, consolidated,
                         timedim, chunks, grid, tag, domain, site, varname,
                         files):
    """ create a dictionary describing the zarr store build process
    that can be used to rebuild the store """

    path = {'history': historydir, 'store': storedir}
    options = {'storetype': storetype, 'consolidated': consolidated,
               'timedim': timedim, 'chunks': chunks, 'grid': grid,
               'tag': tag, 'domain': domain, 'site': site}

    build = {'path': path, 'options': options, 'varname': varname,
             'files': files}

    return build


def update_yaml(storepath, varname, update_history, overwrite=False):
    """ create/update history yaml for zarr store

    storepath: str
        path for zarr store
    varname: str
        variable in zarr store
    updatehistory: dict
        dictionary of updated history

    """

    # the yaml file contains the history of how the store was built
    ymlhistory = f'{storepath}/{varname}.yml'

    if os.path.exists(ymlhistory) and not overwrite:
        with open(ymlhistory) as f:
            store_history = yaml.load(f, Loader=yaml.FullLoader)
            f.close()

        # this is not supposed to change
        assert store_history['path'] == update_history['path']
        assert store_history['options'] == update_history['options']
        assert store_history['varname'] == update_history['varname']

        # this is the sensitive part: if the content of files is not
        # already in the store history, then we need to update. Otherwise
        # we are most likely trying to rebuild and we want to keep the
        # history intact!

        for kv in update_history['files']:
            if kv not in store_history['files']:
                store_history['files'].append(kv)
    else:
        # first dump/overwrite
        store_history = update_history
    # write to yml file
    with open(ymlhistory, 'w') as fnew:
        yaml.dump(store_history, fnew, default_flow_style=False)

    return None
