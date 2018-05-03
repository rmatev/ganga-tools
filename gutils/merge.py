import os
try: # for Ganga >= v7.0.0
    import GangaCore
except ImportError:
    import Ganga as GangaCore

logger = GangaCore.Utility.logging.getLogger('gutils.merge')

from utils import subjobs, outputfiles
from download import download_temp, get_access_urls
from root_utils import get_tree_enties

def _merge_root(inputs, output):
    config = GangaCore.GPI.config
    old_ver = config['ROOT']['version']
    config['ROOT']['version'] = '6.02.05'  # corresponds to DaVinci v36r5

    # rootMerger = RootMerger(args='-f6')
    # -O gives the best reading performance:
    rootMerger = GangaCore.GPI.RootMerger(args='-O')
    rootMerger._impl.mergefiles(inputs, output)

    config['ROOT']['version'] = old_ver

    assert get_tree_enties(inputs) == get_tree_enties(output)


def _merged_path(jobs, name, path, overwrite=False, partial=False):
    if any(x in name for x in ['*', '?', '[', ']']):
        raise ValueError('Wildcard characters in name not supported.')

    jobnames = set(j.name for j in jobs)
    if len(jobnames) != 1:
        raise ValueError('Not all jobs have the same name ({}).'.format(jobnames))
    jobname = list(jobnames)[0]

    default_filename = jobname + ('-partial' if partial else '') + os.path.splitext(name)[1]
    path = os.path.abspath(path)
    if os.path.isdir(path):
        path = os.path.join(path, default_filename)
    if os.path.isfile(path):
        if not overwrite:
            raise ValueError('File "{}" already exists.'.format(path))

    return path


def download_merge(jobs, name, path, parallel=True, keep_temp=False, overwrite=False, partial=False):
    path = _merged_path(jobs, name, path, overwrite=overwrite, partial=partial)
    with download_temp(jobs, name, parallel=parallel, keep_temp=keep_temp) as filenames:
        _merge_root(filenames, path)
    return path


def direct_merge(jobs, name, path, **kwargs):
    path = _merged_path(jobs, name, path, **kwargs)
    files = outputfiles(jobs, name, one_per_job=True)

    if not files:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    urls = get_access_urls(files)
    _merge_root(urls, path)
    return path
