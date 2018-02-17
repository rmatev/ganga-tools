import os
import shutil
import tempfile
from collections import defaultdict
import Ganga
from utils import subjobs, outputfiles
from download import download_files, get_access_urls
from root_utils import get_tree_enties

logger = Ganga.Utility.logging.getLogger('gutils.merge')


def _merge_root(inputs, output):
    config = Ganga.GPI.config
    old_ver = config['ROOT']['version']
    config['ROOT']['version'] = '6.02.05'  # corresponds to DaVinci v36r5

    # rootMerger = RootMerger(args='-f6')
    # -O gives the best reading performance:
    rootMerger = Ganga.GPI.RootMerger(args='-O')
    rootMerger._impl.mergefiles(inputs, output)

    config['ROOT']['version'] = old_ver

    assert get_tree_enties(inputs) == get_tree_enties(output)


def _prepare_merge(jobs, name, path, overwrite=False, partial=False):
    if any(x in name for x in ['*', '?', '[', ']']):
        raise ValueError('Wildcard characters in name not supported.')

    files = outputfiles(jobs, name, one_per_job=True)

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

    return (files, path)


def download_merge(jobs, name, path, parallel=True, keep_temp=False, **kwargs):
    files, path = _prepare_merge(jobs, name, path, **kwargs)

    tempdir = tempfile.mkdtemp(prefix='merge-{}-'.format(name))
    filenames = download_files(files, tempdir, parallel)
    if not filenames:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    _merge_root(filenames, path)

    if not keep_temp:
        shutil.rmtree(tempdir)
    return path


def direct_merge(jobs, name, path, **kwargs):
    files, path = _prepare_merge(jobs, name, path, **kwargs)

    if not files:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    urls = get_access_urls(files)
    _merge_root(urls, path)
    return path
