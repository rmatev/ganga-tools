import os
import shutil
import tempfile
from collections import defaultdict
import Ganga
from utils import outputfiles
from download import download_files, get_access_urls


def _merge_root(inputs, output):
    config = Ganga.GPI.config
    old_ver = config['ROOT']['version']
    config['ROOT']['version'] = '6.02.05'  # corresponds to DaVinci v36r5

    #rootMerger = RootMerger(args='-f6')
    rootMerger = Ganga.GPI.RootMerger(args='-O') # -O gives the best reading performance
    rootMerger._impl.mergefiles(inputs, output)

    config['ROOT']['version'] = old_ver


def _prepare_merge(jobs, name, path, overwrite):
    if any(x in name for x in ['*', '?', '[', ']']):
        raise ValueError('Wildcard characters in name not supported.')

    jobnames = set(j.name for j in jobs)
    if len(jobnames) != 1:
        raise ValueError('Not all jobs have the same name ({}).'.format(jobnames))
    jobname = list(jobnames)[0]

    default_filename = jobname + os.path.splitext(name)[1]
    path = os.path.abspath(path)
    if os.path.isdir(path):
        path = os.path.join(path, default_filename)
    if os.path.isfile(path):
        if not overwrite:
            raise ValueError('File "{}" already exists.'.format(path))

    files = outputfiles(jobs, name, one_per_job=True)

    return (files, path)


def download_merge(jobs, name, path, overwrite=False, parallel=True, keep_temp=False):
    files, path = _prepare_merge(jobs, name, path, overwrite)

    tempdir = tempfile.mkdtemp(prefix='merge-{}-'.format(name))
    filenames = download_files(files, tempdir, parallel)
    if not filenames:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    _merge_root(filenames, path)

    if not keep_temp: shutil.rmtree(tempdir)


def direct_merge(jobs, name, path, overwrite=False):
    files, path = _prepare_merge(jobs, name, path, overwrite)

    if not files:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    urls = get_access_urls(files)
    _merge_root(urls, path)
