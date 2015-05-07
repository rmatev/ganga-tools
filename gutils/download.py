import os
import time
import tempfile
from collections import defaultdict
import Ganga
from utils import outputfiles


def get_file(file, path):
    if any(x in file.namePattern for x in ['*', '?', '[', ']']):
        raise ValueError('namePattern contains wildcard characterss.')
    if os.path.isdir(path):
        file.localDir = path
        file.get()
        fn = os.path.join(path, file.namePattern)
    else:
        localdir, basename = os.path.split(path)
        if not os.path.isdir(localdir):
            raise ValueError('Directory "{}" does not exist.'.format(localdir))
        file.localDir = tempfile.mkdtemp()
        file.get()
        os.rename(os.path.join(file.localDir, file.namePattern), path)
        os.rmdir(file.localDir)
        file.localDir = None  # or restore to original value?
        fn = path
    return fn


def download_files(files, path, parallel=True, block=True):
    if not os.path.isdir(path):
        raise ValueError('Path must be existing directory.')

    filenames = []
    for job,file in files:
        root, ext = os.path.splitext(file.namePattern)
        fn = os.path.join(path, '{}-{}{}'.format(root, job.fqid, ext))
        filenames.append(fn)
        if parallel:
            Ganga.GPI.queues.add(get_file, args=(file, fn))
        else:
            get_file(file, fn)

    if parallel and block:
        while Ganga.GPI.queues.totalNumUserThreads(): time.sleep(2)

    return filenames


def download(jobs, name, path, parallel=True, ignore_missing=False):
    if any(x in name for x in ['*', '?', '[', ']']):
        raise ValueError('Wildcard characters in name not supported.')
    files = outputfiles(jobs, name, one_per_job=True, ignore_missing=ignore_missing)
    download_files(files, path, parallel)


def dirac_get_access_urls(lfns):
    if not lfns: return {}

    from GangaDirac.Lib.Utilities.DiracUtilities import execute
    #opts = '--Protocol root,xroot'
    opts = ''
    cmd = 'dirac-dms-lfn-accessURL {} {}'.format(','.join(lfns), opts)
    output = execute(cmd, shell=True)
    urls = {}
    for line in output.splitlines():
        items = [x.strip() for x in line.split(':', 1)]
        if len(items) < 2: continue
        k,v = items
        if k not in lfns or 'file not found' in v.lower():
            continue
        if k not in urls: urls[k] = v
    return urls


def get_access_urls(files):
    lfns = [f.lfn for job,f in files]
    urls_dict = dirac_get_access_urls(lfns)
    return [urls_dict[lfn] for lfn in lfns]
