import os
import shutil
import tempfile
from collections import defaultdict
import Ganga
from utils import outputfiles
from download import download_files, get_access_urls
from ROOT import TFile

def _get_entries_of_trees(files):
    """Get number of entries of all trees in files"""
    if not hasattr(files,"__iter__"): #allow single tree to be passed
        files = [files]
    entries = defaultdict(int)
    for f in files:
        #open file
        file0 = TFile.Open(f)
        #get trees
        keys = set(key.GetName() for key in file0.GetListOfKeys())
        #set(file0.Get(key) for key in keys if file0.Get(key).IsA().GetName() == 'TTree')
        #should be faster, because one less Get():
        trees = set()
        for key in keys:
            obj = file0.Get(key)
            if obj.IsA().GetName() == 'TTree':
                trees.add(obj)
        #get entries
        for tree in trees:
            entries[tree.GetName()] += tree.GetEntries() #could use key again here, but set is not ordered
    return entries

def _merge_root(inputs, output):
    config = Ganga.GPI.config
    old_ver = config['ROOT']['version']
    config['ROOT']['version'] = '6.02.05'  # corresponds to DaVinci v36r5

    #rootMerger = RootMerger(args='-f6')
    rootMerger = Ganga.GPI.RootMerger(args='-O') # -O gives the best reading performance
    rootMerger._impl.mergefiles(inputs, output)

    config['ROOT']['version'] = old_ver


def _prepare_merge(jobs, name, path, overwrite=False, ignore_missing=False):
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

    files = outputfiles(jobs, name, one_per_job=True, ignore_missing=ignore_missing)

    return (files, path)


def download_merge(jobs, name, path, parallel=True, keep_temp=False, **kwargs):
    files, path = _prepare_merge(jobs, name, path, **kwargs)

    tempdir = tempfile.mkdtemp(prefix='merge-{}-'.format(name))
    filenames = download_files(files, tempdir, parallel)
    if not filenames:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    _merge_root(filenames, path)

    if not keep_temp: shutil.rmtree(tempdir)


def direct_merge(jobs, name, path, **kwargs):
    files, path = _prepare_merge(jobs, name, path, **kwargs)

    if not files:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    urls = get_access_urls(files)
    _merge_root(urls, path)
