import os
import shutil
import tempfile
from collections import defaultdict
import Ganga
from utils import subjobs, outputfiles
from download import download_files, get_access_urls

logger = Ganga.Utility.logging.getLogger('gutils.merge')

def _get_trees(x,dir_name=""):
    """Recursively get trees from x.
       x can be a TFile or a TDirectoryFile
       Returns a set of tuples: set( (tree_name, tree_object) )
       Prepends the name of the object with dir_name (dir_name should include a trailing /)
    """
    trees = set()
    keys = set(key.GetName() for key in x.GetListOfKeys())
    for key in keys:
        obj = x.Get(key)
        class_name = obj.IsA().GetName()
        if class_name == "TTree":
            trees.add((dir_name+obj.GetName(),obj))
        elif class_name == "TDirectoryFile":
            trees = trees.union(_get_trees(obj,dir_name+obj.GetName()+"/"))
    return trees

def _get_entries(files,ignore_empty=False,ignore_missing=False):
    """Get number of entries of all trees in files
       Returns a dictionary: {"tree_name":tree_entries}
       tree_name includes the directory name(s), if applicable
    """
    from ROOT import TFile
    from collections import defaultdict
    if not hasattr(files,"__iter__"): #allow single filename to be passed
        files = [files]
    entries = defaultdict(int)
    for f in files:
        file0 = TFile.Open(f)
        if not file0:
            if ignore_missing:
                print "Warning: Can't find/open file: "+f
                continue
            raise IOError("Can't find/open file: "+f)

        trees = _get_trees(file0)
        if not trees:
            if ignore_empty:
                print 'Warning: No TTree objects found in '+f
                continue
            raise ValueError('No TTree objects found in '+f)

        for name, tree in trees:
            entries[name] += tree.GetEntries()
        file0.Close("R")
    return entries

def _merge_root(inputs, output):
    config = Ganga.GPI.config
    old_ver = config['ROOT']['version']
    config['ROOT']['version'] = '6.02.05'  # corresponds to DaVinci v36r5

    #rootMerger = RootMerger(args='-f6')
    rootMerger = Ganga.GPI.RootMerger(args='-O') # -O gives the best reading performance
    rootMerger._impl.mergefiles(inputs, output)

    config['ROOT']['version'] = old_ver

    assert _get_entries(inputs) == _get_entries(output)


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

    if not keep_temp: shutil.rmtree(tempdir)


def direct_merge(jobs, name, path, **kwargs):
    files, path = _prepare_merge(jobs, name, path, **kwargs)

    if not files:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    urls = get_access_urls(files)
    _merge_root(urls, path)
    return path
