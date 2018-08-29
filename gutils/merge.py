import os
import shutil

try: # for Ganga >= v7.0.0
    import GangaCore
except ImportError:
    import Ganga as GangaCore

logger = GangaCore.Utility.logging.getLogger('gutils.merge')

from utils import subjobs, outputfiles
from download import download_temp, get_access_urls
from root_utils import get_tree_enties


def _getrootprefix_patch(rootsys=None):
    return 0, 'lb-run ROOT '


def _merge_root(inputs, output):
    config = GangaCore.GPI.config
    GangaCore.Utility.root.getrootprefix = _getrootprefix_patch

    # rootMerger = RootMerger(args='-f6')
    # -O gives the best reading performance:
    rootMerger = GangaCore.GPI.RootMerger(args='-O')
    rootMerger._impl.mergefiles(inputs, output)

    n_in, n_out = get_tree_enties(inputs), get_tree_enties(output)
    if n_in != n_out:
        logger.error("Got {} input entries but merged file contains {}!"
                     .format(n_in, n_out))


def _merge_mdf(inputs, output):
    if any(x.startswith('root://') for x in inputs):
        raise NotImplementedError('Direct merging of MDF files not implemented.')
    with open(output, 'wb') as fout:
        for inp in inputs:
            with open(inp, 'rb') as fin:
                shutil.copyfileobj(fin, fout)


def _merge(inputs, output):
    ext = os.path.splitext(output)[1]
    if not all(os.path.splitext(x)[1] == ext for x in inputs):
        raise ValueError("Incompatible extensions of inputs ({}) and output "
                         "({}).".format(inputs, output))
    if ext == '.root':
        _merge_root(inputs, output)
    elif ext == '.mdf' or ext == '.raw':
        _merge_mdf(inputs, output)
    else:
        raise ValueError("Do not know how to merge {} files.".format(ext))


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
        _merge(filenames, path)
    return path


def direct_merge(jobs, name, path, **kwargs):
    path = _merged_path(jobs, name, path, **kwargs)
    files = outputfiles(jobs, name, one_per_job=True)

    if not files:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    urls = get_access_urls(files)
    _merge(urls, path)
    return path
