import os
import time
import tempfile
from collections import defaultdict
import Ganga
import GangaDirac
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
try:
    from Ganga.GPIDev.Lib.File.IGangaFile import IGangaFile  # for Ganga <= v6.1.14
except ImportError:
    from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile  # for Ganga >= v6.1.16
from utils import ganga_type, outputfiles

logger = Ganga.Utility.logging.getLogger('gutils.download')


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


def download(jobs, name, path, parallel=True, ignore_missing=True):
    if any(x in name for x in ['*', '?', '[', ']']):
        raise ValueError('Wildcard characters in name not supported.')
    files = outputfiles(jobs, name, one_per_job=True, ignore_missing=ignore_missing)
    download_files(files, path, parallel)


def dirac_get_access_urls(lfns):
    if isinstance(lfns, basestring):
        lfns = [lfns]
    if not lfns:
        return {}
    print "getting dirac access urls for:"
    print lfns
    print "Using Dirac version {}".foramt(os.environ['GANGADIRACENVIRONMENT'].split(os.sep)[-1])

    from GangaDirac.Lib.Utilities.DiracUtilities import execute
    opts = '--Protocol root,xroot'
    cmd = 'dirac-dms-lfn-accessURL {} {}'.format(','.join(lfns), opts)
    output = execute(cmd, shell=True)
    urls = {}
    for line in output.splitlines():
        items = [x.strip() for x in line.split(':', 1)]
        if len(items) < 2 or items[0][0] != '/':
            continue
        k, v = items
        if k not in lfns:
            logger.warning('Unexpected key (LFN) in output of dirac-dms-lfn-accessURL: ' + k)
        elif 'file not found' in v.lower():
            logger.error('File not found in the bookkeeping: ' + k)
        elif k not in urls:
            urls[k] = v
    return urls


def get_access_urls(files):
    urls = [None] * len(files)
    for i, (job, f) in enumerate(files):
        file_type = ganga_type(f)
        if not issubclass(file_type, IGangaFile):
            raise ValueError('file must be a Ganga file object!')

        if issubclass(file_type, GangaDirac.Lib.Files.DiracFile):
            pass  # deal with this case separately, see below
        elif issubclass(file_type, Ganga.GPIDev.Lib.File.MassStorageFile):
            # TODO this is LHCb specific, but there is no generic easy way
            urls[i] = 'root://eoslhcb.cern.ch/' + f.location()[0]
        elif issubclass(file_type, Ganga.GPIDev.Lib.File.LocalFile):
            urls[i] = os.path.join(job.outputdir, f.namePattern)
        else:
            raise NotImplementedError('get_access_url() does not yet implement {}'.format(repr(f)))

    # Collect all DiracFile(s) to make a single call to the Dirac API (calls are slow!)
    dirac_lfns = [f.lfn for job, f in files if issubclass(ganga_type(f), GangaDirac.Lib.Files.DiracFile)]
    dirac_urls_dict = dirac_get_access_urls(dirac_lfns)
    for i, (job, f) in enumerate(files):
        if issubclass(ganga_type(f), GangaDirac.Lib.Files.DiracFile):
            try:
                urls[i] = dirac_urls_dict[f.lfn]
            except KeyError:
                logger.error('No available replica for LFN {} from job {}'.format(job.fqid))
                raise RuntimeError('Cannot handle unaccessible files.')
    return urls
