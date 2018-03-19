import os
import shutil
import time
import tempfile
import GangaDirac
try: # for Ganga >= v7.0.0
    import GangaCore
    from GangaCore.GPIDev.Base.Proxy import GPIProxyObject
    from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
    logger = GangaCore.Utility.logging.getLogger('gutils.download')
except ImportError: 
    import Ganga
    from Ganga.GPIDev.Base.Proxy import GPIProxyObject
    try:
        from Ganga.GPIDev.Lib.File.IGangaFile import IGangaFile  # for Ganga <= v6.1.14
    except ImportError:
        from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile  # for Ganga >= v6.1.16
    logger = Ganga.Utility.logging.getLogger('gutils.download')

from utils import ganga_type, outputfiles




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
    for job, file in files:
        root, ext = os.path.splitext(file.namePattern)
        fn = os.path.join(path, '{}-{}{}'.format(root, job.fqid, ext))
        filenames.append((file, fn))
        if parallel:
            Ganga.GPI.queues.add(get_file, args=(file, fn))
        else:
            get_file(file, fn)

    if parallel and block:
        while Ganga.GPI.queues.totalNumUserThreads():
            time.sleep(2)

    downloaded = []
    for src, fn in filenames:
        if os.path.isfile(fn):
            downloaded.append(fn)
        else:
            logger.warning('File {!r} could not be downloaded'.format(src))
    # if len(downloaded) < len(filenames):
    #     raise RuntimeError('Not all files could be downloaded')
    return downloaded


def download(jobs, name, path, ignore_missing=True, **kwargs):
    if any(x in name for x in ['*', '?', '[', ']']):
        raise ValueError('Wildcard characters in name not supported.')
    files = outputfiles(jobs, name, one_per_job=True, ignore_missing=ignore_missing)
    return download_files(files, path, **kwargs)


def download_temp(jobs, name, ignore_missing=True, keep_temp=False, **kwargs):
    tempdir = tempfile.mkdtemp(prefix='download_temp-{}-'.format(name))
    filenames = download(jobs, name, tempdir, ignore_missing=ignore_missing, **kwargs)
    if not filenames:
        raise RuntimeError('No files found for given job(s). Check the name pattern.')
    class TempFileList(list):
        def __enter__(self):
            return self
        def __exit__(self, type, value, traceback):
            if not keep_temp:
                shutil.rmtree(tempdir)
    return TempFileList(filenames)


def dirac_get_access_urls(lfns):
    if isinstance(lfns, basestring):
        lfns = [lfns]
    if not lfns:
        return {}

    opts = '--Protocol xroot,root'
    cmd = 'dirac-dms-lfn-accessURL {} {}'.format(','.join(lfns), opts)
    # from GangaDirac.Lib.Utilities.DiracUtilities import execute
    # output = execute(cmd, shell=True)
    import subprocess
    output = subprocess.check_output(['lb-run', 'LHCbDirac'] + cmd.split())
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
    dirac_lfns = []
    for i, (job, f) in enumerate(files):
        file_type = ganga_type(f)
        if not issubclass(file_type, IGangaFile):
            raise ValueError('file must be a Ganga file object!')

        if issubclass(file_type, GangaDirac.Lib.Files.DiracFile.DiracFile):
            dirac_lfns.append(f.lfn)  # deal with this case separately below
        elif issubclass(file_type, Ganga.GPIDev.Lib.File.MassStorageFile):
            # TODO this is LHCb specific, but there is no generic easy way
            urls[i] = 'root://eoslhcb.cern.ch/' + f.location()[0]
        elif issubclass(file_type, Ganga.GPIDev.Lib.File.LocalFile):
            urls[i] = os.path.join(job.outputdir, f.namePattern)
        else:
            raise NotImplementedError('get_access_url() does not yet implement {}'.format(repr(f)))

    # Make a single call to the Dirac API for all DiracFiles (calls are slow!)
    if(len(dirac_lfns) > 0):
        dirac_urls_dict = dirac_get_access_urls(dirac_lfns)
        for i, (job, f) in enumerate(files):
            if issubclass(ganga_type(f), GangaDirac.Lib.Files.DiracFile.DiracFile):
                try:
                    urls[i] = dirac_urls_dict[f.lfn]
                except KeyError:
                    logger.error('No available replica for LFN {} from job {}'.format(job.fqid))
                    raise RuntimeError('Cannot handle unaccessible files.')
    return urls
