import os
import re
import functools
import logging

import GangaDirac
import GangaCore
from GangaCore.GPIDev.Base.Proxy import GPIProxyObject
from GangaCore.GPIDev.Lib.Job.Job import Job
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
logger = GangaCore.Utility.logging.getLogger('gutils.utils')


def ganga_type(x):
    """Return the type of the underlying object or just type(x) if not a proxy."""
    return type(x._impl) if isinstance(x, GPIProxyObject) else type(x)


def subjobs(jobs):
    """Return an interator on the (flattened) subjobs."""
    if (issubclass(ganga_type(jobs), Job)):
        if len(jobs.subjobs):
            for j in jobs.subjobs: yield j
        else:
            yield jobs
    else:
        for job in jobs:
            for j in subjobs(job):
                yield j


def is_existing_file(file, job=None):
    """Does the Ganga file object represent anexisting physical file?"""

    file_type = ganga_type(file)
    if not issubclass(file_type, IGangaFile):
        raise ValueError('file must be a Ganga file object!')

    if issubclass(file_type, GangaDirac.Lib.Files.DiracFile.DiracFile):
        ok = bool(file.lfn)
    elif issubclass(file_type, GangaCore.GPIDev.Lib.File.MassStorageFile):
        ok = bool(file.location())
    elif issubclass(file_type, GangaCore.GPIDev.Lib.File.LocalFile):
        # Ganga 6.0.44 does not set localDir of LocalFile thus no way
        # to check if file object is a placeholder or refers to a real file
        # TODO file a bug report

        # if job object is given, figure out location, otherwise return True
        ok = os.path.isfile(os.path.join(job.outputdir, file.namePattern)) if job else True
    else:
        raise NotImplementedError('Do not know how to check if file exits {}'.format(repr(file)))

    return ok


def outputfiles(jobs, pattern, one_per_job=False, ignore_missing=True):
    """Return a flat list of outputfiles matching the pattern for the given jobs."""
    files = []
    for job in subjobs(jobs):
        job_files = job.outputfiles.get(pattern)
        if one_per_job:
            if len(job_files) == 0:
                raise RuntimeError('File "{}" not found for job {}'.format(pattern, job.fqid))
            elif len(job_files) > 1:
                raise RuntimeError('Too many files matching pattern "{}" for job {}'.format(pattern, job.fqid))
        for file in job_files:
            # Here we need to check that the file exists. Unfortunatelly, Ganga does
            # not provide an abstract method of IGangaFile to do that, hence the "hack".
            if not is_existing_file(file, job):
                msg = 'File {} from job {} ({}) does not represent an existing physical file!'.format(repr(file), job.fqid, job.status)
                if not ignore_missing:
                    raise RuntimeError(msg + '\nAre all (sub)jobs completed?')
                else:
                    level = logging.INFO if job.status == 'completed' else logging.WARNING
                    logger.log(level, msg + ' Will ignore it!')
            else:
                if job.status != 'completed':
                    logger.warning('File {} from job {} exists but job is {}!'.format(repr(file), job.fqid, job.status))
                files.append((job, file))
    return files


def smart_jobs_select(specs):
    """Return list of (sub)jobs from a list of string job specifiers.

    Examples:
        smart_jobs_select(['100', '-100.1', '105:110', '-108'])
        smart_jobs_select(['100.0', '100.1', '100.10'])
    """
    jobs = []
    for spec in specs:
        m = re.match(r"^(?P<remove>-)?((?P<master>\d+)|(?P<subjob>\d+\.\d+)|(?P<range1>\d+):(?P<range2>\d+))$", spec)
        if not m:
            raise ValueError("Unsupported spec '{}'".format(spec))
        if m.group('master'):
            sjobs = list(subjobs(GangaCore.GPI.jobs(m.group('master'))))
        elif m.group('subjob'):
            sjobs = [GangaCore.GPI.jobs(m.group('subjob'))]
        elif m.group('range1'):
            sjobs = list(subjobs(GangaCore.GPI.jobs.select(int(m.group('range1')), int(m.group('range2')))))
        else:
            assert False
        if not m.group('remove'):
            jobs += [j for j in sjobs if j not in jobs]  # add only unique
        else:
            jobs = [j for j in jobs if j not in sjobs]  # keep only non-removed

    return jobs


def master_id(job):
    """Return the master id of a (sub)job."""
    return job.master.id if job.master else job.id


def recheck(jobs, only_failed=True):
    """Re-check (only failed) subjobs"""
    # TODO this way of rerunning checkers may not work in the future
    for job in subjobs(jobs):
        if job.status == 'failed' or (not only_failed and job.status == 'completed'):
            logger.info('Re-checking job {}'.format(job.fqid))
            if job.status == 'completed': job.force_status('failed')
            job.force_status('completed')


def resubmit(jobs, only_failed=True):
    """Resubmit (only failed) subjobs"""
    for job in subjobs(jobs):
        if not only_failed or job.status == 'failed':
            job.resubmit()


def remove(jobs):
    """Remove jobs and their data."""
    for job in jobs:
        if job.master:
            raise ValueError('remove cannot take subjobs')
        job.backend.removeOutputData()
        job.remove()


def runtimes(jobs):
    """Return list of runtimes of finished jobs (in seconds)"""
    return [job.time.runtime().total_seconds() for job in subjobs(jobs) if job.status == 'completed']


def status(j):
    """Return an overview of how many subjobs are in what status"""
    for stat in ["new","submitting","submitted","running","failed","completing","completed"]:
        l = len(j.subjobs.select(status=stat))
        if l > 0:
            print(stat+":", l)


def memoize(obj):
    """Simple memoization decorator."""
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer
