import os
import re
import logging
import Ganga
import GangaDirac
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.GPIDev.Lib.Job.Job import Job
from Ganga.GPIDev.Lib.File.IGangaFile import IGangaFile

logger = Ganga.Utility.logging.getLogger('gutils.utils')

def subjobs(jobs):
    """Return an interator on the (flattened) subjobs."""
    if isinstance(jobs, GPIProxyObject) and isinstance(jobs._impl, Job):
        if len(jobs.subjobs):
            for j in jobs.subjobs: yield j
        else:
            yield jobs
    else:
        for job in jobs:
            for j in subjobs(job):
                yield j


def is_existing_file(file, job=None):
    """Is a an existing physical file is represented by a Ganga file object?"""

    if not isinstance(file, GPIProxyObject) or not isinstance(file._impl, IGangaFile):
        raise ValueError('file must be a Ganga file object!')

    if isinstance(file._impl, GangaDirac.Lib.Files.DiracFile):
        ok = bool(file.lfn)
    elif isinstance(file._impl, Ganga.GPIDev.Lib.File.MassStorageFile):
        ok = bool(file.location())
    elif isinstance(file._impl, Ganga.GPIDev.Lib.File.LocalFile):
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
    """
    jobs = []
    for spec in specs:
        m = re.match(r"^(?P<int>^-?\d+)$|^(?P<float>^-?\d+\.\d+)$|^(?P<range1>\d+):(?P<range2>\d+)$", spec)
        if not m:
            raise ValueError("Unsupported spec '{}'".format(spec))
        if m.group('int'):
            sjobs = subjobs(Ganga.GPI.jobs(abs(int(m.group('int')))))
            remove = m.group('int')[0] == '-'
        elif m.group('float'):
            sjobs = [Ganga.GPI.jobs(str(abs(float(m.group('float')))))]
            remove = m.group('float')[0] == '-'
        elif m.group('range1'):
            sjobs = Ganga.GPI.jobs.select(int(m.group('range1')), int(m.group('range2')) + 1)
            remove = False
        else:
            assert False
        if not remove:
            jobs += filter(lambda j: j not in jobs, sjobs)  # add only unique
        else:
            jobs = filter(lambda j: j not in sjobs, jobs)  # keep only non-removed

    return jobs


def master_id(job):
    """Return the master id of a (sub)job."""
    return job.master.id if job.master else job.id


def recheck(jobs, only_failed=True):
    """Re-check (only failed) subjobs"""
    for job in subjobs(jobs):
        if job.status == 'failed' or (not only_failed and job.status == 'completed'):
            # TODO this way of rerunning checkers may not work in the future
            # see GANGA-1984
            job.force_status('completed')


def resubmit(jobs, only_failed=True):
    """Resubmit (only failed) subjobs"""
    for job in subjobs(jobs):
        if not only_failed or job.status == 'failed':
            job.resubmit()

def runtimes(jobs):
    """Return list of runtimes of finished jobs (in seconds)"""
    return [job.time.runtime().total_seconds() for job in subjobs(jobs) if job.status == 'completed']
