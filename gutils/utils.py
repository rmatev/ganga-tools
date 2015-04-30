import Ganga

def subjobs(jobs):
    """Return an interator on the (flattened) subjobs."""
    if isinstance(jobs, Ganga.GPIDev.Base.Proxy.GPIProxyObject) and isinstance(jobs._impl, Ganga.GPIDev.Lib.Job.Job):
        if len(jobs.subjobs):
            for j in jobs.subjobs: yield j
        else:
            yield jobs
    else:
        for job in jobs:
            for j in subjobs(job):
                yield j

def outputfiles(jobs, pattern, one_per_job=False):
    files = []
    for job in subjobs(jobs):
        job_files = job.outputfiles.get(pattern)
        if one_per_job:
            if len(job_files) == 0:
                raise Exception('File "{}" not found in job {}'.format(pattern, job.fqid))
            elif len(job_files) > 1:
                raise Exception('Too many files matching pattern "{}" for job {}'.format(pattern, job.fqid))
        for file in job_files:
            # if not file.lfn:
            #     print 'Warning: No LFN for file "{}" of job {}'.format(file.namePattern, job.fqid)
            #     continue
            files.append((job, file))
    return files
