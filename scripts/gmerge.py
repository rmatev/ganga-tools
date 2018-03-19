import os
import argparse
import tempfile
from gutils.utils import master_id, smart_jobs_select
from gutils.merge import direct_merge, download_merge

try: # for Ganga >= v7.0.0
    import GangaCore
    logger = GangaCore.Utility.logging.getLogger('gmerge')
except ImportError:
    import Ganga
    logger = Ganga.Utility.logging.getLogger('gmerge')

parser = argparse.ArgumentParser(description='Directly merge outputfiles')
parser.add_argument('jobs', nargs='+', help='Job IDs. One output per argument. Use comma separated values for merge accross (sub)jobs.')
parser.add_argument('--name', '-n', required=True, help='Name of job output file in job.outputfiles')
parser.add_argument('--output', '-o', default=tempfile.gettempdir(), help='Where to put the merged file? Defaults to $TMPDIR')
parser.add_argument('--overwrite', action='store_true', help='Overwrite existing output file')
parser.add_argument('--ignore-incomplete', action='store_true', help='Ignore non-completed jobs')
parser.add_argument('--download', action='store_true', help='Download files and merge locally')
args = parser.parse_args()

if not os.path.isdir(args.output):
    parser.error('Output (--output) must be an existing directory!')

for specs in args.jobs:
    jobs = smart_jobs_select(specs.split(','))

    unique_names = list(set(j.name for j in jobs))
    if len(unique_names) == 1:
        logger.info('Downloading/merging files for job(s) {} named {}'.format(specs, unique_names[0]))
    else:
        logger.warning('Downloading/merging jobs with different names ({}).'.format(unique_names))

    master_ids = list(set(master_id(j) for j in jobs))
    if len(master_ids) > 1:
        logger.warning('Downloading/merging output from more than one master job ({})'.format(master_ids))

    partial = any(j.status != 'completed' for j in jobs)
    if partial:
        if args.ignore_incomplete:
            jobs = [j for j in jobs if j.status == 'completed']
            logger.warning('Ignoring incomplete jobs! Output filename will be suffixed with "partial".')
        else:
            logger.error('There are incomplete jobs! Will not do merging!')
            break

    if not args.download:
        direct_merge(jobs, args.name, args.output, overwrite=args.overwrite, partial=partial)
    else:
        download_merge(jobs, args.name, args.output, overwrite=args.overwrite, partial=partial, keep_temp=False)

logger.info('Your merged files are at {}'.format(args.output))
