import os
import argparse
import tempfile
from gutils.utils import smart_jobs_select
from gutils.download import download
try: # for Ganga >= v7.0.0
    import GangaCore #to possibly raise the exception
    logger = GangaCore.Utility.logging.getLogger('gdownload')
except ImportError:
    logger = Ganga.Utility.logging.getLogger('gdownload')


parser = argparse.ArgumentParser(description='Download outputfiles')
parser.add_argument('jobs', nargs='+', help='Job IDs')
parser.add_argument('--name', '-n', required=True, help='Name of job output file in job.outputfiles')
parser.add_argument('--output', '-o', default=tempfile.gettempdir(), help='Where to put the downloaded files? Defaults to $TMPDIR')
parser.add_argument('--overwrite', action='store_true', help='Overwrite existing output file')
args = parser.parse_args()

if not os.path.isdir(args.output):
    parser.error('Output (--output) must be an existing directory!')

for specs in args.jobs:
    jobs = smart_jobs_select(specs.split(','))

    unique_names = list(set(j.name for j in jobs))
    print 'Downloading files for job(s)', unique_names

    # if only one job is given, download in a directory named after the job
    if len(unique_names) == 1:
        path = os.path.join(args.output, unique_names[0])
        os.mkdir(path)
        logger.info('Downloading files for job(s) {} named {}'.format(specs, unique_names[0]))
    else:
        path = args.output
        logger.warning('Downloading jobs with multiple names ({}).'.format(unique_names))

    download(jobs, args.name, path)

    logger.info('Your downloads are at {}'.format(path))
