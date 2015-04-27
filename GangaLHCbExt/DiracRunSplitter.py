from collections import defaultdict

from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from GangaDirac.Lib.Splitters.SplitterUtils import DiracSplitter
from Ganga.Utility.logging import getLogger

logger = getLogger()

def DiracRunSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields datasets for dirac split jobs by run
    """
    
    metadata = inputs.bkMetadata()
    if not result_ok(metadata):
        logger.error('Error getting input metadata: %s' % str(metadata))
        raise SplittingError('Error splitting files.')
    if metadata['Value']['Failed']:
        logger.error('Error getting part of metadata')
        raise SplittingError('Error splitting files.')
 
    runs = defaultdict(list)
    for lfn,v in metadata['Value']['Successful'].items():
        f = [f for f in inputs.files if f.name == lfn][0]
        runs[v['RunNumber']].append(f)
    logger.info('Found %d runs in inputdata'%len(runs))

    for run,files in sorted(runs.items()):
        run_inputs = inputs.__class__()
        run_inputs.files = files
        if len(files) > filesPerJob:
            datasets = list(DiracSplitter(run_inputs, filesPerJob, None, ignoremissing))
        else:
            datasets = [files]
        logger.info('Run %d with %d files was split in %d subjobs'%(run, len(files), len(datasets)))
        for ds in datasets:
            yield ds

