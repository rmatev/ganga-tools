import os
import re

import GangaCore
from GangaCore.GPI import (
    diracAPI, BKQuery, LHCbDataset, MassStorageFile, DiracFile)
from GangaDirac.Lib.Utilities.DiracUtilities import GangaDiracError


# Disable the info message from LHCbDataset.bkMetadata()
GangaCore.GPI.config['Logging']['GangaLHCb.Lib.LHCbDataset'] = 'WARNING'
logger = GangaCore.Utility.logging.getLogger('gutils.datasets')


STREAM_ID = {
    'FULL': ['90000000', '90000001'],
    'LUMI': ['93000000'],
    'NOBIAS': ['96000000'],
    'BEAMGAS': ['97000000', '97000001'],
    'EXPRESS': ['91000000', '91000001'],
}


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def bkAPI(cmd, timeout=120, tries=3):
    for i in range(tries):
        try:
            result = diracAPI(
                'from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient; '
                'output(BookkeepingClient().{})'.format(cmd), timeout=timeout)
            return result
        except GangaDiracError as e:
            if e.message != 'DIRAC command timed out':
                raise
    raise RuntimeError('Command timed out {} times! Increase timeout.'.format(tries))


def bkMetadata(dataset):
    """Return the metadata for a dataset."""
    if not isinstance(dataset, LHCbDataset):
        raise TypeError('Expect an LHCbDataset object')
    m = dataset.bkMetadata()
    if m['Failed']:
       print m
       raise RuntimeError("bkMetadata call failed")
    return m['Successful']


def _getRunInformation(in_dict):
    return bkAPI("getRunInformation({})".format(repr(in_dict)))


def get_raw_dataset_runs(runs, streams, warn=True):
    if not runs:
        return LHCbDataset()
    if streams[0] in STREAM_ID:  # expand stream name to numeric id(s)
        streams = STREAM_ID[streams[0]] + streams[1:]

    log = logger.warning if warn else logger.info

    m = {}
    ds = LHCbDataset()
    for subruns in chunks(runs, 20):
        path = '/{}-{}/Real Data/{}/RAW'.format(min(subruns), max(subruns), streams[0])
        logger.info('BK query: {}'.format(path))
        query = BKQuery(dqflag='All', type='Run', path=path)
        ds.files += query.getDataset().files
        m.update(bkMetadata(ds))

    # Select only the files corresponding to requested runs
    ds.files = filter(lambda f: m[f.lfn]['RunNumber'] in runs, ds.files)
    # Check if there are remaining runs and if so use the next stream
    runs_out = set(x['RunNumber'] for x in m.values())
    runs_remain = list(set(runs) - runs_out)
    if runs_remain:
        if len(streams) > 1:
            logger.info('Files for run(s) {} not found in stream {}'.
                        format(runs_remain, streams[0]))
            ds_remain = get_raw_dataset_runs(runs_remain, streams[1:], warn=warn)
            ds.files = ds.files + ds_remain.files
        else:
            log('Files for run(s) {} not found in any of the streams'.format(runs_remain))
    return ds


def get_raw_dataset_fill(fill, streams, destinations=None, runtypes=None):
    def dest(runinfo):
        return set('OFFLINE' if s['EVENTTYPEID'] % 2 == 0 else 'CASTOR'
                   for s in runinfo['Statistics'])

    runs = sorted(bkAPI('getRunsForFill({})'.format(fill)))
    if destinations or runtypes:
        runinfos = _getRunInformation({
            "RunNumber": runs, "Fields": ["ConfigVersion"], "Statistics": ["EVENTTYPEID"]})
    if destinations:
        destinations = set(map(str.upper, destinations))
        runs = [run for run in runs if dest(runinfos[run]) & destinations]
    if runtypes:
        runtypes = map(str.upper, runtypes)
        runs = [run for run in runs if runinfos[run]['ConfigVersion'].upper() in runtypes]
    if not runs:
        return None
    return get_raw_dataset_runs(runs, streams, warn=(('FULL' not in streams) and
                                                     ('90000000' not in streams)))


def get_raw_dataset(inp, streams, **kwargs):
    """Return a dataset (list of files) for a run/fill."""
    if inp.isdigit() and int(inp) > 50000:  # this is a run
        return 'run', get_raw_dataset_runs([int(inp)], streams, **kwargs)
    elif inp.isdigit() and int(inp) < 50000:  # this is a fill
        return 'fill', get_raw_dataset_fill(int(inp), streams, **kwargs)
    else:
        raise ValueError('Unknown input value!')
