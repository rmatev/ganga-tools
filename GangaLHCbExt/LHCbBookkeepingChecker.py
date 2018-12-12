from GangaCore.Utility.logging import getLogger, _set_log_level
from GangaCore.GPIDev.Adapters.IChecker import IChecker
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem

logger = getLogger()

def bkMetadataStat(md):
    return md['FullStat'] if md['FileType'] == 'RAW' else md['EventStat']

class LHCbBookkeepingChecker(IChecker):
    """
    Compares the number of processed events (metadata) and the number of input events.
    """
    _schema = IChecker._schema.inherit_copy()
    _schema.datadict['maxAbsDiff'] = SimpleItem(defvalue=0, doc='Maximum absolute difference')
    _schema.datadict['maxRelDiff'] = SimpleItem(defvalue=0.0, doc='Maximum relative difference')
    _category = 'postprocessor'
    _name = 'LHCbBookkeepingChecker'
    _exportmethods = ['check']

    def check(self, job):
        from GangaLHCb.Lib.LHCbDataset.LHCbDataset import logger as ds_logger
        old = ds_logger.level
        _set_log_level(ds_logger, 'WARNING')
        bkmd = job.inputdata.bkMetadata()
        ds_logger.setLevel(old)

        if bkmd['Failed']:
            logger.warning('Could not get the bookeeping metadata.')
            return self.failure
        n_expected = sum(bkMetadataStat(v) for v in bkmd['Successful'].values())

        try:
            n_processed = job.metadata['events']['input']
        except KeyError:
            #raise PostProcessException("The metadata value ['events']['input'] was not defined")
            logger.warning("The metadata value ['events']['input'] was not defined")
            return self.failure

        if n_processed != n_expected:
            diff = n_expected - n_processed
            reldiff = float(diff) / float(n_expected)
            logger.info('Job {}: Number of processed events ({}) differs from the '
                        'expected number of input events ({}) by {}'
                        .format(job.fqid, n_processed, n_expected, diff))
            if self.maxAbsDiff >= 0 and diff > self.maxAbsDiff:
                logger.warning('Job {}: Absolute difference ({}) is more than the maximum allowed ({})'
                               .format(job.fqid, diff, self.maxAbsDiff))
            elif self.maxRelDiff >= 0.0 and reldiff > self.maxRelDiff:
                logger.warning('Job {}: Relative difference ({}) is more than the maximum allowed ({})'
                               .format(job.fqid, reldiff, self.maxRelDiff))
            else:
                return self.success
            return self.failure

        return self.success
