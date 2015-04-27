from Ganga.Utility.logging import getLogger, _set_log_level
from Ganga.GPIDev.Adapters.IChecker import IChecker

logger = getLogger()

class LHCbBookkeepingChecker(IChecker):
    """
    Compares the number of processed events (metadata) and the number of input events.
    """
    _schema = IChecker._schema.inherit_copy()
    _category = 'postprocessor'
    _name = 'LHCbBookkeepingChecker'
    _exportmethods = ['check']

    def check(self, job):
        t = getLogger('Ganga.GangaLHCb.Lib.LHCbDataset')
        old = t.level
        _set_log_level(t, 'WARNING')
        bkmd = job.inputdata.bkMetadata()
        t.setLevel(old)
        
        if not bkmd['OK'] or bkmd['Value']['Failed']:
            logger.warning('Could not get the bookeeping metadata.')
            return self.failure
        n_expected = sum(v['EventStat'] for v in bkmd['Value']['Successful'].values())    

        try:        
            n_processed = job.metadata['events']['input']
        except KeyError:
            #raise PostProcessException("The metadata value ['events']['input'] was not defined")
            logger.warning("The metadata value ['events']['input'] was not defined")
            return self.failure
        
        if n_processed != n_expected:
            logger.info('Number of processed events (%d) differs from the expected number of input events (%d).'%(n_processed,n_expected))
            return self.failure

        return self.success

