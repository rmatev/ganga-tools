from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor
from GangaCore.GPIDev.Base.Proxy import GPIProxyObject
from GangaCore.GPIDev.Adapters.IChecker import IChecker
from GangaCore.GPIDev.Schema import ComponentItem, FileItem, Schema, SimpleItem, Version
from GangaCore.Utility.Config import makeConfig, ConfigError, getConfig
from GangaCore.Utility.Plugin import allPlugins
from GangaCore.Utility.logging import getLogger, log_user_exception

logger = getLogger()


class LHCbCompleteChecker(IChecker):
    """
    Checks that all input files were fully processed (using the metadata).

    Example:
    job.postprocessors.append(LHCbCompleteChecker())
    """
    _schema = IChecker._schema.inherit_copy()
    _category = 'postprocessor'
    _name = 'LHCbCompleteChecker'

    def check(self, job):
        """Checks metadata of job is within a certain range."""
        try:
            nfullfiles = len(job.metadata['xmldatafiles'].get('full', []))
        except KeyError:
            raise PostProcessException("The metadata value 'xmldatanumbers' was not defined")
        try:
            nskipped = len(job.metadata['xmlskippedfiles'])
        except KeyError:
            raise PostProcessException("The metadata value 'xmlskippedfiles' was not defined")

        ninputfiles = len(job.inputdata)
        try:
            ninputfiles = job.splitter.maxFiles or ninputfiles
        except AttributeError:
            pass

        if nskipped > 0:
            logger.info('Job {} skipped {} files, likey unreachable PFNs'.format(job.fqid, nskipped))

        ok = nfullfiles == ninputfiles
        if not ok:
            logger.info('LHCbCompleteChecker has failed job {} (expected={}, nfullfiles={}, nskipped={})'.format(
                job.fqid, ninputfiles, nfullfiles, nskipped))
        return ok
