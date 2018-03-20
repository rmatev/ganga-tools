_externalPackages = {
   }

try: # for Ganga >= v7.0.0
    from GangaCore.Utility.Setup import PackageSetup
except ImportError:
    from Ganga.Utility.Setup import PackageSetup # for Ganga < v7.0.0

# The setup object
setup = PackageSetup(_externalPackages)

def standardSetup(setup=setup):
    """ Perform automatic initialization of the environment of the package.
    The gangaDir argument is only used by the core package, other packages should have no arguments.
    """
    pass
