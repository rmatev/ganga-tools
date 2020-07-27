import json
import subprocess

# FIXME lb-run ROOT does not seem to work these days
ROOT_PREFIX = ['lb-run', 'Gaudi/latest']

def _get_trees(x, dir_name=""):
    """Recursively get trees from x.
       x can be a TFile or a TDirectoryFile
       Returns a set of tuples: set( (tree_name, tree_object) )
       Prepends the name of the object with dir_name
       (dir_name should include a trailing /)
    """
    trees = set()
    keys = set(key.GetName() for key in x.GetListOfKeys())
    for key in keys:
        obj = x.Get(key)
        class_name = obj.IsA().GetName()
        if class_name == "TTree":
            trees.add((dir_name+obj.GetName(), obj))
        elif class_name == "TDirectoryFile":
            trees = trees.union(_get_trees(obj, dir_name+obj.GetName()+"/"))
    return trees


def _get_tree_entries(files, ignore_empty=False, ignore_missing=False):
    """Get number of entries of all trees in files
       Returns a dictionary: {"tree_name":tree_entries}
       tree_name includes the directory name(s), if applicable
    """
    from ROOT import TFile
    from collections import defaultdict
    if not hasattr(files, "__iter__"):  # allow single filename to be passed
        files = [files]
    entries = defaultdict(int)
    for f in files:
        file0 = TFile.Open(f)
        if not file0:
            if ignore_missing:
                print("Warning: Can't find/open file: "+f)
                continue
            raise IOError("Can't find/open file: "+f)

        trees = _get_trees(file0)
        if not trees:
            if ignore_empty:
                print('Warning: No TTree objects found in '+f)
                continue
            raise ValueError('No TTree objects found in '+f)

        for name, tree in trees:
            entries[name] += tree.GetEntries()
        file0.Close("R")
    return dict(entries)


def get_tree_entries(files, ignore_empty=False, ignore_missing=False):
    import os

    cmd = '_get_tree_entries({!r}, {!r}, {!r})'.format(files, ignore_empty, ignore_missing)
    env = os.environ.copy()
    del env['TERM']  # fix for https://bugs.python.org/issue19884
    out = subprocess.check_output(ROOT_PREFIX + ['python', __file__, cmd], env=env)
    return json.loads(out)


if __name__ == '__main__':
    import sys
    cmd = sys.argv[1]
    print(json.dumps(eval(cmd)))
