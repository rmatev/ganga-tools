# ganga-tools
Ganga tools and extensions for LHCb

# Plugins
### LHCbCompleteChecker
A _checker_ that ensures that all input files were fully processed
(using the metadata). This is a simplified version of the existing
`LHCbMetaDataChecker`, which also overcomes some deficiencies.
This checker is not suitable if only part of the input is processed
(e.g. `EvtMax` is used).

Use:
```python
job.postprocessors.append(LHCbCompleteChecker())
```

### LHCbBookkeepingChecker
A _checker_ that compares the number of processed events as reported in
the metadata and the number of events from the bookkeeping
(_EventStat/FullStat_ field).
Fails if the processed and the expected number of events differ.
This checker is not suitable if only part of the input is processed
(e.g. `EvtMax` is used).

Use:
```python
job.postprocessors.append(LHCbBookkeepingChecker())
```
### SplitByFilesAndRun
A _splitter_ that is similar to `SplitByFiles` but ensures that each subjob's
input contains files from one _run_ only.
Expect many subjobs if using this splitter (at least as many as the number
of runs)!

Use (see `SplitByFiles`):
```python

job.splitter = SplitByFilesAndRun(filesPerJob=50)
```

## Setup instructions
First, clone the repository
```
git clone https://github.com/rmatev/ganga-tools.git ~/ganga-tools
```
Second, edit `~/.gangarc` by uncommenting `RUNTIME_PATH` and appending the
appropriate path, e.g.:
```python
#RUNTIME_PATH = GangaDirac:GangaGaudi:GangaLHCb:
```
becomes
```python
RUNTIME_PATH = GangaDirac:GangaGaudi:GangaLHCb:~/ganga-tools/GangaLHCbExt:
```
