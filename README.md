# isamples-cli

Offers `iscli`, a python comamnd line client for iSamples.

## Installation

Using [`pipx`](https://pypa.github.io/pipx/):

```
pipx install git+https://github.com/datadavev/isamples-cli.git
```

## Operation

```
Usage: iscli [OPTIONS] COMMAND [ARGS]...

Options:
  -L, --LOGLEVEL TEXT  Logging level
  --help               Show this message and exit.

Commands:
  fields    List fields of the Solr search index.
  pids      List identifiers
  records   Retrieve records from SERVICE
  services  List the available service endpoints
  stream    Stream a potentially larger number of records
```