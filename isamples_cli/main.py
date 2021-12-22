import os
import sys
import click
import logging
import httpx
import urllib.parse
import json
import asyncio

from isamples_cli import isamples_api

# List of services. This should be dynamic, probably from github.
# URLs should not include a trailing slash
SERVICES = [
    {"name": ["mars", "dev"], "url": "https://mars.cyverse.org"},
    {"name": ["opencontext", "oc"], "url": "https://henry.cyverse.org/opencontext"},
    {"name": ["smithsonian", "si"], "url": "https://henry.cyverse.org/smithsonian"},
    {
        "name": [
            "geome",
        ],
        "url": "https://henry.cyverse.org/geome",
    },
    {
        "name": [
            "sesar",
        ],
        "url": "https://henry.cyverse.org/sesar",
    },
    {"name": ["central", "isc"], "url": "https://hyde.cyverse.org"},
    {
        "name": [
            "local",
        ],
        "url": "http://localhost:8000",
    },
]
RECORD_FORMATS = ["core", "original", "full", "solr"]

FNAME_CHAR_REPLACEMENT = [(":", "_"), ("/", "~"), ("\\", "~")]

# enable -h for help
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

# Names for log levels
LOGLEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}

L = logging.getLogger("iSample")


def getServiceByName(name):
    for service in SERVICES:
        if name.lower() in service.get("name", []):
            return service
    return None


def identifierToFileName(pid, path="."):
    fname = pid
    for a, b in FNAME_CHAR_REPLACEMENT:
        fname = fname.replace(a, b)
    final_name = fname
    cntr = 1
    while os.path.exists(os.path.join(path, final_name)):
        final_name = f"{fname}-{cntr}"
    return final_name


@click.group()
@click.pass_context
@click.option("-L", "--LOGLEVEL", help="Logging level", default="info")
def main(ctx, loglevel):
    lformat = "%(levelname)s:%(name)s %(message)s"
    logging.basicConfig(
        level=LOGLEVELS.get(loglevel, "info"),
        format=lformat,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ctx.ensure_object(dict)
    return 0


@main.command(name="services", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def listServices(ctx):
    """
    List the available service endpoints
    """
    for svc in SERVICES:
        print(", ".join(svc.get("name", [])))
        print(f"  {svc.get('url', '')}")


@main.command(name="fields", context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.argument("service")
def listFields(ctx, service):
    """
    List fields of the Solr search index.
    """
    svc = getServiceByName(service)
    if svc is None:
        L.error("Unknown service: '%s'", service)
        return 1
    api = isamples_api.ISamplesAPI(svc.get("url"))
    res = api.thing_select_info()
    fields = res["schema"]["fields"]
    for k, v in fields.items():
        print(f"{k} ({v['type']}) {v['flags']}")
    # print(json.dumps(api.thing_select_info(), indent=2))


@main.command(name="pids", context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.argument("service")
@click.option("-q", "--query", help="Solr query to filter records", default="*:*")
@click.option(
    "-n", "--numrecs", help="Number of records to retrieve", default=10, type=int
)
def getIdentifiers(ctx, service, query, numrecs):
    """
    List identifiers
    """
    svc = getServiceByName(service)
    if svc is None:
        L.error("Unknown service: '%s'", service)
        return 1
    api = isamples_api.ISamplesAPI(svc.get("url"))
    for pid in api.getIDs(q=query, rows=numrecs):
        print(pid)


@main.command(name="records", context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.argument("service")
@click.option("-q", "--query", help="Solr query to filter records", default="*:*")
@click.option(
    "-n", "--numrecs", help="Number of records to retrieve", default=10, type=int
)
@click.option(
    "-m",
    "--model",
    help="Record model",
    type=click.Choice(RECORD_FORMATS),
    default="core",
)
@click.option("-d", "--destfolder", help="Folder for saving records", default=None)
def getRecords(ctx, service, query, numrecs, model, destfolder):
    """
    Retrieve records from SERVICE

    SERVICE is the name of an iSamples endpoint

    Output is to stdout unless DESTFOLDER is specified, in which case
    individual JSON records are written to the folder with an additional
    index.json file that contains the mapping between filename and PID.
    """
    svc = getServiceByName(service)
    if svc is None:
        L.error("Unknown service: '%s'", service)
        return 1
    if destfolder is not None:
        os.makedirs(destfolder, exist_ok=True)
    api = isamples_api.ISamplesAPI(svc.get("url"))
    _ids = list(api.getIDs(q=query, rows=numrecs))
    _index = {}
    counter = 0
    for (pid, rec) in asyncio.run(api.getRecords(_ids, format=model)):
        counter += 1
        if destfolder is None:
            print(json.dumps(rec, indent=2))
        else:
            _index[pid] = identifierToFileName(pid, path=destfolder)
            with open(os.path.join(destfolder, _index[pid]), "w") as destf:
                json.dump(rec, destf, indent=2)
    if destfolder is not None:
        with open(os.path.join(destfolder, "index.json"), "w") as destf:
            json.dump(_index, destf, indent=2)
    L.info("Retrieved %s records", counter)


@main.command(name="stream", context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.argument("service")
@click.option("-q", "--query", help="Solr query to filter records", default="*:*")
@click.option(
    "-n", "--numrecs", help="Number of records to retrieve", default=10, type=int
)
@click.option("-f", "--fields", help="Fields to return", default=None)
@click.option("-r", "--random_sel", help="Random selection of records", is_flag=True)
@click.option(
    "--xycount", help="Aggregate by longitude, latitude and return count", is_flag=True
)
def getStream(ctx, service, query, numrecs, fields, random_sel, xycount):
    '''
    Stream a potentially larger number of records
    '''
    svc = getServiceByName(service)
    if svc is None:
        L.error("Unknown service: '%s'", service)
        return 1
    fl = None
    if fields is not None:
        fl = fields.split(",")
    api = isamples_api.ISamplesAPI(svc.get("url"))
    for record in api.thing_stream(
        q=query, rows=numrecs, fl=fl, random_sel=random_sel, xy_count=xycount
    ):
        print(json.dumps(record))


if __name__ == "__main__":
    sys.exit(main())
