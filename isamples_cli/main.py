import sys
import click
import logging
import httpx
import urllib.parse
import json
import asyncio

SERVICES = [{"name": ["mars", "dev"], "url": "https://mars.cyverse.org"}]
RECORD_FORMATS = ['core', 'original', 'full', 'solr']

L = logging.getLogger("iSample")


def getServiceByName(name):
    for service in SERVICES:
        if name.lower() in service.get("name", []):
            return service
    return None




@click.group()
@click.pass_context
def main(ctx):
    lformat = "%(levelname)s:%(name)s %(message)s"
    logging.basicConfig(
        level=logging.DEBUG,
        format=lformat,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ctx.ensure_object(dict)
    return 0


@main.command()
@click.pass_context
@click.argument("service")
@click.option("-q", "--query", help="Solr query to filter records", default="*:*")
@click.option(
    "-n", "--numrecs", help="Number of records to retrieve", default=10, type=int
)
@click.option(
    "-f",
    "--format",
    help="Record format",
    type=click.Choice(RECORD_FORMATS),
    default="core"
)
def getRecords(ctx, service, query, numrecs, format):
    svc = getServiceByName(service)
    if svc is None:
        L.error("Unknown service: '%s'", service)
        return 1
    api = ISamplesAPI(svc.get("url"))
    _ids = list(api.getIDs(q=query, rows=numrecs))
    print(json.dumps(asyncio.run( api.getRecords(_ids, format=format)), indent=2))
    #for _id in api.getIDs(q=query, rows=numrecs):
    #    L.info(_id)
    #    print(json.dumps(api.thing(_id, fmt=format), indent=2))


if __name__ == "__main__":
    sys.exit(main())
