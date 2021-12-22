import logging
import urllib.parse
import httpx
import asyncio

class ISamplesAPI:
    RECORD_FORMATS = ['core', 'original', 'full', 'solr']

    def __init__(self, base_url):
        self.base_url = base_url
        self.L = logging.getLogger("iSamplesAPI")

    def thing_url(self, pid, fmt="core"):
        assert fmt in ISamplesAPI.RECORD_FORMATS
        _id = urllib.parse.quote(pid)
        url = f"{self.base_url}/thing/{_id}"
        params = {"format": fmt}
        return f"{url}?{urllib.parse.urlencode(params)}"

    def thing(self, pid, fmt="core"):
        assert fmt in ISamplesAPI.RECORD_FORMATS
        _id = urllib.parse.quote(pid)
        url = f"{self.base_url}/thing/{_id}"
        params = {"format": fmt}
        res = httpx.get(url, params=params)
        return res.json()

    def thing_select(self, q="*:*", fq=None, rows=10, offset=0, fields="*"):
        url = f"{self.base_url}/thing/select"
        params = {"q": q, "wt": "json", "rows": rows, "start": offset, "fl": fields}
        if fq is not None:
            params["fq"] = fq
        res = httpx.get(url, params=params)
        return res.json()

    # API convenience methods
    def getIDs(self, q="*:*", fq=None, rows=10, offset=0):
        res = self.thing_select(q=q, fq=fq, rows=rows, offset=offset, fields="id")
        for doc in res.get("response", {}).get("docs", []):
            yield doc.get("id", None)

    async def _getUrl(self, url):
        async with httpx.AsyncClient() as client:
            res = await client.get(url)
            return res.json()

    async def getRecords(self, pids, format='core'):
        urls = []
        for pid in pids:
            urls.append(self.thing_url(pid, format))
        return await asyncio.gather(*map(self._getUrl, urls))
