import logging
import urllib.parse
import httpx
import io
import asyncio
import ijson

MAX_STREAMING_ROWS = 500000


class StringIteratorIO(io.TextIOBase):
    # https://gist.github.com/anacrolix/3788413

    def __init__(self, iter):
        self._iter = iter
        self._left = ""

    def readable(self):
        return True

    def _read1(self, n=None):
        while not self._left:
            try:
                self._left = next(self._iter)
            except StopIteration:
                break
        ret = self._left[:n]
        self._left = self._left[len(ret) :]
        return ret

    def read(self, n=None):
        l = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                l.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                l.append(m)
        return "".join(l)

    def readline(self):
        l = []
        while True:
            i = self._left.find("\n")
            if i == -1:
                l.append(self._left)
                try:
                    self._left = next(self._iter)
                except StopIteration:
                    self._left = ""
                    break
            else:
                l.append(self._left[: i + 1])
                self._left = self._left[i + 1 :]
                break
        return "".join(l)


class ISamplesAPI:
    RECORD_FORMATS = ["core", "original", "full", "solr"]

    def __init__(self, base_url):
        self.base_url = base_url
        self.L = logging.getLogger("iSamplesAPI")

    def thing_url(self, pid, fmt="core"):
        assert fmt in ISamplesAPI.RECORD_FORMATS
        _id = urllib.parse.quote(pid)
        url = f"{self.base_url}/thing/{_id}"
        params = {"format": fmt}
        return f"{url}?{urllib.parse.urlencode(params)}"

    async def thing(self, pid, fmt="core"):
        assert fmt in ISamplesAPI.RECORD_FORMATS
        _id = urllib.parse.quote(pid)
        url = f"{self.base_url}/thing/{_id}"
        params = {"format": fmt}
        async with httpx.AsyncClient() as client:
            res = await client.get(url, params=params)
        return (pid, res.json())
        # res = httpx.get(url, params=params)
        # return res.json()

    def thing_list_metadata(self):
        url = self.base_url
        res = httpx.get(url)
        return res.json()

    def thing_types(self):
        url = f"{self.base_url}/"
        res = httpx.get(url)
        return res.json()

    def thing_list(self, offset=0, limit=1000, status=200, authority=None):
        url = f"{self.base_url}/"
        params = {
            "offset": offset,
            "limit": limit,
            "status": status,
            "authority": authority,
        }
        res = httpx.get(url, params=params)
        return res.json()

    def thing_select(self, q="*:*", fq=None, rows=10, offset=0, fields="*"):
        url = f"{self.base_url}/thing/select"
        params = {"q": q, "wt": "json", "rows": rows, "start": offset, "fl": fields}
        if fq is not None:
            params["fq"] = fq
        res = httpx.get(url, params=params)
        return res.json()

    def thing_select_info(self):
        url = f"{self.base_url}/thing/select/info"
        res = httpx.get(url)
        return res.json()

    def thing_stream(
        self,
        q: str = "*:*",
        rows: int = MAX_STREAMING_ROWS,
        offset: int = 0,
        xy_count: bool = False,
        fq: list = None,
        fl: list = None,
        random_sel: bool = False,
        sort: dict = None,
    ):

        url = f"{self.base_url}/thing/stream"
        params = {
            "q": q,
            "rows": rows,
            "offset": offset,
            "xycount": xy_count,
        }
        if fq is not None:
            params["fq"] = fq
        if fl is not None:
            params["fl"] = ",".join(fl)
        if sort is not None:
            params["sort"] = sort
        if random_sel:
            params["select"] = "random"
        with httpx.stream("GET", url, params=params) as response:
            for record in ijson.items(
                StringIteratorIO(response.iter_text()), "result-set.docs.item", use_float=True
            ):
                yield record

    def things_geojson_heatmap(
        self,
        query="*:*",
        fq=None,
        min_lat=-90.0,
        max_lat=90.0,
        min_lon=-180.0,
        max_lon=180.0,
    ):
        raise NotImplementedError()

    def things_leaflet_heatmap(
        self,
        query="*:*",
        fq=None,
        min_lat=-90.0,
        max_lat=90.0,
        min_lon=-180.0,
        max_lon=180.0,
    ):
        raise NotImplementedError()

    # API convenience methods
    def getIDs(self, q="*:*", fq=None, rows=10, offset=0):
        res = self.thing_select(q=q, fq=fq, rows=rows, offset=offset, fields="id")
        for doc in res.get("response", {}).get("docs", []):
            yield doc.get("id", None)

    async def getRecords(self, pids, format="core"):
        """
        Retrieve the records in the specified format.

        Args:
            pids: List of Identifiers for the records
            format: Name of a supported record format

        Returns:
            iterator of promises that resolve to (pid, record) tuples
        """

        def _rmap(pid):
            return self.thing(pid, format)

        return await asyncio.gather(*map(_rmap, pids))
