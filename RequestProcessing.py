import asyncio
import aiohttp
from time import sleep
from rudata.DocsAPI import LIMIT


class RequestProcessing:
    """
    Class for sending requests to RU DATA and getting responses
    """

    def __init__(self, url, payload, headers, semaphore=asyncio.Semaphore(1)):
        self.__url: str = url
        self.__payload: dict = payload
        self.__headers: dict = headers
        self.__semaphore: asyncio.Semaphore = semaphore
        self._response = []

    @property
    async def response(self):
        self._response = await self.post()
        return self._response

    @response.setter
    def response(self, value):
        self._response = value

    async def post(self):
        async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=LIMIT), trust_env=True, timeout=aiohttp.ClientTimeout(3600)
        ) as session:
            async with self.__semaphore, session.post(
                    self.__url,
                    json=self.__payload,
                    headers=self.__headers
            ) as response:
                if response.ok:
                    response.raise_for_status()
                    try:
                        response_body = await response.json()
                        await asyncio.sleep(0.2)
                    except aiohttp.client_exceptions.ClientConnectorError:
                        sleep(10)
                        raise ConnectionError("Restart")
                    finally:
                        return response_body
                else:
                    return []
