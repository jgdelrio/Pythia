import asyncio
from time import time

from src.utils import cycle
from src.config import VANTAGE_SEMAPHORE_LIMIT, VANTAGE_COOLDOWN, QUANDL_SEMAPHORE_LIMIT


class SemaphoreController:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not SemaphoreController._instance:
            SemaphoreController._instance = super(SemaphoreController, cls).__new__(cls, *args, **kwargs)
        return SemaphoreController._instance

    def __init__(self):
        self._vantage_semaphore = asyncio.Semaphore(value=VANTAGE_SEMAPHORE_LIMIT)
        self._vantage_timers = [None] * VANTAGE_SEMAPHORE_LIMIT
        self._vantage_timer_ind = cycle(range(VANTAGE_SEMAPHORE_LIMIT))
        self._quandl_semaphore = asyncio.Semaphore(value=QUANDL_SEMAPHORE_LIMIT)

    async def get_semaphore(self, api):
        if api in ["vantage", "alpha_vantage"]:
            # Control number of asynchronous requests taking into account the limit of requests per minute
            if any([k is None for k in self._vantage_timers]) or \
               any([time() >= (k + VANTAGE_COOLDOWN) for k in self._vantage_timers]):
                self._vantage_timers[next(self._vantage_timer_ind)] = time()
                await self._vantage_semaphore.acquire()
            else:
                await asyncio.sleep(VANTAGE_COOLDOWN)
                await self.get_semaphore(api)

        elif api == "quandl":
            await self._vantage_semaphore.acquire()

        else:
            raise ValueError("Invalid api: {}".format(api))

    def release_semaphore(self, api):
        if api in ["vantage", "alpha_vantage"]:
            self._vantage_semaphore.release()

        elif api == "quandl":
            self._quandl_semaphore.release()

        else:
            raise ValueError("Invalid api: {}".format(api))
