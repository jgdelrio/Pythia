import asyncio
from src.config import VANTAGE_SEMAPHORE_LIMIT


class SemaphoreController:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not SemaphoreController._instance:
            SemaphoreController._instance = super(SemaphoreController, cls).__new__(cls, *args, **kwargs)
        return SemaphoreController._instance

    def __init__(self):
        self._vantage_semaphore = asyncio.Semaphore(value=VANTAGE_SEMAPHORE_LIMIT)

    async def get_semaphore(self, api):
        if api in ["vantage", "alpha_vantage"]:
            await self._vantage_semaphore.acquire()

    def release_semaphore(self, api):
        if api in ["vantage", "alpha_vantage"]:
            self._vantage_semaphore.release()
