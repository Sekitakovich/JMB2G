from typing import Dict
from starlette.websockets import WebSocket
from starlette.websockets import WebSocketDisconnect  # 毎回これを忘れて探す
from loguru import logger
from queue import Queue
from threading import Thread, Lock
import asyncio


class WebsocketServer(object):
    def __init__(self, *, debug: bool = False):
        self.debug = debug
        self.clients: Dict[str, WebSocket] = {}

        self.locker = Lock()  # 念の為

        self.eventLoop = asyncio.get_event_loop()
        # nest_asyncio.apply()
        # self.bcQueue = Queue()  # for boradcast
        # self.bcThread = Thread(target=self.broadCaster, daemon=True, name='broadCaster')
        # self.bcThread.start()

    async def wsserver(self, ws: WebSocket):
        await ws.accept()
        key = ws.headers.get('sec-websocket-key')
        with self.locker:
            self.clients[key] = ws
        if self.debug:
            logger.debug('+++ Websocket: hold %d clients' % (len(self.clients)))
        # if self.debug:
        #     logger.debug('%s has come' % key)

        while True:
            try:
                msg = await ws.receive_text()
            except WebSocketDisconnect as e:
                with self.locker:
                    del self.clients[key]
                await ws.close()
                if self.debug:
                    logger.warning('%s was lost' % key)
                break
            else:
                for k, v in self.clients.items():
                    if k != key:
                        await v.send_text(msg)
                if self.debug:
                    logger.debug('[%s] from %s' % (msg, key))

    async def _broadcast(self, *, message: str):  # 念願叶う
        try:
            for v in self.clients.values():
                await v.send_text(message)
        except Exception as e:
            logger.error(e)

    def broadcast(self, *, message: str):
        if len(self.clients) and message:
            with self.locker:
                try:
                    self.eventLoop.run_until_complete(self._broadcast(message=message))
                    # await self._broadcast(message=message)
                except RuntimeError as e:
                    logger.error(e)

    # def broadCaster(self):
    #     asyncio.set_event_loop(self.eventLoop)  # Notice!
    #     while True:
    #         message: str = self.bcQueue.get()
    #         if len(self.clients) and message:
    #             with self.locker:
    #                 try:
    #                     # asyncio.set_event_loop(self.eventLoop)  # Notice!
    #                     self.eventLoop.run_until_complete(self._broadcast(message=message))
    #                 except RuntimeError as e:
    #                     logger.error(e)