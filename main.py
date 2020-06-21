import responder
from threading import Thread, Event, Lock
import signal
from typing import List, Dict
import sys
from datetime import datetime as dt
import time
from loguru import logger

from collector import Stock, Collector
from websocketserver import WebsocketServer
from dbsession import DBSession, Record

logger.remove()
logger.add(
    sys.stdout,
    colorize=True,
    format="<level>{time:HH:mm:ss} {file}:{line}:{function} {message}</level>")
logger.add(
    'logs/mc.log',
    rotation='1 day',
    level='WARNING',
    encoding='utf-8')


class Main(responder.API):
    def __init__(self):
        super().__init__()

        self.lack: Dict[str, Stock] = {}
        self.holdSecs = (60 * 2)
        self.locker = Lock()

        self.collector = Collector()
        self.collector.start()

        self.dbsession = DBSession(path='NMEA', buffersize=5000)
        self.dbsession.start()

        self.ws = WebsocketServer(debug=True)
        self.add_route('/ws', self.ws.wsserver, websocket=True)

        self.counter = 0
        self.ticker = Event()
        signal.signal(signal.SIGALRM, self.clocker)
        signal.setitimer(signal.ITIMER_REAL, 1, 1)

        self.cycle = Thread(target=self.routineWorker, daemon=True)
        self.cycle.start()

        self.mainLoop = Thread(target=self.loop, daemon=True)
        self.mainLoop.start()

        self.run(address='0.0.0.0', port=8080)

        # self.collector.join()
        self.dbsession.join()

    def clocker(self, number, frame):
        self.ticker.set()
        self.counter += 1

    def loop(self):
        while True:
            try:
                stock: Stock = self.collector.outputQueue.get()
            except (KeyboardInterrupt,) as e:
                logger.warning(e)
            else:
                # now = dt.now()
                self.ws.broadcast(message=stock.raw.decode())
                self.dbsession.qp.put(stock.raw)
                name = stock.item[0][1:]
                with self.locker:
                    # if name not in self.lack:
                    #     logger.success('+++ append %s' % name)
                    self.lack[name] = stock

    def cleaner(self):
        now = dt.now()
        with self.locker:
            expired: List[str] = []
            for k, v in self.lack.items():
                if (now - v.at).total_seconds() >= self.holdSecs:
                    expired.append(k)
            if len(expired):
                # logger.debug('--- %s was expired' % expired)
                for name in expired:
                    del self.lack[name]

    def routineWorker(self):
        lastEntries: int = 0
        while True:
            self.ticker.clear()
            self.ticker.wait()
            thisEntries: int = len(self.lack)
            if thisEntries != lastEntries:
                logger.info('Holding %d NMEA' % thisEntries)
            lastEntries = thisEntries
            if self.counter % 5 == 0:
                self.cleaner()

if __name__ == '__main__':

    main = Main()
    while True:
        time.sleep(5)
        # pprint(main.lack)
