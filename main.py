import responder
from threading import Thread, Event, Lock
import signal
from typing import List, Dict
import sys
import os
from datetime import datetime as dt
import time
import psutil
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
    'logs/JMB.log',
    rotation='1 day',
    level='INFO',
    encoding='utf-8')


class Main(responder.API):
    def __init__(self):
        super().__init__()

        logger.info('### startup')
        self.running = True

        self.lack: Dict[str, Stock] = {}
        self.holdSecs = (60 * 2)
        self.locker = Lock()

        self.collector = Collector()
        self.collector.start()

        self.dbsession = DBSession(path='NMEA', buffersize=1000)
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

        # logger.debug('$$$ collector = %d' % self.collector.pid)
        # logger.debug('$$$ dbsession = %d' % self.dbsession.pid)

        self.run(address='0.0.0.0', port=8080)

        self.running = False
        # self.collector.join()
        self.dbsession.join()
        logger.info('shutdown')

    def healthChecker(self, *, pid: int, name:str):
        p = psutil.Process(pid)
        cpuP = p.cpu_percent(interval=1)
        cpuN = p.cpu_num()
        logger.debug('=== %s: %d - %.1f' % (name, cpuN, cpuP))

    def clocker(self, number, frame):
        self.ticker.set()
        self.counter += 1

    def loop(self):
        while self.running:
            try:
                stock: Stock = self.collector.outputQueue.get()
            except (KeyboardInterrupt,) as e:
                logger.warning(e)
            else:
                for nmea in stock.nmea:
                    self.ws.broadcast(message=nmea.raw.decode())
                    self.dbsession.qp.put(nmea.raw)
                    name = nmea.item[0][1:]
                    with self.locker:
                        # if name not in self.lack:
                        #     logger.success('+++ append %s' % name)
                        self.lack[name] = nmea

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
        while self.running:
            self.ticker.clear()
            self.ticker.wait()
            thisEntries: int = len(self.lack)
            if thisEntries != lastEntries:
                logger.debug('Holding %d NMEA' % thisEntries)
            lastEntries = thisEntries

            if self.counter % 5 == 0:
                self.cleaner()
                # self.healthChecker(name='main', pid=os.getpid())
                self.healthChecker(name=self.collector.name, pid=self.collector.pid)
                # self.healthChecker(name=self.dbsession.name, pid=self.dbsession.pid)

if __name__ == '__main__':

    main = Main()
    while True:
        time.sleep(5)
        # pprint(main.lack)
