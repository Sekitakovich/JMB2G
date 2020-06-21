import time
from datetime import datetime as dt
from typing import List
from contextlib import closing
import pathlib
import sqlite3
from multiprocessing import Process, Queue as MPQueue, Lock
from queue import Queue, Empty
from dataclasses import dataclass
from loguru import logger


@dataclass()
class Record(object):
    stream: bytes = b''  # NMEA asis
    # passed: float  # delta secs from prev
    at: dt = dt.now()  # 受信日時
    ds: float = 0.0


class DBSession(Process):
    def __init__(self, *, path: str, timeout: int = 5, buffersize: int = 32, debug: bool = False):
        super().__init__()
        self.daemon = False
        self.name = 'SQLite'

        self.debug = debug
        self.live: bool = True

        self.qp: MPQueue = MPQueue()
        self.path = pathlib.Path(path)  # path for *.db
        self.nameformat: str = '%04d-%02d-%02d.db'
        self.dateformat: str = '%Y-%m-%d %H:%M:%S.%f'
        self.locker = Lock()

        self.counter: int = 0
        now = dt.now()
        self.lastat: dt = now
        self.lastsave: dt = now
        self.timeout = timeout
        # self.buffer: List[Record] = []
        self.buffersize = buffersize

        self.fifo = Queue()

        self.schema = 'CREATE TABLE "sentence" ( \
                    	"id"	INTEGER NOT NULL DEFAULT 0 PRIMARY KEY AUTOINCREMENT, \
                        "at"	TEXT NOT NULL DEFAULT \'\', \
                        "ds"	REAL NOT NULL DEFAULT 0.0, \
                        "stream"	TEXT NOT NULL DEFAULT \'\' \
                    )'

    def __del__(self):
        logger.warning('DEL!')
        self.append(at=self.lastat)

    def stop(self):
        self.live = False

    def create(self, *, cursor: sqlite3.Cursor):
        cursor.execute(self.schema)

    def append(self):
        at = dt.now()
        size = self.fifo.qsize()
        if size:
            # passed = (at - self.lastat).total_seconds()
            name = self.nameformat % (at.year, at.month, at.day)
            file = self.path / name  # pathlib
            exists = file.exists()
            now = dt.now()
            with closing(sqlite3.connect(str(file))) as db:
                ts = time.time()
                cursor = db.cursor()
                if exists is False:
                    self.create(cursor=cursor)
                    if self.debug:
                        logger.debug('$$$ %s was created' % str(file))
                for index in range(size):
                    ooo: Record = self.fifo.get()
                    item = [ooo.at.strftime(self.dateformat), ooo.ds, ooo.stream]
                    # print(item)
                    query = 'insert into sentence(at,ds,stream) values(?,?,?)'
                    cursor.execute(query, item)
                cursor.close()
                db.commit()  # never forget
                te = time.time()
                after = int((now - self.lastsave).total_seconds())
                # if self.debug:
                logger.info(
                    '+++ %d records were saved to %s in %f after %d secs' % (size, file, round(te - ts, 2), after))
                self.lastsave = now

    def run(self) -> None:
        if self.debug:
            logger.debug('DBSession start (%d)' % self.pid)
        while self.live:
            try:
                raw: bytes = self.qp.get(timeout=self.timeout)
                self.counter += 1
            except Empty as e:
                self.append()
                # self.lastat = dt.now()
            except KeyboardInterrupt as e:
                self.live = False
                pass
            else:
                now = dt.now()
                if now.day != self.lastat.day:
                    self.append()
                    if self.debug:
                        logger.debug('just in today')

                record = Record(stream=raw, at=now, ds=(now-self.lastat).total_seconds())
                self.fifo.put(record)

                if self.fifo.qsize() >= self.buffersize:
                    self.append()

                self.lastat = now
        self.append()
        if self.debug:
            logger.debug('<<< Roger!')
