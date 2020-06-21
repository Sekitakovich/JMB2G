from datetime import datetime as dt
from dataclasses import dataclass
from typing import Dict, List
from queue import Queue
from multiprocessing import Process, Queue as MPQueue
from functools import reduce
from operator import xor
from loguru import logger

from receiver import Receiver, Antenna, Packet


@dataclass()
class Stock(object):
    item: List[str]
    at: dt
    ip: str
    raw: bytes
    member: str
    sfi: str = ''


class Format450(object):
    def __init__(self):
        self.symbol = b'UdPbC'
        self.delimitter = b'\\'
        self.item: List[str] = []

    def validate(self, *, nmea: bytes) -> bool:
        ok = True
        part = nmea.split(b'*')
        if len(part) == 2:
            csum = int(part[1][0:2], 16)
            body = part[0][1:]
            calc = reduce(xor, body, 0)
            if csum != calc:
                ok = False
        return ok

    def parse(self, *, src: bytes, sender: str, member: str) -> Stock:
        part = src.split(self.delimitter)
        raw = part[2]
        self.validate(nmea=raw)
        item = raw.decode().split(',')
        at = dt.now()
        ip = sender
        member = member
        stock = Stock(item=item, at=at, ip=ip, raw=raw, member=member)
        return stock


class Collector(Process):

    def __init__(self):

        super().__init__()
        self.daemon = True

        self.inputQueue = Queue()
        self.outputQueue = MPQueue()
        self.receiver: Dict[str, Receiver] = {}
        self.f450 = Format450()

        for a in range(1, 17):
            name = 'CH%02d' % a
            ip = '239.192.0.%d' % a
            port = 60000 + a
            t = Receiver(name=name, params=Antenna(ip=ip, port=port), qp=self.inputQueue)
            self.receiver[name] = t

    def run(self) -> None:
        for k, v in self.receiver.items():
            logger.info('+++ %s start' % k)
            v.start()
        while True:
            try:
                packet: Packet = self.inputQueue.get()
            except (KeyboardInterrupt,) as e:
                logger.error(e)
                break
            else:
                stock = self.f450.parse(src=packet.stream, sender=packet.sender, member=packet.member)
                self.outputQueue.put(stock)
