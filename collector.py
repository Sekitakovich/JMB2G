from datetime import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, List
from queue import Queue
from multiprocessing import Process, Queue as MPQueue
from functools import reduce
from operator import xor
from enum import IntEnum
from loguru import logger

from receiver import Receiver, Antenna, Packet, StreamType


@dataclass()
class Sentence(object):
    item: List[str] = field(default_factory=list)
    raw: bytes = b''
    at: dt = dt.now()
    error: int = 0

    class Error(IntEnum):
        none = 0
        badSum = 1


@dataclass()
class Stock(object):
    # item: List[str] = field(default_factory=list)
    nmea: List[Sentence] = field(default_factory=list)
    ip: str = ''
    # raw: bytes = b''
    member: str = ''
    sfi: str = ''
    valid: bool = True


class NMEA(object):

    @classmethod
    def checksum(cls, *, raw: bytes) -> bool:
        ok = True
        part = raw.split(b'*')
        try:
            if len(part) == 2:
                csum = int(part[1][0:2], 16)
                body = part[0][1:]
                calc = reduce(xor, body, 0)
                if csum != calc:
                    ok = False
        except (IndexError,) as e:
            ok = False
            logger.error(e)
        return ok


class Format450(object):
    def __init__(self):
        self.symbol = b'UdPbC'
        self.delimitter = b'\\'
        self.item: List[str] = []

    def parse(self, *, src: bytes, sender: str, member: str) -> Stock:
        stock = Stock(member=member)
        part = src.split(self.delimitter)
        sfi = ''
        try:
            header = part[0]
            info = part[1]
            nmea = part[2]

            for ooo in info.split(b'*')[0].split(b','):
                ppp = ooo.split(b':')
                if ppp[0] == b's':
                    sfi = ppp[1].decode()

            sentence: List[Sentence] = []
            for ooo in nmea.split(b'\r\n'):
                if ooo:
                    raw = ooo
                    ppp = raw.decode().split('*')
                    item = ppp[0].split(',')
                    error = Sentence.Error.none if NMEA.checksum(raw=raw) else Sentence.Error.badSum
                    sentence.append(Sentence(raw=raw, item=item, error=error, at=dt.now()))

        except (IndexError,) as e:
            stock.valid = False
            logger.error(e)
        else:
            stock.nmea = sentence
            # stock.item = nmea.decode().split(',')
            stock.at = dt.now()
            stock.ip = sender
            # stock.raw = nmea
            stock.sfi = sfi
            # stock = Stock(item=item, at=at, ip=ip, raw=nmea, member=member)
        return stock


class Collector(Process):

    def __init__(self):

        super().__init__()
        self.daemon = True
        self.name = 'Collector'

        self.inputQueue = Queue()
        self.outputQueue = MPQueue()
        self.receiver: Dict[str, Receiver] = {}
        self.f450 = Format450()

        for a in range(1, 17):
            name = 'CH%02d' % a
            ip = '239.192.0.%d' % a
            port = 60000 + a
            t = Receiver(name=name, params=Antenna(ip=ip, port=port, streamType=StreamType.Type450), qp=self.inputQueue)
            self.receiver[name] = t

    def run(self) -> None:
        for k, v in self.receiver.items():
            logger.debug('+++ %s start' % k)
            v.start()
        while True:
            try:
                packet: Packet = self.inputQueue.get()
            except (KeyboardInterrupt,) as e:
                # logger.error(e)
                break
            else:
                stock = self.f450.parse(src=packet.stream, sender=packet.sender, member=packet.member)
                self.outputQueue.put(stock)
