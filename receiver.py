from datetime import datetime as dt
from dataclasses import dataclass
import socket
from threading import Thread
from queue import Queue
from multiprocessing import Queue as MPQueue
from contextlib import closing
from enum import IntEnum

from loguru import logger


class StreamType(IntEnum):
    TypeNaked = 0
    Type450 = 1
    TypeFURUNO = 99


@dataclass()
class Antenna(object):
    streamType: int
    ip: str
    port: int


@dataclass()
class Packet(object):
    member: str
    stream: bytes
    sender: str
    at: dt


class Receiver(Thread):

    def __init__(self, *, name: str, params: Antenna, qp: Queue, bufferSize: int = 4096):
        super().__init__()
        self.daemon = True
        self.name = name

        self.qp = qp
        self.params = params
        self.bufferSize = bufferSize

        self.running = True

    def run(self) -> None:
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                                socket.inet_aton(self.params.ip) + socket.inet_aton('0.0.0.0'))
                sock.bind(('', self.params.port))

                while self.running:
                    stream, ipv4 = sock.recvfrom(self.bufferSize)
                    self.qp.put(Packet(sender=ipv4[0], at=dt.now(), stream=stream, member=self.name))
        except (socket.error,) as e:
            self.running = False
            logger.error(e)
        except (KeyboardInterrupt,) as e:
            self.running = False
