from multiprocessing import Process, freeze_support
import time

if __name__ == '__main__':

    freeze_support()

    def clock():
        while True:
            time.sleep(1)
            print('OK')


    p = Process(target=clock)
    p.start()

    time.sleep(30)
    p.join()