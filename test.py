import time

from multiprocessing import Process, Queue
from paramiko import client



class Worker:
    def __init__(self, q):
        self.q = q
        self.c = client.SSHClient()
        self.c.set_missing_host_key_policy(client.AutoAddPolicy())

    def work(self):
        self.c.connect(hostname='127.0.0.1', username='bob', port=2022, key_filename='/home/m/Documents/GitHub/psync/priv')
        cmd = "for i in 1 2 3 4 5 ; do echo cool ; done && echo END"
        sin, sout, serr = self.c.exec_command(cmd)
        c = 0
        while True:
            c += 1
            o = sout.readline()
            print(type(o))
            time.sleep(0.3)
            #if c == 6:
            #    o = None
            #    print("YES")
            #    self.q.put(o)
            #    return
            self.q.put(o)

def main():
    que = Queue()
    worker = Worker(que)
    job = worker.work
    p = Process(target=job)
    p.start()
    while True:
        m = que.get()
        time.sleep(0.3)
        print(m)
        print(p.is_alive())
    p.join()


main()
