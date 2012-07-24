import gevent
from gevent import queue

q = queue.Queue()

def getter():
    for item in q:
        print(item)

def putter():
    for i in range(10):
        q.put(i)
        gevent.sleep(1.0)
    q.put(StopIteration)

a = gevent.spawn(getter)
b = gevent.spawn(putter)
gevent.joinall([a,b])
