"""Fan-out of live events to every open server-sent-events stream."""

import json
import queue
import threading

lock = threading.Lock()
streams = set()


def push(event):
    data = json.dumps(event)
    with lock:
        for q in list(streams):
            try:
                q.put_nowait(data)
            except queue.Full:
                streams.discard(q)


def subscribe():
    q = queue.Queue(1000)
    with lock:
        streams.add(q)
    return q


def unsubscribe(q):
    with lock:
        streams.discard(q)


def subscribed(q):
    with lock:
        return q in streams
