import os

from redis import Redis
from rq import Connection, Queue, Worker

listen = ["mn2512"]
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

conn = Redis.from_url(redis_url)

if __name__ == "__main__":
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work(with_scheduler=True)
