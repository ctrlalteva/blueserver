from atproto import FirehoseSubscribeReposClient, models, firehose_models, parse_subscribe_repos_message
import multiprocessing as mp
import multiprocessing.sharedctypes as mpt
import signal
import sqlite3
import time
from types import FrameType
from typing import Any
import lib.data_processing as dp


def signal_handler(_: int, __: FrameType) -> None:
    # Stop receiving new messages
    print("Keyboard interrupt received. Waiting for the queue to empty before terminating processes...")
    client.stop()
    # Drain the messages queue
    wait_timer = 0
    while not queue.empty():
        print("Waiting for the queue to empty...")
        time.sleep(0.5)
        wait_timer += 0.5
        if wait_timer > 5:
            print("Queue is stuck. Force terminating processes...")
            pool.terminate()
            exit(0)
    print("Queue is empty. Gracefully closing processes...")
    pool.terminate()
    pool.join()
    exit(0)


def get_firehose_params(
    cursor_value: mpt.Value,
) -> models.ComAtprotoSyncSubscribeRepos.Params:
    return models.ComAtprotoSyncSubscribeRepos.Params(cursor=cursor_value.value)


def measure_events_per_second(func: callable) -> callable:
    def wrapper(*args) -> Any:
        con = sqlite3.connect("./data/test.db")
        wrapper.calls += 1
        cur_time = time.time()
        if cur_time - wrapper.start_time >= 1:
            calls = wrapper.calls
            qsize = queue.qsize()
            workers = len(mp.active_children())
            posts = dp.firehose_raw_size(con)
            print(f"CLIENT: {calls} evts/s --- QUEUE: {qsize} evts --- WORKERS: {workers} --- POSTS: {posts}")
            wrapper.start_time = cur_time
            wrapper.calls = 0
        return func(*args)

    wrapper.calls = 0
    wrapper.start_time = time.time()
    return wrapper


def worker_main(cursor_value: mpt.Value, pool_queue: mp.Queue) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    con = sqlite3.connect("./data/test.db")

    while True:
        # worker gets message from queue
        new_message = pool_queue.get()

        # parse message for commit
        commit = parse_subscribe_repos_message(new_message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            continue
        if commit.seq % 20 == 0:
            cursor_value.value = commit.seq
        if not commit.blocks:
            continue

        # decode commit and
        create_post_records = dp.get_create_post_record(commit)
        if len(create_post_records) != 0:
            for record in create_post_records:
                dp.insert_commit(record, con)


if __name__ == "__main__":
    # signal handling setup
    signal.signal(signal.SIGINT, signal_handler)

    # cursor setup
    start_cursor = None
    params = None
    cursor = mp.Value("i", 0)
    if start_cursor is not None:
        cursor = mp.Value("i", start_cursor)
        params = get_firehose_params(cursor)

    # worker and queue setup
    workers_count = 4
    max_queue_size = 10000
    queue = mp.Queue(maxsize=max_queue_size)
    pool = mp.Pool(workers_count, worker_main, (cursor, queue))

    # database and client setup
    dp.sqlite_setup()
    client = FirehoseSubscribeReposClient(params)

    @measure_events_per_second
    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        if cursor.value:
            client.update_params(get_firehose_params(cursor))
        queue.put(message)

    # run client
    print("Starting firehose capture...")
    client.start(on_message_handler)
