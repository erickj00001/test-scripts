#!/usr/bin/python3

import eventlet
from eventlet.db_pool import RawConnectionPool
import time

start_time = time.time()


def log(msg):
    offset = time.time() - start_time
    print('[{:6.3f}] {}'.format(offset, msg))


class DummyConnection(object):
    counter = 1
    next_delay = 0

    def __init__(self):
        self.myid = DummyConnection.counter
        DummyConnection.counter += 1
        delay = DummyConnection.next_delay
        log('[{}] Opening (delay {})...'.format(self, delay))
        done = False
        try:
            if delay:
                eventlet.sleep(delay)
            log('[{}] Opened'.format(self))
            done = True
        finally:
            if not done:
                log('[{}] Connection aborted!'.format(self))

    def __repr__(self):
        return 'conn-{}'.format(self.myid)

    def close(self):
        log('[{}] Closed'.format(self, self.myid))

    def rollback(self):
        pass


class DummyDatabase(object):

    @staticmethod
    def connect(**_):
        return DummyConnection()


def thread_task(name, pool):
    with eventlet.Timeout(8):
        try:
            log('({}) Requesting connection'.format(name))
            with pool.item() as conn:
                log('({}) Checked out connection {}'.format(name, conn))
            return True
        except eventlet.Timeout:
            log('({}) Timed out'.format(name))
            return False


def main():
    pool = RawConnectionPool(DummyDatabase,
                             max_size=1,
                             max_age=4,
                             max_idle=60,
                             connect_timeout=60)

    with pool.item() as conn:
        log('(main) Checked out connection {}'.format(conn))

        # The next time we open a connection, there will be a long
        # delay in the connect function (i.e. as if the database were
        # unreachable).
        DummyConnection.next_delay = 10

        # Start a thread which will try to get a connection from the
        # pool (but will have to wait because we are still holding it
        # and the pool has max_size=1)
        t = eventlet.spawn(thread_task, 'test1', pool)

        # Sleep 6 seconds.  Note that this is longer than max_age, so
        # by the time we return the connection it will be expired.
        eventlet.sleep(6)
        log('(main) Returning connection {}'.format(conn))

    log('Waiting for test1...')
    t.wait()
    log('test1 finished')
    eventlet.sleep(1)

    # At this point we have no connections checked out, and no
    # background threads running.  We'll also set the connection
    # delay to zero.  So the next test ought to be able to create
    # a new connection immediately -- but instead it times out!
    DummyConnection.next_delay = 0
    t = eventlet.spawn(thread_task, 'test2', pool)
    result = t.wait()

    if result:
        log('--- TEST PASSED ---')
    else:
        log('--- TEST FAILED ---')


if __name__ == '__main__':
    main()
