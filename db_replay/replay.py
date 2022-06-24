import dataclasses
import io
import itertools
import math
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from queue import Queue
from threading import Thread

import click
import dateutil.parser
import psycopg2.pool
from psycopg2.errors import UniqueViolation, ForeignKeyViolation, InvalidCursorName, NoActiveSqlTransaction

from db_replay.interaction import QueryInteraction, ActiveQuery
from db_replay.interpolation import PolyLine


@dataclasses.dataclass
class Query:
    """
    A query from a log file. Later, it gets extra data such as `my_time_ms` meaning
    how long it took to execute it against current database or optionally `failure_msg`
    if the query failed for us.
    """
    file: str
    line: int
    txid: int
    process_id: int
    original_time_ms: float
    process_start: float
    timestamp: datetime
    failure: Exception =  None
    last_line: int = None
    my_time_ms: float = None
    sql: str = None
    my_process_id: int = None


class Waiter:
    def __init__(self, time_factor: float, first_query: datetime):
        self.first_query = first_query
        self.time_factor = time_factor
        self.real_start_time = datetime.now(timezone.utc)
        self.last_reported = self.real_start_time
        self.last_reported_wait = self.real_start_time
        self.counter = 0

    def wait_for_query(self, query_time: datetime):
        query_offset = query_time - self.first_query
        now = datetime.now(timezone.utc)
        real_offset_s = (now - self.real_start_time).total_seconds()
        wanted_offset_s = query_offset.total_seconds() / self.time_factor
        self.counter += 1

        if now - self.last_reported > timedelta(seconds=5):
            self.last_reported = now
            if real_offset_s - wanted_offset_s > 4:
                click.echo(f'LAGGING! Wanted offset (s)/ Real offset (s): {wanted_offset_s:.0f} / {real_offset_s:.0f}')
            click.echo(f'Processed: {self.counter: 6d} queries in {now - self.real_start_time}')

        sleep_duration = wanted_offset_s - real_offset_s
        if sleep_duration > 0:
            if sleep_duration > 0.5 and now - self.last_reported_wait > timedelta(seconds=2):
                self.last_reported_wait = now
                click.echo(f'Waiting for {sleep_duration:4.1f}s')
            time.sleep(sleep_duration)


POISON_PILL = object()
RE_HEADER = re.compile(r'^(?P<timestamp>.* CEST) \[\d+] (?P<process_start>[a-f0-9]+)\.(?P<process_id>[a-f0-9]+)')
RE_QUERY = re.compile(r' [^ ]+ (?P<txid>[0-9]+) LOG:  duration: (?P<duration_ms>[0-9.]+) ms  statement: (?P<sql>.*)$')
RE_EXCLUDE = re.compile(r'Connection reset by peer|archive-push|pushed WAL file|ERROR:|DETAIL:|STATEMENT:|^\t')
EXECUTOR = ThreadPoolExecutor(max_workers=200)
NON_INTERACTIVE = False
EXTRA_TIMEOUT_MS = PolyLine([
    (0, 30),
    (1, 35),  # For queries that took 1 ms in production, wait extra 35 ms before canceling.
    (5, 40),
    (100, 100),
    (1_000, 800),
    (10_000, 5000),
    (100_000, 20_000),  # The last value is the extra timeout for the longest queries.
])


@click.command()
@click.option('--time-factor', default=1.0, help='Run faster or slower than production')
@click.option('--progress-db', default='replay.sqlite', help='Where to store progress data')
@click.option('--allow-unsorted-files')
@click.option('--noninteractive')
@click.argument('files', nargs=-1)
def replay(time_factor, progress_db, allow_unsorted_files, noninteractive, files):
    global NON_INTERACTIVE
    NON_INTERACTIVE = noninteractive
    # One indirection for better debugging. Without it, py-bt in gdb didn't see line numbers.
    return _replay(time_factor, progress_db, allow_unsorted_files, files)


def _replay(time_factor, progress_db, allow_unsorted_files, files):
    if list(sorted(files)) != list(files) and not allow_unsorted_files:
        click.echo(files)
        raise ValueError('The log files are not sorted. If you are sure the order is correct, '
                         'pass --allow-unsorted-files')

    with sqlite3.connect(progress_db) as db:
        prepare_local_db(db)
        last_pos = get_position(db)

    # The database to which we'll send the queries from log files
    target_pool = psycopg2.pool.ThreadedConnectionPool(0, 200, fallback_application_name='db-replay')

    # The source of queries from log files. Skips already processed lines.
    queries = parse_files(files, last_pos)
    # Peek to find the time of the first query
    try:
        first_query = next(queries)
        click.echo(f'Starting at {first_query.file}:{first_query.line}')
        queries = itertools.chain([first_query], queries)
    except StopIteration:
        # Special case - empty file or already processed. We could return here
        # but we'll proceed to check that the concurrency logic works also with
        # empty queues.
        first_query = Query(files[0], 0, 0, 0, 0, 0, datetime.now(timezone.utc), '', 0)

    # In order to simulate the original concurrency, we'll need to wait occasionally.
    # `waiter` will pause the threads to not hammer the database too much. The `time_factor`
    # can speed up or slow down the herd. Setting time_factor really high makes it go
    # as fast as we can.
    #
    # The limit of the current program design seems to be at around 1000 queries per second.
    waiter = Waiter(time_factor, first_query.timestamp)

    # There is a dedicated thread writing to SQLite database because it's not thread-safe
    # to share the SQLite connection.
    progress_reporter_q = Queue(maxsize=10)
    progress_reporter = ProgressReporter(progress_db, progress_reporter_q, target_pool)
    progress_reporter.start()

    # There are two types of workers. `transactions` holds workers that simulate transactions
    # Whenever a transaction is open with BEGIN, the following queries will go to the same
    # worker.
    transactions = {}
    # The other type of worker is the one that processes queries without a transaction. They
    # share a queue and just execute the queries.
    independent_query_queue = Queue(maxsize=80)
    independent_query_executors = [
            EXECUTOR.submit(session_task, independent_query_queue, progress_reporter_q, target_pool, waiter, 'X.X')
            for _ in range(40)
    ]

    # An optimization - avoid waiting for transactional tasks when they're likely not finished yet.
    # Instead, put them into a queue and wait when there are a few tasks in the queue.
    finishing_tasks = Queue()

    try:
        for query in queries:
            if progress_reporter.finish:
                break

            if query.process_id in transactions:
                task, queue = transactions.get(query.process_id)
                queue.put(query)
                if query.sql in ['COMMIT', 'ROLLBACK']:
                    queue.put(POISON_PILL)
                    # This kind of task finishes when it depletes the queue.
                    finishing_tasks.put(task)
                    del transactions[query.process_id]
            elif query.sql == 'BEGIN':
                queue = Queue()
                task = EXECUTOR.submit(
                    session_task,
                    queue,
                    progress_reporter_q,
                    target_pool,
                    waiter,
                    f'{query.process_start:X}.{query.process_id:X}'.lower())
                transactions[query.process_id] = (task, queue)
            else:
                independent_query_queue.put(query)

            if finishing_tasks.qsize() > 100:
                finishing_tasks.get().result()
    finally:
        print('Waiting for queries to finish')
        # Wait for transaction tasks that we know should finish
        while not finishing_tasks.empty():
            finishing_tasks.get().result()

        if transactions:
            click.echo(f'There are {len(transactions)} unfinished transactions that will be rolled back.')
        for task, queue in transactions.values():
            queue.put(POISON_PILL)
            task.result()

        # Wait until all the independent queries have been processed by the tasks.
        independent_query_queue.join()

        # Now, just send signal to the workers that we're done.
        for i in range(len(independent_query_executors)):
            independent_query_queue.put(POISON_PILL)

        # And collect their exceptions if any.
        for task in independent_query_executors:
            task.result()

        # Once nobody is sending queries, wait for all to be safely reported.
        progress_reporter_q.join()


def parse_files(filenames, last_pos):
    """Return consecutive log records from files, optionally skipping """
    if last_pos:
        filenames = itertools.dropwhile(lambda x: x != last_pos[0], filenames)

    for filename in filenames:
        start_line = last_pos[1] + 1 if last_pos and filename == last_pos[0] else None
        yield from parse_lines(filename, start_line)


def parse_lines(filename, start_line) -> [Query]:
    query = None
    buffer = None
    last_txid = 0

    with open(filename) as f:
        for lineno, line in enumerate(f, start=1):
            if lineno % 1000 == 0: print(lineno, last_txid)
            if start_line and lineno < start_line:
                continue

            if not line.startswith('\t'):
                if query:
                    query.sql = buffer.getvalue()
                    query.last_line = lineno - 1
                    yield query
                    buffer = None
                    query = None

                if header := RE_HEADER.match(line):
                    if query_match := RE_QUERY.match(line[header.end():]):
                        query = Query(
                            file=filename,
                            line=lineno,
                            txid=int(query_match.group('txid')),
                            process_id=int(header.group('process_id'), base=16),
                            original_time_ms=float(query_match.group('duration_ms')),
                            process_start=int(header.group('process_start'), base=16),
                            timestamp=dateutil.parser.parse(header.group('timestamp')),
                        )
                        if query.txid:
                            last_txid = query.txid
                        buffer = io.StringIO()
                        buffer.write(query_match.group('sql'))
                        continue
            elif buffer:
                assert line[0] == "\t"
                buffer.write(line[1:])
                continue

            if not RE_EXCLUDE.search(line):
                print(line)


class ProgressReporter(Thread):
    def __init__(self, progress_dbname, queue: Queue, target_pool):
        super().__init__(name='Progress', daemon=True)
        self.progress_dbname = progress_dbname
        self.queue = queue
        self.target_pool = target_pool
        self.ignored_exceptions = {UniqueViolation, ForeignKeyViolation, InvalidCursorName, NoActiveSqlTransaction}
        self.finish = False
        self.query_cache = {}

    def run(self):
        current_filename = None
        current_file_id = None


        with sqlite3.connect(self.progress_dbname) as progress_db:
            while True:
                q: Query = self.queue.get()
                self.query_cache[q.my_process_id] = ActiveQuery(wait_event='?', query=q.sql)
                self.handle_interaction(q)

                if q.file != current_filename:
                    if current_filename is not None:
                        click.echo(f'Switching file to {q.file}')
                    c = progress_db.cursor()
                    c.execute('SELECT id FROM files WHERE filename = ?', (q.file,))
                    row = c.fetchone()
                    if row:
                        current_file_id = row[0]
                    else:
                        c.execute('INSERT INTO files (filename) VALUES (?)', (q.file,))
                        current_file_id = c.lastrowid
                    current_filename = q.file

                progress_db.execute('''
                INSERT INTO queries (txid, file, line_from, line_to, original_time_ms, my_time_ms, failure_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', [q.txid, current_file_id, q.line, q.last_line, q.original_time_ms, q.my_time_ms, str(q.failure)])
                progress_db.commit()
                self.queue.task_done()

    def should_interact(self, query: Query):
        if NON_INTERACTIVE or self.finish:
            return False
        return query.failure and type(query.failure) not in self.ignored_exceptions

    def handle_interaction(self, query):
        if not self.should_interact(query):
            return

        conn = self.target_pool.getconn()
        try:
            qi = QueryInteraction(conn, query,
                                  lambda: sum(1 for q in self.queue.queue if self.should_interact(q)),
                                  self.query_cache)
            if qi.interact() == QueryInteraction.SysExit:
                self.finish = True
        finally:
            self.target_pool.putconn(conn)

        self.ignored_exceptions.update(qi.ignored_exceptions)


def execute_query(query: Query, cursor, progress_queue, waiter):
    timeout = query.original_time_ms + EXTRA_TIMEOUT_MS.evaluate(query.original_time_ms)

    cursor.execute('SET statement_timeout = %s', (timeout,))
    cursor.execute('SET lock_timeout = %s', (timeout - 5,))
    waiter.wait_for_query(query.timestamp)

    start_time = time.monotonic()
    try:
        cursor.execute(query.sql)
    except Exception as e:
        query.failure = e
    finally:
        end_time = time.monotonic()
        query.my_time_ms = (end_time - start_time) * 1000
        progress_queue.put(query)


def session_task(queue: Queue, progress_queue: Queue, target_pool, waiter, session_id):
    target_db = target_pool.getconn()
    try:
        target_db.set_isolation_level(0)  # Explicit BEGIN needed for a transaction
        cursor = target_db.cursor()
        cursor.execute('SELECT pg_backend_pid()')
        my_pid, = cursor.fetchone()
        while True:
            query: Query = queue.get()
            try:
                if query is POISON_PILL:
                    cursor.close()
                    return
                query.my_process_id = my_pid
                try:
                    execute_query(query, cursor, progress_queue, waiter)
                except Exception:
                    cursor.execute('ROLLBACK')
            finally:
                queue.task_done()
    finally:
        target_pool.putconn(target_db)


def prepare_local_db(progress_db):
    progress_db.executescript('''
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY,
        filename VARCHAR NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS filename_unique ON files(filename);
    CREATE TABLE IF NOT EXISTS queries (
        id INTEGER PRIMARY KEY,
        txid VARCHAR NOT NULL,
        file INTEGER NOT NULL,
        line_from INT NOT NULL,
        line_to INT NOT NULL,
        original_time_ms FLOAT NOT NULL,
        my_time_ms FLOAT NOT NULL,
        failure_message TEXT,
        FOREIGN KEY (file) REFERENCES files (id)
    );
    ''')


def get_position(progress_db):
    c = progress_db.cursor()
    c.execute('SELECT filename, line_to FROM queries JOIN files ON file = files.id ORDER BY queries.id DESC LIMIT 1')
    pos = c.fetchone()
    return pos


if __name__ == '__main__':
    replay()
