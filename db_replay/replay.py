import dataclasses
import io
import itertools
import re
import sqlite3
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from queue import Queue
from threading import Thread

import click
import dateutil.parser
import psycopg2.pool
from psycopg2 import DatabaseError
from psycopg2.errors import UniqueViolation, ForeignKeyViolation


@dataclasses.dataclass
class Query:
    file: str
    line: int
    txid: int
    process_id: int
    original_time_ms: float
    process_start: float
    timestamp: datetime
    failure_msg: str = None
    last_line: int = None
    my_time_ms: float = None
    seq: int = None
    sql: str = None


class Waiter:
    def __init__(self, time_factor: float, first_query: datetime):
        self.first_query = first_query
        self.time_factor = time_factor
        self.real_start_time = datetime.now(timezone.utc)
        self.last_reported = self.real_start_time
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
            if sleep_duration > 0.5:
                click.echo(f'Waiting for {sleep_duration:4.1f}s')
            time.sleep(sleep_duration)


POISON_PILL = object()
RE_HEADER = re.compile(r'^(?P<timestamp>.* CEST) \[\d+] (?P<process_start>[a-f0-9]+)\.(?P<process_id>[a-f0-9]+)')
RE_QUERY = re.compile(r' [^ ]+ (?P<txid>[0-9]+) LOG:  duration: (?P<duration_ms>[0-9.]+) ms  statement: (?P<sql>.*)$')
RE_EXCLUDE = re.compile(r'Connection reset by peer|archive-push|pushed WAL file|ERROR:|DETAIL:|STATEMENT:|^\t')
EXECUTOR = ThreadPoolExecutor()
PROGRESS_REPORT_QUEUE = Queue(maxsize=0)


@click.command()
@click.option('--time-factor', default=1.0, help='Run faster or slower than production')
@click.option('--progress-db', default='replay.sqlite', help='Where to store progress data')
@click.option('--allow-unsorted-files')
@click.argument('files', nargs=-1)
def replay(time_factor, progress_db, allow_unsorted_files, files):
    if list(sorted(files)) != list(files) and not allow_unsorted_files:
        click.echo(files)
        raise ValueError('The log files are not sorted. If you are sure the order is correct, '
                         'pass --allow-unsorted-files')

    with sqlite3.connect(progress_db) as db:
        prepare_local_db(db)
        last_pos = get_position(db)

    target_pool = psycopg2.pool.ThreadedConnectionPool(0, 200)

    queries = parse_files(files, last_pos)
    # Peek to find the time of the first query
    first_query = next(queries)
    click.echo(f'Starting at {first_query.file}:{first_query.line}')
    queries = itertools.chain([first_query], queries)

    waiter = Waiter(time_factor, first_query.timestamp)
    progress_reporter = ProgressReporter(progress_db)
    progress_reporter.start()

    # Simulate multiple workers querying the database. Each session (PostgreSQL process) is a single thread.
    tasks = {}

    for query in queries:
        existing = tasks.get(query.process_id)

        if existing and existing[2] != query.process_start:
            existing[1].queue.put(POISON_PILL)
            existing = None

        if existing:
            task, queue, process_start = existing
        else:
            queue = Queue(maxsize=10)
            task = EXECUTOR.submit(
                session_task,
                queue,
                PROGRESS_REPORT_QUEUE,
                target_pool,
                waiter,
                f'{query.process_start:X}.{query.process_id:X}'.lower())
            tasks[query.process_id] = (task, queue, query.process_start)

        queue.put(query)

    for task, queue, _ in tasks.values():
        queue.put(POISON_PILL)
        task.result()

    while not PROGRESS_REPORT_QUEUE.empty():
        click.echo('Waiting to save progress messages...')
        time.sleep(0.5)

    progress_reporter.setDaemon(True)


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
                buffer.write("' || E'\\n' || '")
                buffer.write(line[1:-1])
                continue

            if not RE_EXCLUDE.search(line):
                print(line)


class ProgressReporter(Thread):
    def __init__(self, progress_dbname):
        super().__init__(name='Progress')
        self.progress_dbname = progress_dbname

    def run(self):
        current_filename = None
        current_file_id = None
        with sqlite3.connect(self.progress_dbname) as progress_db:
            while True:
                q: Query = PROGRESS_REPORT_QUEUE.get()

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
                ''', [q.txid, current_file_id, q.line, q.last_line, q.original_time_ms, q.my_time_ms, q.failure_msg])
                progress_db.commit()


def session_task(queue: Queue, progress_queue: Queue, target_pool, waiter, session_id):
    print(f'Starting thread {session_id}')
    try:
        target_db = target_pool.getconn()
    except DatabaseError:
        traceback.print_exc()
        sys.exit(1)

    try:
        cursor = target_db.cursor()
        while True:
            query: Query = queue.get()
            if query is POISON_PILL:
                click.echo(f'Finishing {session_id}')
                cursor.close()
                return

            cursor.execute('SET statement_timeout = %s', (query.original_time_ms * 10 + 5,))
            waiter.wait_for_query(query.timestamp)

            start_time = time.monotonic()
            try:
                cursor.execute(query.sql)
            except Exception as e:
                if not isinstance(e, (UniqueViolation, ForeignKeyViolation)):
                    traceback.print_exc()
                query.failure_msg = str(e)
                target_db.rollback()
            end_time = time.monotonic()

            query.my_time_ms = (end_time - start_time) * 1000
            progress_queue.put(query)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
    finally:
        print('Finishing connection')
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
