import dataclasses
import io
import itertools
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from queue import Queue
from re import Match
from threading import Thread
from typing import Dict, List

import click
import dateutil.parser
import psycopg2.pool


@dataclasses.dataclass
class Query:
    file: str
    line: int
    txid: int
    process_id: int
    original_time_ms: float
    process_start: float
    timestamp: datetime
    my_time_ms: float = None
    seq: int = None
    sql: str = None


class Waiter:
    def __init__(self, time_factor: float, first_query: datetime):
        self.first_query = first_query
        self.time_factor = time_factor
        self.real_start_time = datetime.now(timezone.utc)

    def wait_for_query(self, query_time: datetime):
        query_offset = query_time - self.first_query
        real_offset = datetime.now(timezone.utc) - self.real_start_time

        wanted_offset_s = query_offset.total_seconds() / self.time_factor
        sleep_duration = wanted_offset_s - real_offset.total_seconds()
        if sleep_duration > 0:
            time.sleep(sleep_duration)


POISON_PILL = object()
RE_HEADER = re.compile(r'^(?P<timestamp>.* CEST) \[\d+] (?P<process_start>[a-f0-9]+)\.(?P<process_id>[a-f0-9]+)')
RE_QUERY = re.compile(r' [^ ]+ (?P<txid>[0-9]+) LOG:  duration: (?P<duration_ms>[0-9.]+) ms  statement: (?P<sql>.*)$')
RE_EXCLUDE = re.compile(r'Connection reset by peer|archive-push|pushed WAL file|ERROR:|DETAIL:|STATEMENT:|^\t')
EXECUTOR = ThreadPoolExecutor()
PROGRESS_REPORT_QUEUE = Queue(maxsize=0)


@click.command()
@click.option('--start-txid', default=None, help='Start at transaction')
@click.option('--time-factor', default=1.0, help='Run faster or slower than production')
@click.option('--progress-db', default='replay.sqlite', help='Where to store progress data')
@click.option('--allow-unsorted-files')
@click.argument('files', nargs=-1)
def replay(start_txid, time_factor, progress_db, allow_unsorted_files, files):
    if list(sorted(files)) != list(files) and not allow_unsorted_files:
        click.echo(files)
        raise ValueError('The log files are not sorted. If you are sure the order is correct, '
                         'pass --allow-unsorted-files')

    with sqlite3.connect(progress_db) as db:
        prepare_local_db(db)
        if not start_txid:
            start_txid = get_last_txid(db) + 1
        else:
            start_txid = int(start_txid)

    target_pool = psycopg2.pool.ThreadedConnectionPool(0, 200)

    queries = parse_files(files, start_txid)
    # Peek to find the time of the first query
    first_query = next(queries)
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
            task, q, process_start = existing
            if task.done():
                task.exception()
        else:
            q = Queue()
            task = EXECUTOR.submit(session_task, q, PROGRESS_REPORT_QUEUE, target_pool, waiter)
            tasks[query.process_id] = (task, q, query.process_start)

        q.put(query)

    for task, q, _ in tasks.values():
        q.put(POISON_PILL)
        task.result()

    while not PROGRESS_REPORT_QUEUE.empty():
        click.echo('Waiting to save progress messages...')
        time.sleep(0.5)

    progress_reporter.setDaemon(True)


def parse_files(filenames, start_txid):
    transactions: Dict[int, List[Query]] = {}

    lines_iterator = (query for filename in filenames for query in parse_lines(filename))

    # Skip lines until we see txid
    for query in lines_iterator:
        if query.txid >= start_txid:
            transactions.setdefault(query.process_id, []).append(query)
            click.echo(click.style(f'Found transaction {t[-1].txid}', fg='green', bold=True))
            break

        t = transactions.setdefault(query.process_id, [])

        if t and query.sql == 'COMMIT' and query.txid == 0:
            # Discard old transactions
            transactions[query.process_id] = []
        if not t or query.txid == 0 or t[-1].txid == 0 or query.txid == t[-1].txid:
            t.append(query)
        else:
            transactions[query.process_id] = [query]

    for ts in transactions.values():
        yield from ts

    yield from lines_iterator


def parse_lines(filename) -> [Query]:
    query = None
    buffer = None
    last_txid = 0

    with open(filename) as f:
        for lineno, line in enumerate(f, start=1):
            if lineno % 1000 == 0: print(lineno, last_txid)
            if not line.startswith('\t'):
                if query:
                    query.sql = buffer.getvalue()
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
                        buffer = io.StringIO(query_match.group('sql'))
                        continue
            elif buffer:
                buffer.write(line[1:])
                continue

            if not RE_EXCLUDE.search(line):
                print(line)


class ProgressReporter(Thread):
    def __init__(self, progress_dbname):
        super().__init__(name='Progress')
        self.progress_dbname = progress_dbname

    def run(self):
        with sqlite3.connect(self.progress_dbname) as progress_db:
            while True:
                query: Query = PROGRESS_REPORT_QUEUE.get()
                progress_db.execute('''
                INSERT INTO queries (txid, file, line, original_time_ms, my_time_ms)
                VALUES (%s, %s, %s, %s, %s)
                ''', [query.txid, query.file, query.line, query.original_time_ms, query.my_time_ms])


def session_task(queue: Queue, progress_queue: Queue, target_pool, waiter):
    target_db = target_pool.getconn()
    try:
        query: Query = queue.get()
        if query is POISON_PILL:
            click.echo('Finishing')
            return

        waiter.wait_for_query(query.timestamp)

        start_time = time.monotonic()
        try:
            target_db.execute(query.sql)
        except Exception as e:
            print(e)
        end_time = time.monotonic()
        print('.')

        query.my_time_ms = (end_time - start_time) * 1000
        progress_queue.put(query)
    except Exception as e:
        print(e)
        raise
    finally:
        print('Finishing connection')
        target_pool.putconn(target_db)


def prepare_local_db(progress_db):
    progress_db.execute('''
    CREATE TABLE IF NOT EXISTS queries (
        txid VARCHAR NOT NULL,
        file VARCHAR NOT NULL,
        line INT NOT NULL,
        original_time_ms FLOAT NOT NULL,
        my_time_ms FLOAT NOT NULL,
        failure_message TEXT
    );
    ''')
    progress_db.execute('CREATE INDEX IF NOT EXISTS queries_txid ON queries (txid) WHERE txid > 0')


def get_last_txid(progress_db):
    c = progress_db.cursor()
    c.execute('SELECT MAX(txid) FROM queries WHERE txid > 0')
    max_txid, = c.fetchone()

    if not max_txid:
        raise ValueError('No transaction ID remembered. Can\'t continue. Specify --start-txid')
    return max_txid


if __name__ == '__main__':
    replay()
