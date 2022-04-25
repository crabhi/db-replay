import asyncio
import dataclasses
import io
import re
import time
from asyncio import Queue
from collections import namedtuple
from re import Match
from typing import Dict, List

import aiofiles as aiofiles
import aiopg
import aiosqlite
import click


POISON_PILL = object()

@dataclasses.dataclass
class Query:
    file: str
    line: int
    txid: int
    process_id: int
    original_time_ms: float
    process_start: float
    seq: int = None
    sql: str = None

RE_HEADER = re.compile(r'^(?P<timestamp>.* CEST) \[\d+] (?P<process_start>[a-f0-9]+)\.(?P<process_id>[a-f0-9]+)')
RE_QUERY = re.compile(r' [^ ]+ (?P<txid>[0-9]+) LOG:  duration: (?P<duration_ms>[0-9.]+) ms  statement: (?P<sql>.+)$')


@click.command()
@click.option('--start-txid', default=None, help='Start at transaction')
@click.option('--time-factor', default=1, help='Run faster or slower than production')
@click.option('--progress-db', default='replay.sqlite', help='Where to store progress data')
@click.option('--allow-unsorted-files')
@click.argument('files', nargs=-1)
def replay(start_txid, time_factor, progress_dbname, allow_unsorted_files, files):
    if list(sorted(files)) != files and not allow_unsorted_files:
        raise ValueError('The log files are not sorted. If you are sure the order is correct, '
                         'pass --allow-unsorted-files')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(_replay(start_txid, time_factor, progress_dbname, files))


async def _replay(start_txid, time_factor, progress_dbname, files):
    tasks = {}

    async with aiosqlite.connect(progress_dbname) as progress_db, aiopg.create_pool('') as target_pool:
        await prepare_local_db(progress_db)
        if not start_txid:
            start_txid = (await get_last_txid(progress_db)) + 1

        for filename in files:
            async for query in parse_lines(filename):
                if query.txid < start_txid:
                    continue

                existing = tasks.get(query.process_id)

                if existing[1].process_start != query.process_start:
                    await existing[1].queue.put(POISON_PILL)
                    existing = None

                if existing:
                    task, executable = existing
                else:
                    target_db = await target_pool.acquire()
                    executable = SessionTask(Queue(maxsize=2), progress_db, target_db, query.process_start)
                    task = asyncio.create_task(executable.run())
                    tasks[query.process_id] = (task, executable)

                await executable.queue.put(query)


async def parse_files(filenames, start_txid):
    transactions: Dict[int, List[Query]] = {}

    # Skip lines until we see txid
    filenames_it = iter(filenames)
    for filename in filenames_it:
        lines_it = iter(parse_lines(filename))
        async for query in lines_it:
            t = transactions.setdefault(query.process_id, [])

            if not t or query.txid == 0 or t[-1].txid == 0 or query.txid == t[-1].txid:
                t.append(query)
            else:
                if t[-1].txid >= start_txid:
                    for q in t:
                        yield q
                    break
                transactions[query.process_id] = [query]

    # Finish current file
    async for query in lines_it:
        yield query

    # And continue with other files
    for filename in filenames_it:
        async for query in parse_lines(filename):
            yield query


async def parse_lines(filename) -> [Query]:
    query = None
    buffer = None

    async with aiofiles.open(filename) as f:
        line = 0
        async for line in f:
            line += 1
            if line.startswith('\t'):
                buffer.write(line[1:])
            else:
                if query:
                    query.sql = buffer.getvalue()
                    yield query
                    buffer = None
                    query = None

                header: Match = RE_HEADER.match(line)
                if query_match := RE_QUERY.match(line[header.endpos + 1:]):
                    query = Query(
                        file=filename,
                        line=line,
                        txid=int(query_match.group('txid')),
                        process_id=int(header.group('process_id'), base=16),
                        original_time_ms=float(query_match.group('duration_ms')),
                        process_start=int(header.group('process_start'), base=16),
                    )
                    buffer = io.StringIO()


class SessionTask:
    def __init__(self, queue, progress_db, target_db, process_start):
        self.process_start = process_start
        self.queue = queue
        self.target_db = target_db
        self.progress_db = progress_db

    async def run(self):
        query: Query = await self.queue.get()
        if query is POISON_PILL:
            return

        start_time = time.monotonic()
        async with self.target_db.execute(query.sql):
            pass
        end_time = time.monotonic()

        await self.progress_db.execute('''
            INSERT INTO queries (txid, seq, file, line, original_time_ms, my_time_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
            ''', [query.txid, query.seq, query.file, query.line, query.original_time_ms,
            (end_time - start_time) * 1000])


async def prepare_local_db(progress_db):
    await progress_db.execute('''
    CREATE TABLE IF NOT EXISTS queries (
        txid VARCHAR NOT NULL,
        seq INT NOT NULL,
        file VARCHAR NOT NULL,
        line INT NOT NULL,
        original_time_ms FLOAT NOT NULL,
        my_time_ms FLOAT NOT NULL,
        failure_message TEXT,
        PRIMARY KEY (txid, seq)
    )
    ''')


async def get_last_txid(progress_db):

    async with progress_db.execute('SELECT MAX(txid) FROM queries') as c:
        max_txid, = await c.fetchone()

    if not max_txid:
        raise ValueError('No transaction ID remembered. Can\'t continue. Specify --start-txid')
    return max_txid




if __name__ == '__main__':
    replay()
