import dataclasses
import textwrap
import traceback
from typing import Callable

import sqlparse
from prompt_toolkit import prompt, print_formatted_text as pf, HTML, PromptSession
from prompt_toolkit.completion import NestedCompleter
from prompt_toolkit.formatted_text import FormattedText
from psycopg2 import DatabaseError
from psycopg2.extensions import TRANSACTION_STATUS_IDLE
from terminaltables import SingleTable


@dataclasses.dataclass
class ActiveQuery:
    wait_event: str
    query: str


class QueryInteraction:
    class SysExit: pass

    def __init__(self, conn, query, waiting_queries_count: Callable, queries_snap=None):
        self.waiting_queries = waiting_queries_count
        self.conn = conn
        self.query = query
        self.ignored_exceptions = []
        self.queries_snap = queries_snap if queries_snap else {}
        self.session = PromptSession(bottom_toolbar=self._bottom_toolbar)

    def interact(self):
        self.command_e()
        self.command_q()
        self.show_locks()

        while True:
            try:
                reply = self.session.prompt(
                    '\n(l)ocks (sq <PID>) - snapshot query (q [PID={pid}]) query (e)xception (ex)plain (i)gnore\n> '
                    .format(pid=self.query.my_process_id),
                    completer=self._get_completer(),
                ).split()
                if reply:
                    command = getattr(self, 'command_' + reply[0], None)
                    if command:
                        command(*reply[1:])
            except KeyboardInterrupt:
                return self.SysExit
            except EOFError:
                break
            except (DatabaseError, TypeError) as e:
                print(e)

            if self.conn.info.transaction_status != TRANSACTION_STATUS_IDLE:
                self.conn.rollback()

    def _bottom_toolbar(self):
        return HTML('Errors in queue: {}').format(self.waiting_queries())

    def _get_completer(self):
        pids = {str(pid) for pid in self.queries_snap.keys()}
        return NestedCompleter.from_nested_dict({
            'l': None,
            'sq': pids,
            'q': pids,
            'e': None,
            'ex': None,
        })

    def command_e(self):
        pf(HTML('<b>{}</b>: {}').format(type(self.query.failure).__name__, self.query.failure))

    def show_query(self):
        formatted_sql = sqlparse.format(self.query.sql, reindent_aligned=True, wrap_after=80)
        formatted_sql = textwrap.indent(formatted_sql, '    ')
        slowdown = self.query.my_time_ms / self.query.original_time_ms if self.query.my_time_ms else '0'
        pf(HTML('Timing: original time <b>{orig:.1f}</b> ms '
                'vs <b>{my:.1f}</b> ms => {slowdown:.1f}x slower').format(
            orig=self.query.original_time_ms,
            my=self.query.my_time_ms,
            slowdown=slowdown,
        ))
        pf(HTML('Query:'))
        pf(FormattedText([('#606060', formatted_sql)]))

    def show_locks(self):
        sql = '''SELECT l.relation::regclass::text, l.tuple, l.mode, l.pid 
            FROM pg_locks l 
                JOIN pg_class c ON l.relation = c.oid 
                JOIN pg_namespace n ON (c.relnamespace = n.oid) 
            WHERE l.granted and c.relkind = 'r' AND n.nspname = 'public'
        '''
        with self.conn.cursor() as c:
            c.execute(sql)
            data = c.fetchall()
        if data:
            data = [['Table', 'Tuple', 'Mode', 'PID']] + data
            print(SingleTable(data, 'Locks').table)
        else:
            pf('No locks held')

    def command_l(self):
        self.show_locks()
        self.snapshot_queries()

    def command_q(self, pid=None):
        if pid is None:
            return self.show_query()

        with self.conn.cursor() as c:
            c.execute('SELECT wait_event, query FROM pg_stat_activity WHERE pid = %s', (pid,))
            data = c.fetchone()
        if data:
            pf(HTML('<b>{}</b> {}').format(*data))
        else:
            pf(HTML('No query for PID {}').format(pid))

    def command_sq(self, pid):
        data = self.queries_snap.get(int(pid))
        if data:
            pf(HTML('<b>{q.wait_event}</b> {q.query}').format(q=data))
        else:
            pf(HTML('No query for PID {}').format(pid))

    def snapshot_queries(self):
        with self.conn.cursor() as c:
            c.execute('SELECT pid, wait_event, query FROM pg_stat_activity')
            self.queries_snap = {row[0]: ActiveQuery(query=row[2], wait_event=row[1]) for row in c}

    def command_ex(self):
        with self.conn.cursor() as c:
            c.execute('EXPLAIN ' + self.query.sql)
            for row, in c:
                print(row)

    def command_i(self):
        self.ignored_exceptions.append(type(self.query.failure))
        pf(HTML('Will ignore <b>{}</b>').format(type(self.query.failure)))
