import random
from datetime import datetime

import psycopg2

from db_replay.interaction import QueryInteraction
from db_replay.replay import Query


def main():
    qi = QueryInteraction(psycopg2.connect(), Query(
        'f', 11, 0x125, 4878, 12.35, 12345, datetime.now(), failure=Exception('aaa'), sql='SELECT * FROM mytable',
        my_time_ms=155,
    ), lambda: random.randint(0, 5))
    qi.interact()


if __name__ == '__main__':
    main()
