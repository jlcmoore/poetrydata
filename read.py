"""
Functions for reading from poetry database
"""

from __future__ import print_function
import sqlite3
from sql_util import DATABASE

from Poem import Poem

MAX_LINES = 20
LINE_LENGTH = 32

SELECT_POEM_LINES = """SELECT poem_line FROM LINES WHERE pid = ?;"""
SELECT_POEMS_BASE = """SELECT PM.pid, PM.poem_name, PT.poet_name, PM.translator, PM.year,
                        PM.source, PM.url
                        FROM POEMS AS PM JOIN POETS AS PT ON PT.PID = PM.poet_id
                        JOIN LINES L ON L.pid = PM.pid
                        WHERE PM.num_lines <= ?"""
GROUP_BY_CHAR_COUNT = """GROUP BY PM.pid HAVING sum(LENGTH(L.poem_line)) <= ?"""
SELECT_POEMS_POET_BASE = SELECT_POEMS_BASE + """AND PT.poet_name = ? """
ORDER_RANDOM = """ORDER BY RANDOM() LIMIT 1"""

SELECT_RANDOM_POEM = SELECT_POEMS_BASE + GROUP_BY_CHAR_COUNT +  ORDER_RANDOM + ";"
SELECT_RANDOM_POEM_POET = SELECT_POEMS_POET_BASE + GROUP_BY_CHAR_COUNT + ORDER_RANDOM + ";"

def get_random_poem(author=None, max_lines=MAX_LINES, line_length=LINE_LENGTH):
    """
    Returns a random Poem from the DATABASE of max length max_lines and from
    author if given. Returns None if no poems found
    """
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    max_characters = max_lines * line_length

    result = None
    if author:
        result = cursor.execute(SELECT_RANDOM_POEM_POET, (max_lines, author, max_characters))
    else:
        result = cursor.execute(SELECT_RANDOM_POEM, (max_lines, max_characters))
    poem_array = result.fetchone()

    if not poem_array:
        print("query for poem failed")
        return None
    poem_id, title, author, translator, year, source, url = poem_array

    lines = []
    for row in cursor.execute(SELECT_POEM_LINES, (poem_id,)):
        lines.append(row[0])

    cursor.close()
    conn.commit()

    return Poem(title=title, author=author, lines=lines, translator=translator,
                year=year, source=source, url=url)
