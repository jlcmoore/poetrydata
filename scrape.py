"""
PoetryFoundation Scraper. Edited from the original by Eric Li
Author: Jared Moore

Web scraper that scrapes a poets' poems from the PoetryFoundation
website into a sqlite database
"""

from __future__ import print_function
import HTMLParser
import re
import sqlite3
import urllib2

from bs4 import BeautifulSoup

from poem import Poem
from sql_util import *

POET_URL = "https://www.poetryfoundation.org/poets/%s#about"

POETS = "poets.txt"

INSERT_LINE = """INSERT INTO LINES (lid, pid, poem_line) VALUES (?, ?, ?);"""
INSERT_POEM = """INSERT INTO POEMS (poem_name, poet_id, num_lines%s) VALUES (?, ?, ?%s);"""
INSERT_POET_DEAD = """INSERT INTO POETS (poet_name, born, died) VALUES (?, ?, ?);"""
INSERT_POET_ALIVE = """INSERT INTO POETS (poet_name, born) VALUES (?, ?);"""
INSERT_POET = """INSERT INTO POETS (poet_name) VALUES (?);"""

SELECT_POET_ID = """SELECT PID FROM POETS WHERE poet_name = ?;"""
SELECT_POET_EXISTS = """SELECT * FROM POETS WHERE poet_name = ?;"""

SELECT_POEM_ID = """SELECT PID FROM POEMS WHERE poem_name = ? AND poet_id = ?;"""
SELECT_POEM_EXISTS = """SELECT * FROM POEMS WHERE poem_name = ? AND poet_id = ?;"""

WHITESPACE = '[ \t\n\r]+'

def main():
    """
    Main function for running from command line
    """
    conn = sqlite3.connect(DATABASE, isolation_level=None) # auto commit
    cursor = conn.cursor()
    create_tables(cursor)

    poet = raw_input('Enter a poet or RET to read poets.txt: ')
    if poet:
        add_poet_poems(poet, cursor)
    else:
        batch_run(cursor)

    conn.commit()
    cursor.close()

def batch_run(cursor):
    """
    Batch opens poets from in POETS adds their poems to cursor
    """
    with open(POETS, "r") as poet_file:
        poets = poet_file.readlines()
        for poet in poets:
            if not poet.startswith('#'):
                add_poet_poems(poet, cursor)

def poet_name_to_dashes(name):
    """
    Returns name with dashes instead of spaces
    """
    name = name.lower()
    return re.sub('[^a-z]+', '-', name)

def add_poet_poems(poet, cursor):
    """
    Adds all of the poems by poet to cursor
    """
    poet = poet.rstrip('\n')
    poet_dashes = poet_name_to_dashes(poet)
    print("poet is " + poet_dashes)

    poetSoup = find_poet_page(poet_dashes)

    if not poetSoup:
        print("Poet not found")
        return

    poet_years = find_poet_years(poetSoup)
    # todo: could also add in region
    poet_id = create_poet(poet, poet_years, cursor)

    poem_links = find_poem_links(poetSoup)

    if not poem_links:
        print("No poems found")
        return

    for poem in poem_links:
        poemURL = poem.get('href')
        poem = find_poem(poemURL)
        if poem:
            print("poem parsed")
            write_poem(poem, poet_id, cursor)

### Begin scraping functions

def find_poem(poemURL):
    """
    Given a poem url, attempts to parse the page. If sucessful, returns a
    Poem
    """
    try:
        poemPage = urllib2.urlopen(poemURL)
        poemSoup = BeautifulSoup(poemPage.read(), "html.parser")
        poemTitle = poemSoup.find('h1')

        if poemTitle:
            title = unescape_text(poemTitle.text, left=True, right=True)
            print("reading " + title)

            lines = find_poem_lines(poemSoup)
            translator = find_span_beginning_remove(poemSoup,
                                                    'c-txt_attribution',
                                                    'translated by ')
            source = find_span_beginning_remove(poemSoup, 'c-txt_note',
                                                'source: ')
            year = None
            if source:
                year = find_poem_year(source)
            return Poem(title=title, lines=lines, translator=translator,
                        source=source, year=year, url=poemURL)

    except urllib2.HTTPError, err:
        print("Poem not found, error " + str(err))
    except urllib2.URLError, err:
        print("Poem not found, error " + str(err))
    return None

def find_poet_years(soup):
    """
    Returns the years alive of the poet if found in soup
    """
    age_pattern = r'(b. )?\d{4}(-\d{4})?'
    poet_age_str = find_span_element(soup, 'c-txt_poetMeta', age_pattern)
    if poet_age_str:
        return re.findall(r'\d{4}', poet_age_str)
    return None

def find_poet_page(poet):
    """
    Returns the soup of the poet if it was found
    """
    url = POET_URL % poet

    print("opening " + url)
    try:
        page = urllib2.urlopen(url)
        soup = BeautifulSoup(page.read(), "html.parser")

        print("opened " + url)
        return soup
    except urllib2.HTTPError, err:
        print("Poet not found, error " + str(err))
    except urllib2.URLError, err:
        print("Poet not found, error " + str(err))
    return None

def find_poem_links(soup):
    """
    Finds all links to poems in soup and returns them
    """
    poems = soup.find_all('a', href=re.compile('.*/poems/[0-9]+/.*'))
    poems2 = soup.find_all('a', href=re.compile('.*/poem/.*'))
    poems.extend(poems2)
    return poems

def find_poem_year(source):
    """
    Returns the year of the poem if found in source or None
    """
    match = re.search(r'\(\d{4}\)', source)
    if match:
        return match.group(0)
    return None

def find_poem_lines(soup):
    """
    Returns the lines of the poem as parsed from soup
    """
    poemContent = soup.find('div', {'class':'o-poem'})
    poemLines = poemContent.findAll('div')

    lines = []
    for line in poemLines:
        text = unescape_text(line.text, left=True)
        cut = re.split(r'\n\r? ?', text)
        lines = lines + cut
    return lines

def find_span_beginning_remove(soup, span_class, pattern):
    """
    Given a soup a span_class and a patter, finds all examples of span_class that
    contain pattern and returns them with pattern omitted
    """
    result = find_span_element(soup, span_class, pattern)
    if result:
        return result[len(pattern):]
    return None

def find_span_element(soup, span_class, pattern):
    """
    Given a soup a span_class and a patter, finds all examples of span_class that
    contain pattern and returns them
    """
    spans = soup.find_all('span', {'class': span_class})
    for span in spans:
        text = unescape_text(span.text, left=True, right=True)
        if re.search(pattern, text, re.I):
            return text
    return None

def unescape_text(text, left=False, right=False):
    """
    Unescapes the html text and removes trailing whitespace if right and leading if left
    Returns unescaped text
    """
    parser = HTMLParser.HTMLParser()
    text = parser.unescape(text)
    if left:
        text = text.lstrip(WHITESPACE)
    if right:
        text = text.rstrip(WHITESPACE)
    return text

### Begin sql functions

def create_tables(cursor):
    """
    Sets up the tables on cursor if they don't already exist
    """
    cursor.execute(CREATE_POETS)
    cursor.execute(CREATE_POEMS)
    cursor.execute(CREATE_LINES)

def write_poem(poem, poet_id, cursor):
    """
    Writes poem to cursor
    """
    res = poem_exists(poem.title, poet_id, cursor)
    if res:
        print("poem already exists")
        return

    poem_id = create_poem(poem, poet_id, cursor)
    for lid in range(len(poem.lines)):
        line = poem.lines[lid]
        add_line(lid, poem_id, line, cursor)

def poet_exists(poet_name, cursor):
    """
    Returns true if poet_name exists in cursor
    """
    cursor.execute(SELECT_POET_EXISTS, (poet_name,)).fetchall()

def create_poet(poet_name, years, cursor):
    """
    Creates poet_name with years in cursor if not exists
    """
    if not poet_exists(poet_name, cursor):
        if years:
            born = years[0]
            died = None
            if 1 in years:
                died = years[1]
                cursor.execute(INSERT_POET_DEAD, (poet_name, born, died))
            else:
                cursor.execute(INSERT_POET_ALIVE, (poet_name, born))
        else:
            cursor.execute(INSERT_POET, (poet_name,))
    return cursor.execute(SELECT_POET_ID, (poet_name,)).fetchone()[0]

def create_poem(poem, poet_id, cursor):
    """
    Creates an entry for poem of poet_id in cursor
    """
    query_names = ""
    query_values = ""
    num_lines = len(poem.lines)
    params = (poem.title, poet_id, num_lines)

    # TODO: this can be factored out
    if poem.url:
        query_names = query_names + ", url"
        query_values = query_values + ", ?"
        params = params + (poem.url,)
    if poem.source:
        query_names = query_names + ", source"
        query_values = query_values + ", ?"
        params = params + (poem.url,)
    if poem.year:
        query_names = query_names + ", year"
        query_values = query_values + ", ?"
        params = params + (poem.url,)
    if poem.translator:
        query_names = query_names + ", translator"
        query_values = query_values + ", ?"
        params = params + (poem.url,)

    query = INSERT_POEM % (query_names, query_values)
    cursor.execute(query, params)

    return cursor.execute(SELECT_POEM_ID, (poem.title, poet_id)).fetchone()[0]

def poem_exists(poem_name, poet_id, cursor):
    """
    Returns True if poem_name and poet_id exist in cursor
    """
    return cursor.execute(SELECT_POEM_EXISTS, (poem_name, poet_id)).fetchall()

def add_line(lid, pid, line, cursor):
    """
    Adds line with id lid pid and value line to cursor
    """
    cursor.execute(INSERT_LINE, (lid, pid, line))

if __name__ == '__main__':
    main()
