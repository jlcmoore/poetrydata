"""
PoetryFoundation Scraper. Edited from the original by Eric Li

Web scraper that scrapes a poets' poems from the PoetryFoundation
website into a sqlite database
"""

from __future__ import print_function
from bs4 import BeautifulSoup
import urllib2
import re
import HTMLParser
import sqlite3
from sql_util import *
import pdb

from Poem import Poem

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
    with open(POETS, "r") as poet_file:
        poets = poet_file.readlines()
        for poet in poets:
            if not poet.startswith('#'):
                add_poet_poems(poet, cursor)

def poet_name_to_dashes(name):
    name = name.lower()
    return re.sub('[^a-z]+', '-', name)

def add_poet_poems(poet, cursor):
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
    age_pattern = r'(b. )?\d{4}(-\d{4})?'
    poet_age_str = find_span_element(soup, 'c-txt_poetMeta', age_pattern)
    if poet_age_str:
        return re.findall(r'\d{4}', poet_age_str)
    return None

def find_poet_page(poet):
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
    poems = soup.find_all('a', href=re.compile('.*/poems/[0-9]+/.*'))
    poems2 = soup.find_all('a', href=re.compile('.*/poem/.*'))
    poems.extend(poems2)
    return poems

def find_poem_year(source):
    match = re.search(r'\(\d{4}\)', source)
    if match:
        return match.group(0)
    return None

def find_poem_lines(soup):
    poemContent = soup.find('div', {'class':'o-poem'})
    poemLines = poemContent.findAll('div')

    lines = []
    for line in poemLines:
        text = unescape_text(line.text, left=True)
        cut = re.split(r'\n\r? ?', text)
        lines = lines + cut
    return lines

def find_span_beginning_remove(soup, span_class, pattern):
    result = find_span_element(soup, span_class, pattern)
    if result:
        return result[len(pattern):]
    return None

def find_span_element(soup, span_class, pattern):
    spans = soup.find_all('span', {'class': span_class})
    for span in spans:
        text = unescape_text(span.text, left=True, right=True)
        if re.search(pattern, text, re.I):
            return text
    return None

def unescape_text(text, left=False, right=False):
    parser = HTMLParser.HTMLParser()
    text = parser.unescape(text)
    if left:
        text = text.lstrip(WHITESPACE)
    if right:
        text = text.rstrip(WHITESPACE)
    return text

### Begin sql functions

def create_tables(cursor):
    cursor.execute(CREATE_POETS)
    cursor.execute(CREATE_POEMS)
    cursor.execute(CREATE_LINES)

def write_poem(poem, poet_id, cursor):
    res = poem_exists(poem.title, poet_id, cursor)
    if res:
        print("poem already exists")
        return

    poem_id = create_poem(poem, poet_id, cursor)
    for lid in range(len(poem.lines)):
        line = poem.lines[lid]
        add_line(lid, poem_id, line, cursor)

def poet_exists(poet_name, cursor):
    cursor.execute(SELECT_POET_EXISTS, (poet_name,)).fetchall()

def create_poet(poet_name, years, cursor):
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
    return cursor.execute(SELECT_POEM_EXISTS, (poem_name, poet_id)).fetchall()

def add_line(lid, pid, line, cursor):
    cursor.execute(INSERT_LINE, (lid, pid, line))

if __name__ == '__main__':
    main()
