DATABASE = "poems.db"

CREATE_POETS = """
CREATE TABLE IF NOT EXISTS POETS
       (pid INTEGER PRIMARY KEY,
       poet_name VARCHAR(124),
       born INTEGER,
       died INTEGER);
"""

CREATE_POEMS = """
CREATE TABLE IF NOT EXISTS POEMS
       (pid INTEGER PRIMARY KEY, 
       poem_name VARCHAR(124),
       poet_id REFERENCES POETS(pid),
       url VARCHAR(256),
       source VARCHAR(124),
       translator VARCHAR(124),
       year INTEGER,
       num_lines INTEGER);
"""

CREATE_LINES = """
CREATE TABLE IF NOT EXISTS LINES
       (lid INTEGER,
       pid INTEGER REFERENCES POEMS(pid),
       poem_line VARCHAR(256),
       PRIMARY KEY (lid, pid));
"""