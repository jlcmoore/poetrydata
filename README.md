# poetrydata
Scraper for Poetry Foundation Website and poem data utils

*Usage*
Simply run the scrape.py with no arguments and it will prompt you for a poet. After you enter the poet's name, if it is a valid poet on the site, the program will proceed to download all of the poet's works into a sqlite database. If you do not enter a poet, it will read from poets.txt to batch download all of their poems.

There is little error handling here and the url creation is not sophisicated.