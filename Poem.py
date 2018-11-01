class Poem(object):

    def __init__(self, title, lines, author=None, url=None, year=None,
                 translator=None, source=None):
        self.title = title
        self.author = author
        self.lines = lines
        self.url = url
        self.year = year
        self.translator = translator
        self.source = source
