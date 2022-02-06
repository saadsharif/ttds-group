import re

from stemming.porter2 import stem as porter_stem


class Analyzer:
    def __init__(self, stop_words=[], stem=True):
        self._stop_words = stop_words
        self._stem = stem

    def _tokenize(self, text):
        return re.split("\W+", text)

    def process_token(self, token):

        def filter_stop(token):
            if token in self._stop_words:
                return None
            return token

        def case_folder(token):
            return token.lower()

        token = case_folder(token)
        if filter_stop(token):
            if self._stem:
                return porter_stem(token)
            return token
        return None

    def _process_tokens(self, tokens):
        terms = []
        for token in tokens:
            term = self.process_token(token)
            if term:
                terms.append(term)
        return terms

    def process(self, text):
        tokens = self._tokenize(text)
        return self._process_tokens(tokens)