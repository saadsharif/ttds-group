
class ScoredPosting:
    def __init__(self, posting, score=0):
        self._posting = posting
        self._score = score

    @property
    def score(self):
        return self._score

    @property
    def doc_id(self):
        return self._posting.doc_id

    def __iter__(self):
        return iter(self._posting)


class Posting:
    def __init__(self, doc_id):
        self._positions = []
        self._doc_id = doc_id

    @property
    def doc_id(self):
        return self._doc_id

    @property
    def score(self):
        # this saves us creating a scored posting for when we dont need one
        return 0

    @property
    def frequency(self):
        return len(self._positions)

    def add_position(self, position):
        self._positions.append(position)

    def __iter__(self):
        return iter(self._positions)


class TermPostings:

    def __init__(self):
        self._collection_frequency = 0
        self._postings = []

    def add_position(self, doc_id, position):
        # we assume single threaded index construction and that one doc is added at a time
        if len(self._postings) == 0 or self._postings[-1].doc_id != doc_id:
            self._postings.append(Posting(doc_id))
        self._postings[-1].add_position(position)
        self._collection_frequency += 1

    @property
    def doc_frequency(self):
        return len(self._postings)

    @property
    def collection_frequency(self):
        return self._collection_frequency

    def get_first(self):
        if len(self._postings) > 0:
            return self._postings[0]
        return Posting(-1)

    def __iter__(self):
        return iter(self._postings)
