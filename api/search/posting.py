import json


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
    def __init__(self, doc_id, positions=[]):
        self._positions = positions
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

    def to_store_format(self):
        return {
            "id": self._doc_id,
            "pos": self._positions
        }

    @staticmethod
    def from_store_format(data):
        return Posting(data["id"], data["pod"])


class TermPostings:

    def __init__(self, collecting_frequency=0, postings=[]):
        self._collection_frequency = collecting_frequency
        self._postings = postings

    def add_position(self, doc_id, position):
        # we assume single threaded index construction and that one doc is added at a time - we thus create
        # a new posting when the doc id changes
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

    # TODO: These could be significantly improved. They determine how are postings are stored on disk -
    #  currently json encoded (cpu ands space wasteful)
    def to_store_format(self):
        return json.dumps({
            "cf": self._collection_frequency,
            "p": [posting.to_store_format() for posting in self._postings]
        })

    @staticmethod
    def from_store_format(data):
        value = json.loads(data)
        return TermPostings(value["cf"], [Posting.from_store_format(posting) for posting in value["p"]])
