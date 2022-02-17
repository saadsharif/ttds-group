import ujson as json
from heapq import merge

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

    @property
    def is_stop_word(self):
        return False


class Posting:
    def __init__(self, doc_id, stop_word=False):
        self.positions = []
        self._doc_id = doc_id
        self._stop_word = stop_word

    @property
    def is_stop_word(self):
        return self._stop_word

    @property
    def doc_id(self):
        return self._doc_id

    @property
    def score(self):
        # this saves us creating a scored posting for when we dont need one
        return 0

    @property
    def frequency(self):
        return len(self.positions)

    def add_position(self, position):
        self.positions.append(position)

    def __iter__(self):
        return iter(self.positions)

    def to_store_format(self):
        return {
            "id": self._doc_id,
            "pos": self.positions
        }

    @staticmethod
    def from_store_format(data):
        posting = Posting(data["id"])
        posting.positions = data["pos"]
        return posting


# encapsulates all the information about a term
class TermPosting:

    def __init__(self, collecting_frequency=0):
        self._collection_frequency = collecting_frequency
        self.postings = []

    def add_position(self, doc_id, position):
        # we assume single threaded index construction and that one doc is added at a time - we thus create
        # a new posting when the doc id changes
        if len(self.postings) == 0 or self.postings[-1].doc_id != doc_id:
            self.postings.append(Posting(doc_id))
        self.postings[-1].add_position(position)
        self._collection_frequency += 1

    @property
    def doc_frequency(self):
        return len(self.postings)

    @property
    def collection_frequency(self):
        return self._collection_frequency

    def get_first(self):
        if len(self.postings) > 0:
            return self.postings[0]
        return Posting(-1)

    # used to collate term information across many term postings
    def add_term_info(self, term_posting):
        if term_posting:
            self._collection_frequency += term_posting.collection_frequency
            # this should be in order for future merges
            self.postings = list(merge(self.postings, term_posting.postings, key=lambda posting: posting.doc_id))

    def __iter__(self):
        return iter(self.postings)

    # TODO: These could be significantly improved. They determine how are postings are stored on disk -
    #  currently json encoded (cpu ands space wasteful)
    def to_store_format(self):
        return json.dumps({
            "cf": self._collection_frequency,
            "p": [posting.to_store_format() for posting in self.postings]
        })

    @staticmethod
    def from_store_format(data):
        value = json.loads(data)
        termPosting = TermPosting(value["cf"])
        termPosting.postings = [Posting.from_store_format(posting) for posting in value["p"]]
        return termPosting
