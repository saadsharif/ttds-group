import math
from functools import total_ordering

import ujson as json


@total_ordering
class ScoredPosting:
    def __init__(self, posting, score=0):
        self.posting = posting
        self._score = score

    @property
    def score(self):
        return self._score

    @property
    def doc_id(self):
        return self.posting.doc_id

    def __iter__(self):
        return iter(self.posting)

    @property
    def is_stop_word(self):
        return False

    def __eq__(self, other):
        return self.posting.doc_id == other.posting.doc_id

    def __lt__(self, other):
        return self.posting.doc_id < other.posting.doc_id


MIN_LENGTH_FOR_SKIP_LIST = 3


@total_ordering
class Posting:

    def __init__(self, doc_id, stop_word=False):
        self.positions = []
        self.skips = []
        self._doc_id = doc_id
        self._stop_word = stop_word
        self.frequency = 0

    @property
    def is_stop_word(self):
        return self._stop_word

    @property
    def doc_id(self):
        return self._doc_id

    @property
    def posting(self):
        return self

    @property
    def score(self):
        # this saves us creating a scored posting for when we dont need one
        return 0

    def add_position(self, position):
        self.positions.append(position)
        self.frequency += 1

    def __iter__(self):
        return iter(self.positions)

    def _generate_skips(self):
        skips = []
        if len(self.positions) <= MIN_LENGTH_FOR_SKIP_LIST:
            # don't bother on skips on short lists
            return []
        skip_count = math.floor(math.sqrt(len(self.positions)))
        if skip_count > 0:
            pos_index = 0
            skip_period = math.floor(len(self.positions) / skip_count)
            # -1 because of list indexing starts with 0
            skip_index = skip_period - 1
            while pos_index < len(self.positions):
                if pos_index == skip_index:
                    skips.append([self.positions[pos_index], skip_index])
                    skip_index += skip_period
                pos_index += 1
        return skips

    def to_store_format(self, with_positions):
        doc = {
            "i": self._doc_id,
            "f": self.frequency
        }
        if with_positions:
            doc["p"] = self.positions
            skips = self._generate_skips()
            if len(skips) > 0:
                doc["s"] = skips
        return doc

    @staticmethod
    def from_store_format(data, with_positions):
        posting = Posting(data["i"])
        if with_positions:
            posting.positions = data["p"]
            posting.skips = data["s"] if "s" in data else []
        posting.frequency = data["f"]
        return posting

    def __eq__(self, other):
        return self.doc_id == other.doc_id

    def __lt__(self, other):
        return self.doc_id < other.doc_id


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
            # this should be in order for future merges self.postings.append(term_posting.postings)
            self.postings = self.postings + term_posting.postings

    def __iter__(self):
        return iter(self.postings)

    # TODO: These could be significantly improved. They determine how are postings are stored on disk -
    #  currently json encoded (cpu ands space wasteful)
    def to_store_format(self, with_positions=True):
        return json.dumps({
            "cf": self._collection_frequency,
            "p": [posting.to_store_format(with_positions) for posting in self.postings]
        })

    @staticmethod
    def from_store_format(data, with_positions=True):
        value = json.loads(data)
        termPosting = TermPosting(value["cf"])
        termPosting.postings = [Posting.from_store_format(posting, with_positions=with_positions) for posting in
                                value["p"]]
        return termPosting
