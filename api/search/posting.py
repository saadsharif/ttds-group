import math
from functools import total_ordering


def _generate_skips(positions):
    skips = []
    if len(positions) <= MIN_LENGTH_FOR_SKIP_LIST:
        # don't bother on skips on short lists
        return []
    skip_count = math.floor(math.sqrt(len(positions)))
    if skip_count > 0:
        pos_index = 0
        skip_period = math.floor(len(positions) / skip_count)
        # -1 because of list indexing starts with 0
        skip_index = skip_period - 1
        while pos_index < len(positions):
            if pos_index == skip_index:
                skips.append(f"{positions[pos_index]}-{skip_index}")
                skip_index += skip_period
            pos_index += 1
    return skips


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

    @property
    def positions(self):
        return self.posting.positions

    @property
    def skips(self):
        return self.posting.skips

    def __eq__(self, other):
        return self.posting.doc_id == other.posting.doc_id

    def __lt__(self, other):
        return self.posting.doc_id < other.posting.doc_id


MIN_LENGTH_FOR_SKIP_LIST = 3


def parse_skip(skip):
    for i in range(0, len(skip)):
        if skip[i] == "-":
            return int(skip[0:i]), int(skip[i+1:])


@total_ordering
class Posting:

    def __init__(self, doc_id):
        self.positions = []
        self.skips = []
        self._doc_id = doc_id
        self.frequency = 0

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

    def to_store_format(self, with_positions):
        store_rep = f"{self._doc_id};{self.frequency};"
        if with_positions:
            store_rep = f"{store_rep}{':'.join(str(pos) for pos in self.positions)};"
            skips = _generate_skips(self.positions)
            if len(skips) > 0:
                store_rep = f"{store_rep}{':'.join(skips)}"
        else:
            store_rep = f"{store_rep};"
        return store_rep


    @staticmethod
    def from_store_format(data, with_positions):
        components = data.split(";")
        posting = Posting(int(components[0]))
        posting.frequency = int(components[1])
        if with_positions:
            positions = components[2].split(":")
            posting.positions = [int(pos) for pos in positions]
            skips = components[3].split(":")
            posting.skips = [parse_skip(skip) for skip in skips if skip != ""]
        return posting

    def __eq__(self, other):
        return self.doc_id == other.doc_id

    def __lt__(self, other):
        return self.doc_id < other.doc_id


# encapsulates all the information about a term
class TermPosting:

    def __init__(self, collecting_frequency=0, stop_word=False):
        self._collection_frequency = collecting_frequency
        self.postings = []
        self.skips = []
        self._stop_word = stop_word

    def add_position(self, doc_id, position):
        # we assume single threaded index construction and that one doc is added at a time - we thus create
        # a new posting when the doc id changes
        if len(self.postings) == 0 or self.postings[-1].doc_id != doc_id:
            self.postings.append(Posting(doc_id))
        self.postings[-1].add_position(position)
        self._collection_frequency += 1

    @property
    def is_stop_word(self):
        return self._stop_word

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
    def add_term_info(self, term_posting, update_skips=False):
        if term_posting:
            self._collection_frequency += term_posting.collection_frequency
            # this should be in order for future merges self.postings.append(term_posting.postings)
            if update_skips:
                current_offset = len(self.postings)
                if current_offset == 0:
                    # optimization for first segement and when there is only one
                    self.skips = term_posting.skips
                else:
                    self.skips = self.skips + [[skip[0], skip[1]+current_offset] for skip in term_posting.skips]
            self.postings = self.postings + term_posting.postings

    def __iter__(self):
        return iter(self.postings)

    def to_store_format(self, with_positions=True):
        store_rep = "|".join([posting.to_store_format(with_positions) for posting in self.postings])
        skip_rep = ":".join(_generate_skips([posting.doc_id for posting in self.postings]))
        return f"{self.collection_frequency}|{skip_rep}|{store_rep}"

    @staticmethod
    def from_store_format(value, with_positions=True, with_skips=True):
        components = value.split("|")
        termPosting = TermPosting(int(components[0]))
        if with_skips:
            skips = components[1].split(":")
            termPosting.skips = [parse_skip(skip) for skip in skips if skip != ""]
        termPosting.postings = [Posting.from_store_format(posting, with_positions=with_positions) for posting in
                                components[2:]]
        return termPosting
