import heapq

from search.segment import Segment
from search.utils import valid_term
from utils.utils import print_progress
from math import log10

MIN_TERM_FREQ = 1


class TermExpander:
    def __init__(self, analyzer, max_docs_per_term=10, max_terms_per_doc=3):
        self._max_docs_per_term = max_docs_per_term
        self._max_terms_per_doc = max_terms_per_doc
        self._analyzer = analyzer
        self._term_postings = {}
        self._doc_terms = {}

    def add_segment(self, segment: Segment):
        # iterate over segments, get top N docs for each term
        print(f"Building expansions from segment {segment.segment_id}")
        term_freq = {}
        i = 0
        n = segment.num_terms
        for term, posting in segment.postings_items():
            if valid_term(term) and posting.doc_frequency > MIN_TERM_FREQ:
                term_freq[term] = posting.doc_frequency
                top_docs = heapq.nlargest(self._max_docs_per_term, posting.postings, key=lambda p: p.frequency)
                if term not in self._term_postings:
                    self._term_postings[term] = top_docs
                else:
                    self._term_postings[term] = heapq.nlargest(self._max_docs_per_term,
                                                               top_docs + self._term_postings[term],
                                                               key=lambda p: p.frequency)
            i += 1
            print_progress(i, n, label=f"Updating terms with top docs {segment.segment_id}")
        num_docs = segment.number_of_documents
        i = 0
        n = len(self._term_postings)
        for term, postings in self._term_postings.items():
            for posting in postings:
                term_score = posting.frequency * log10(num_docs / term_freq[term])
                if posting.doc_id not in self._doc_terms:
                    # num is an approx - segment only
                    self._doc_terms[posting.doc_id] = [(term, term_score)]
                else:
                    self._doc_terms[posting.doc_id] = heapq.nlargest(self._max_terms_per_doc,
                                                                     self._doc_terms[posting.doc_id] + [
                                                                         (term, term_score)], key=lambda p: p[1])
            i += 1
            print_progress(i, n, label=f"Updating docs with top terms {segment.segment_id}")

    def expand_query(self, query, num_expansions=3):
        tokens = self._analyzer.process(query)
        expansions = []
        for token in tokens:
            if token in self._term_postings:
                postings = self._term_postings[token]
                for posting in postings:
                    expansions += self._doc_terms[posting.doc_id]
        expansions = list({expansion[0]: expansion for expansion in expansions}.values())
        return heapq.nlargest(num_expansions, expansions, key=lambda p: p[1])
