import heapq
from itertools import takewhile

from search.segment import Segment
from search.utils import valid_term
from utils.utils import print_progress
from math import log10, log2, pow

MIN_TERM_FREQ = 1


def compute_mi(n00, n10, n01, n11):
    n = n00 + n10 + n01 + n11
    n1_ = n11 + n10
    n_1 = n11 + n01
    n0_ = n01 + n00
    n_0 = n10 + n00
    mi = 0
    if n11 > 0:
        mi += (n11 / n * log2((n * n11) / (n1_ * n_1)))
    if n01 > 0:
        mi += (n01 / n * log2((n * n01) / (n0_ * n_1)))
    if n10 > 0:
        mi += (n10 / n * log2((n * n10) / (n1_ * n_0)))
    if n00 > 0:
        mi += (n00 / n * log2((n * n00) / (n0_ * n_0)))
    return mi


def compute_ci_sq(n00, n10, n01, n11):
    numerator = ((n11 + n10 + n01 + n00) * pow((n11 * n00) - (n10 * n01), 2))
    denominator = (n11 + n01) * (n11 + n10) * (n10 + n00) * (n01 + n00)
    return numerator / denominator


class TermExpander:
    def __init__(self, analyzer, max_docs_per_term=1000, max_terms_per_doc=5):
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
        num_docs = segment.number_of_documents
        for term, term_posting in segment.postings_items():
            doc_frequency = term_posting.doc_frequency
            if valid_term(term) and doc_frequency > MIN_TERM_FREQ:
                # for sampling we use the docs where the term appears most frequently
                top_postings = heapq.nlargest(self._max_docs_per_term, term_posting.postings, key=lambda p: p.frequency)
                self._term_postings[term] = top_postings
                for posting in top_postings:
                    #term_score = posting.frequency * log10(num_docs / doc_frequency)
                    n00 = num_docs - doc_frequency
                    n10 = doc_frequency - len(top_postings)
                    n11 = len(top_postings)
                    n01 = 0
                    term_score = compute_mi(n00, n10, n01, n11)
                    if posting.doc_id not in self._doc_terms:
                        self._doc_terms[posting.doc_id] = [(term, term_score)]
                    else:
                        self._doc_terms[posting.doc_id] = heapq.nlargest(self._max_terms_per_doc,
                                                                         self._doc_terms[posting.doc_id] + [
                                                                             (term, term_score)], key=lambda p: p[1])
            i += 1
            print_progress(i, n, label=f"Updating terms with top docs {segment.segment_id}")

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
