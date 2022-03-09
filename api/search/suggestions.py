from __future__ import annotations

from copy import deepcopy
from string import ascii_lowercase

from datrie import Trie

from search.posting import TermPosting
from search.segment import Segment

from utils.utils import print_progress

from models.search import SuggestionSearchSchema

from typing import List, Set, Dict, Tuple, Optional

class Suggester:
    def __init__(self, trie=Trie(ascii_lowercase)):
        self._trie: Dict[str, Tuple[int, str]] = trie
        self._old_segment: Segment = None
        self._old_buffer: Dict[str, TermPosting] = None

    def copy_buffer(self, segment: Segment):
        if segment != self._old_segment:
            self._old_segment = segment
            self._old_buffer = deepcopy(segment._buffer)

    def suggest(self, search: SuggestionSearchSchema) -> List[str]:
        search_length = len(search.query)
        max_results = search.max_results if search.max_results else max(3, search_length)
        matches: List[Tuple[str, Tuple[int, str]]] = self._trie.items(search.query)
        matches.sort(key=lambda x: x[1][0], reverse=True)
        ret = list(map(lambda x: x[1][1], matches))
        return ret[:max_results]

    def add_from_segment_buffer(self):
        if not self._old_buffer:
            return
        print("Building trie and flushing segment")
        i = 0
        n = len(self._old_segment._buffer)
        k: str
        v: TermPosting
        for k, v in self._old_segment._buffer.items():
            count = v.collection_frequency
            if k in self._old_buffer:
                count -= self._old_buffer[k].collection_frequency
            self._add_term_to_trie(k, count, occurrence=v.first_occurrence)
            i += 1
            print_progress(i, n, label="Updating trie")
        self._old_segment = None
        self._old_buffer = None

    def _add_term_to_trie(self, term, count, occurrence=""):
        if term in self._trie:
            (frequency, occurrence) = self._trie[term]
            self._trie[term] = (frequency + count, occurrence)
        else:
            if occurrence:
                self._trie[term] = (count, occurrence)

    def _get_first_unstemmed_occurrence(self, index, stem, document=None):
        pos = index.get_term(stem).get_first()
        if not pos.positions:
            return None
        doc = index._get_document(str(pos.doc_id), [])
        if not doc: doc = document
        text = ' '.join(str(x) for x in doc.values())
        terms = [term for term in index.analyzer.tokenize(text) if term.lower() not in index.analyzer._stop_words]
        doc_value_terms = []
        for field in index._doc_value_fields:
            if field in doc:
                doc_value_terms += [f"{field}:{'_'.join(index.analyzer.tokenize(value))}" for value in doc[field]]
        terms = doc_value_terms + terms
        if pos.positions[0] > len(terms):
            return None
        return terms[pos.positions[0]]