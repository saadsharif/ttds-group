from __future__ import annotations
from copy import deepcopy
import re
from string import ascii_lowercase
from datrie import Trie
from search.posting import TermPosting
from search.segment import Segment
from utils.utils import print_progress
from models.search import SuggestionSearchSchema
import cProfile
from typing import List, Dict, Tuple


class Suggester:
    def __init__(self, trie=Trie(ascii_lowercase)):
        self._trie: Dict[str, Tuple[int, str]] = trie
        self._old_segment: Segment = None
        self._old_buffer: Dict[str, TermPosting] = None
        self._tokenizer = re.compile(r'\W+')

    def copy_buffer(self, segment: Segment):
        if segment != self._old_segment:
            self._old_segment = segment
            self._old_buffer = deepcopy(segment._buffer)

    def add_from_segment_buffer(self):
        if self._old_buffer is None:
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

    def suggest(self, search: SuggestionSearchSchema) -> List[str]:
        words = self._tokenizer.split(search.query.lower())
        fixed, search_word = words[:-1], words[-1]
        search_length = len(search_word)
        max_results = search.max_results if search.max_results else max(3, search_length)
        matches: List[Tuple[str, Tuple[int, str]]] = self._trie.items(search_word)
        matches.sort(key=lambda x: x[1][0], reverse=True)
        ret = list(map(lambda x: f"{' '.join(fixed)} {x[1][1].lower()}", matches))[:max_results]
        return ret

    def _add_term_to_trie(self, term, count, occurrence=""):
        if term in self._trie:
            (frequency, occurrence) = self._trie[term]
            self._trie[term] = (frequency + count, occurrence)
        else:
            if occurrence:
                self._trie[term] = (count, occurrence)

    def __getstate__(self):
        """Return state values to be pickled. Just a state file."""
        return self._trie

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""
        self._trie = state
        self._old_segment = None
        self._old_buffer = None
        self._tokenizer = re.compile(r'\W+')
