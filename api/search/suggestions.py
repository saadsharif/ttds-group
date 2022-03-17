from __future__ import annotations
import re
from string import ascii_lowercase
from datrie import Trie
from search.posting import TermPosting
from search.segment import Segment
from search.utils import valid_term
from utils.utils import print_progress
from models.search import SuggestionSearchSchema
from typing import List, Dict, Tuple


class Suggester:
    def __init__(self, trie=Trie(ascii_lowercase)):
        self._trie: Dict[str, Tuple[int, str]] = trie
        self._tokenizer = re.compile(r'\W+')

    def add_segment(self, segment: Segment, reset_count=False):
        print(f"Building trie from segment {segment.segment_id}")
        k: str
        v: TermPosting
        i = 0
        n = segment.num_terms
        for k, v in segment.terms():
            if valid_term(k):
                self._add_term_to_trie(k, v.collection_frequency, occurrence=v.first_occurrence,
                                       reset_count=reset_count)
            i += 1
            print_progress(i, n, label=f"Updating trie with segment {segment.segment_id}")

    def suggest(self, search: SuggestionSearchSchema) -> List[str]:
        words = self._tokenizer.split(search.query.lower())
        fixed, search_word = words[:-1], words[-1]
        search_length = len(search_word)
        max_results = search.max_results if search.max_results else max(3, search_length)
        matches: List[Tuple[str, Tuple[int, str]]] = self._trie.items(search_word)
        matches.sort(key=lambda x: x[1][0], reverse=True)
        ret = list(map(lambda x: f"{' '.join(fixed)} {x[1][1].lower()}", matches))[:max_results]
        return ret

    def _add_term_to_trie(self, term, count, occurrence="", reset_count=False):
        if term in self._trie and not reset_count:
            (frequency, occurrence) = self._trie[term]
            self._trie[term] = (frequency + count, occurrence)
        else:
            if occurrence:
                self._trie[term] = (count, occurrence)
