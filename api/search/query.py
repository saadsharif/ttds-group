import heapq
import math
import re
from operator import itemgetter

from pyparsing import (
    Word,
    alphanums,
    nums,
    Keyword,
    Group,
    Forward,
    Suppress,
    OneOrMore,
    oneOf,
)

from search.posting import ScoredPosting, Posting, TermPosting, VectorPosting

PHRASE_TESTER = re.compile("\"(.*)\"")
PROXIMITY_TESTER = re.compile("#[1-9][0-9]*\(.*\)")


class Query:

    def __init__(self, index):
        self._index = index
        # false indicates positions will not be loaded answering queries - set to true if a phrase or proximity query is used
        self._with_positions = False
        self._with_posting_skips = False
        self._is_natural = False
        # this builds the grammar to parse expressions using pyparser - we support booleans, quotes, proximity
        # + parenthesis (TBC)
        or_operator = Forward()
        # we use : for a field delimiter and _ to concat terms e.g. bigrams
        term_operator = Group(Word(alphanums + "_:")).setResultsName('term')

        phrases_containable = Forward()
        phrases_containable << ((term_operator + phrases_containable) | term_operator)
        phrases_operator = (
                Group(Suppress('"') + phrases_containable + Suppress('"')).setResultsName(
                    'phrases'
                ) | term_operator
        )

        parenthesis_operator = (
                Group(Suppress('(') + or_operator + Suppress(')')).setResultsName(
                    'parenthesis'
                ) | phrases_operator
        )

        not_operator = Forward()
        not_operator << (
                Group(Suppress(Keyword('NOT')) + not_operator).setResultsName(
                    'not'
                ) | parenthesis_operator
        )

        and_operator = Forward()
        and_operator << (
                Group(
                    not_operator + Suppress(Keyword('AND')) + and_operator
                ).setResultsName('and')
                | Group(
            not_operator + OneOrMore(~oneOf('AND OR') + and_operator)
        ).setResultsName('natural')
                | not_operator
        )

        proximity_operator = (
                Group(Suppress('#') + Word(nums) + Suppress('(') + term_operator + Suppress(
                    ',') + term_operator + Suppress(')')).setResultsName(
                    'proximity'
                ) | and_operator
        )

        or_operator << (
                Group(
                    and_operator + Suppress(Keyword('OR')) + or_operator
                ).setResultsName('or')
                | proximity_operator
        )

        self._parser = or_operator.parseString

        self._methods = {
            'and': self._evaluate_and,
            'or': self._evaluate_or,
            'not': self._evaluate_not,
            'parenthesis': self._evaluate_parenthesis,
            'phrases': self._evaluate_phrases,
            'term': self._evaluate_term,
            'proximity': self._evaluate_proximity,
            'natural': self._evaluate_natural
        }

    def _evaluate_and(self, components, condition, score=False):
        self._with_posting_skips = True
        left_term_posting = self.evaluate(components[0], condition=condition, score=score)
        right_term_posting = self.evaluate(components[1], condition=condition, score=score)
        if left_term_posting.is_stop_word or right_term_posting.is_stop_word:
            return self._execute_or(left_term_posting, right_term_posting)
        intersection = self._execute_and(left_term_posting, right_term_posting, condition, score=score)
        self._with_posting_skips = False
        return intersection

    def _execute_and(self, left_term_posting, right_term_posting, condition, score=False):
        intersection = []
        li = 0
        ri = 0
        ls = 0
        rs = 0
        left_skips = left_term_posting.skips
        right_skips = right_term_posting.skips
        left_postings = left_term_posting.postings
        right_postings = right_term_posting.postings
        while li < len(left_postings) and ri < len(right_postings):
            left_posting = left_postings[li]
            right_posting = right_postings[ri]
            if left_posting.doc_id > right_posting.doc_id:
                if rs < len(right_skips):
                    right_skip = right_skips[rs]
                    if left_posting.doc_id >= right_skip[0]:
                        # can use the skip and advance the ri pointer
                        rs += 1
                        ri = right_skip[1]
                        continue
                ri += 1
            elif left_posting.doc_id < right_posting.doc_id:
                if ls < len(left_skips):
                    # we still have a skip to maybe use
                    left_skip = left_skips[ls]
                    if left_skip[0] <= right_posting.doc_id:
                        ls += 1
                        li = left_skip[1]
                        continue
                li += 1
            else:
                if condition(left_posting, right_posting):
                    if score:
                        intersection.append(ScoredPosting(left_posting, left_posting.score + right_posting.score))
                    else:
                        intersection.append(left_posting)
                li += 1
                ri += 1
        term_posting = TermPosting()
        term_posting.postings = intersection
        return term_posting

    def _evaluate_natural(self, components, condition, score=True):
        # TODO: Replace this with HNSW vector scoring
        return self._evaluate_or(components, condition, score=True)

    def evaluate(self, components, condition=lambda left, right, args={}: True, score=False):
        return self._methods[components.getName()](components, condition, score)

    def _extend(self, list, iter):
        item = next(iter, None)
        while item:
            list.append(item)
            item = next(iter, None)

    @staticmethod
    def _posting_merge(left, right):
        last = None
        for posting in heapq.merge(left, right):
            if last is None:
                last = posting
            elif posting != last:
                yield last
                last = posting
            else:
                yield ScoredPosting(posting, posting.score + last.score)
                last = None
        if last is not None:
            yield last

    def _execute_or(self, left_term_posting, right_term_posting):
        if left_term_posting.is_stop_word:
            left_term_posting = TermPosting()
        if right_term_posting.is_stop_word:
            right_term_posting = TermPosting()
        merged_term_posting = TermPosting()
        merged_term_posting.postings = list(
            self._posting_merge(left_term_posting.postings, right_term_posting.postings))
        return merged_term_posting

    def _evaluate_or(self, components, condition, score=False):
        left_term_posting = self.evaluate(components[0], score=score)
        right_term_posting = self.evaluate(components[1], score=score)
        return self._execute_or(left_term_posting, right_term_posting)

    def _evaluate_not(self, components, condition, score):
        not_docs = []
        right_term_posting = self.evaluate(components[0], score=score)
        if right_term_posting.is_stop_word:
            right = None
        else:
            right_side = iter(right_term_posting)
            right = next(right_side, None)
        doc_id = 1
        while doc_id < self._index.current_id:
            if not right or doc_id < right.doc_id:
                not_docs.append(ScoredPosting(Posting(doc_id), score=1))
            else:
                right = next(right_side, None)
            doc_id += 1
        term_posting = TermPosting()
        term_posting.postings = not_docs
        return term_posting

    def _phrase_match(self, left, right):
        left_positions = left.positions
        right_positions = right.positions
        li = 0
        ri = 0
        ls = 0
        rs = 0
        # skip pointers
        left_skips = left.skips
        right_skips = right.skips
        while li < len(left_positions) and ri < len(right_positions):
            left = left_positions[li]
            right = right_positions[ri]
            if left >= right:
                if rs < len(right_skips):
                    # we still have a skip to maybe use
                    right_skip = right_skips[rs]
                    if left >= right_skip[0]:
                        # can use the skip and advance the ri pointer
                        rs += 1
                        ri = right_skip[1]
                        continue
                ri += 1
            else:
                if left + 1 == right:
                    return True
                if ls < len(left_skips):
                    # we still have a skip to maybe use
                    left_skip = left_skips[ls]
                    if left_skip[0] < right:
                        ls += 1
                        li = left_skip[1]
                        continue
                li += 1
        return False

    def _evaluate_parenthesis(self, components, condition, score=False):
        return self.evaluate(components[0], condition, score=score)

    def _evaluate_phrases(self, components, condition, score):
        # first we perform an AND by re-writing the query
        self._with_positions = True
        and_query = self._parser(' AND '.join([component[0] for component in components]))
        # additional condition through lambda _phrase_match on verification step of and performs the phrase check
        phrase_response = self.evaluate(and_query[0], condition=self._phrase_match, score=score)
        self._with_positions = False
        return phrase_response

    def _proximity_match(self, left, right, distance):
        left_side = iter(left)
        right_side = iter(right)
        left = next(left_side, None)
        right = next(right_side, None)
        while True:
            dif = abs(left - right)
            if dif <= distance:
                return True
            if left < right:
                next_left = next(left_side, None)
                if next_left is not None:
                    left = next_left
                    continue
            if right < left:
                next_right = next(right_side, None)
                if next_right is not None:
                    right = next_right
                    continue
            return False

    def _evaluate_proximity(self, components, condition, score):
        self._with_positions = True
        distance = components[0]
        # first we perform an AND by re-writing the query
        and_query = self._parser(' AND '.join([component[0] for component in components[1:]]))
        # additional condition through lambda _phrase_match on verification step of and performs the phrase check
        check_proximity = lambda left, right: self._proximity_match(left, right, int(distance))
        proximity_response = self.evaluate(and_query[0], condition=check_proximity, score=score)
        self._with_positions = False
        return proximity_response

    def _evaluate_term(self, components, condition, score):
        # score the docs
        term = components[0]
        if ":" not in term:
            # : indicates a special lookup on a protected term
            term = self._index.analyzer.process_token(term)
        if term is None:
            # we have a stop word - a special case
            return TermPosting(stop_word=True)
        term_posting = self._index.get_term(term, with_positions=self._with_positions,
                                            with_skips=self._with_posting_skips)
        if not score:
            return term_posting

        scored_postings = []
        for doc_posting in term_posting:
            score = (1 + math.log10(doc_posting.frequency)) * \
                    math.log10(self._index.number_of_docs / term_posting.doc_frequency)
            scored_postings.append(ScoredPosting(doc_posting, score=score))
        scored_posting = TermPosting(collecting_frequency=term_posting.collection_frequency)
        scored_posting.postings = scored_postings
        scored_posting.skips = term_posting.skips
        return scored_posting

    def _get_facets(self, facets, docs):
        facet_values = {}
        for facet in facets:
            if self._index.has_doc_id(facet.field):
                facet_values[facet.field] = {}
                for doc in docs:
                    values = self._index.get_doc_values(facet.field, doc.doc_id)
                    for value in values:
                        if not value in facet_values[facet.field]:
                            facet_values[facet.field][value] = 1
                        else:
                            facet_values[facet.field][value] += 1
                facet_values[facet.field] = dict(
                    sorted(facet_values[facet.field].items(), key=itemgetter(1), reverse=True)[:facet.num_values])
        return facet_values

    def _is_natural_language(self, query_text):
        # single term queries are not NL - insufficient information
        terms = self._index.analyzer.tokenize(query_text)
        if len(terms) == 1:
            return False
        boolean = "NOT" in query_text or "AND" in query_text or "OR" in query_text
        if boolean:
            return False
        if re.match(PHRASE_TESTER, query_text) is not None:
            return False
        if re.match(PROXIMITY_TESTER, query_text) is not None:
            return False
        return True

    def execute(self, query, filters, score, max_results, offset, facets, use_hnsw=True, max_distance=0.8):
        filters = [f"{filter.field}:{'_'.join(self._index.analyzer.tokenize(filter.value))}" for filter in filters]
        filter_query = " AND ".join(filters)
        if use_hnsw and self._is_natural_language(query):
            print("Executing natural language search")
            ids, distances = self._index.find_closest_vectors(query)
            docs = []
            # could do a binary search here but we ultimately need to iterate all values anyway to produce
            # vector postings - we could remove the need for the vector postings object but needs a refactor - change
            # only if performance an issue
            for i in range(len(ids[0])):
                if distances[0][i] > max_distance:
                    break
                # invert the distance to score
                docs.append(VectorPosting(ids[0][i], 1 - distances[0][i]))
            if len(filters) > 0:
                parsed = self._parser(filter_query)
                filtered_docs = self.evaluate(parsed[0], score=score)
                vector_posting = TermPosting()
                # unfortunately need in doc id order for fast intersection - might be worth building a custom
                # intersect to avoid
                docs.sort(key=lambda p: p.doc_id)
                vector_posting.postings = docs
                # intersect filtered with hnsw
                docs = self._execute_and(vector_posting, filtered_docs, lambda left, right, args={}: True, score=False).postings
        else:
            query = f"{query} AND {filter_query}"
            parsed = self._parser(query)
            docs = self.evaluate(parsed[0], score=score).postings
        facet_values = {}
        if len(facets) > 0:
            facet_values = self._get_facets(facets, docs)
        # we would add pagination here
        if score and len(docs) > 0:
            # if we have an offset we need offset + max_results
            sorted_docs = heapq.nlargest(max_results + offset, docs, key=lambda doc: doc.score)
            return sorted_docs[offset:offset + max_results], facet_values, len(docs)
        return heapq.nsmallest(max_results, docs, key=lambda doc: doc.doc_id)[
               offset:offset + max_results], facet_values, len(docs)
