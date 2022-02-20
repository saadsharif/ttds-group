import heapq
import math
import time
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

from search.posting import ScoredPosting, Posting


class Query:

    def __init__(self, index):
        self._index = index
        # this builds the grammar to parse expressions using pyparser - we support booleans, quotes, proximity
        # + parenthesis (TBC)
        or_operator = Forward()
        term_operator = Group(Word(alphanums)).setResultsName('term')

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
        intersection = []
        left_side = iter(self.evaluate(components[0], score=score))
        right_side = iter(self.evaluate(components[1], score=score))
        try:
            left_posting = next(left_side)
            right_posting = next(right_side)
            if (left_posting and left_posting.is_stop_word) or (right_posting and right_posting.is_stop_word):
                return self._evaluate_or(components, condition, score=score)
            while True:
                if left_posting.doc_id > right_posting.doc_id:
                    right_posting = next(right_side)
                elif left_posting.doc_id < right_posting.doc_id:
                    left_posting = next(left_side)
                else:
                    if condition(left_posting, right_posting):
                        if score:
                            intersection.append(ScoredPosting(left_posting,
                                                              left_posting.score + right_posting.score))
                        else:
                            intersection.append(left_posting)
                    right_posting = next(right_side)
                    left_posting = next(left_side)
        except StopIteration:
            pass
        return intersection

    def _evaluate_natural(self, components, condition, score=True):
        start = time.time()
        query_vector = self._index.get_vector(" ".join([" ".join(component) for component in components]))
        print(f"{time.time() - start}s for vector query generation")
        # TODO: Replace this with HNSW vector scoring
        return self._evaluate_or(components, condition, score=True)

    def evaluate(self, components, condition=lambda left, right, args={}: True, score=False):
        return self._methods[components.getName()](components, condition, score)

    def _extend(self, list, iter):
        item = next(iter, None)
        while item:
            list.append(item)
            item = next(iter, None)

    def _evaluate_or(self, components, condition, score=False):
        union = []
        left_side_postings = iter(self.evaluate(components[0], score=score))
        right_side_postings = iter(self.evaluate(components[1], score=score))
        left_posting = next(left_side_postings, None)
        right_posting = next(right_side_postings, None)
        if left_posting and left_posting.is_stop_word:
            left_posting = None
        if right_posting and right_posting.is_stop_word:
            right_posting = None

        while left_posting and right_posting:
            if left_posting.doc_id < right_posting.doc_id:
                union.append(left_posting)
                left_posting = next(left_side_postings, None)
            elif left_posting.doc_id > right_posting.doc_id:
                union.append(right_posting)
                right_posting = next(right_side_postings, None)
            else:
                # doesnt matter which postings we take - although we inherently drop one set of positions, from one
                # side these are only used for phrases currently
                if score:
                    # sum scores from both clauses e.g. terms
                    union.append(ScoredPosting(left_posting, left_posting.score + right_posting.score))
                else:
                    union.append(left_posting)
                right_posting = next(right_side_postings, None)
                left_posting = next(left_side_postings, None)
        if right_posting:
            union.append(right_posting)
            self._extend(union, right_side_postings)
        elif left_posting:
            union.append(left_posting)
            self._extend(union, left_side_postings)
        return union

    def _evaluate_not(self, components, condition, score):
        not_docs = []
        right_side = iter(self.evaluate(components[0], score=score))
        right = next(right_side, None)
        if right.is_stop_word:
            right = None
        doc_id = 1
        while doc_id < self._index.current_id:
            if not right or doc_id < right.doc_id:
                not_docs.append(ScoredPosting(Posting(doc_id), score=1))
            else:
                right = next(right_side, None)
            doc_id += 1
        return not_docs

    def _phrase_match(self, left, right):
        left_side = iter(left)
        right_side = iter(right)
        try:
            left = next(left_side)
            right = next(right_side)
            while True:
                if left >= right:
                    right = next(right_side)
                else:
                    if left + 1 == right:
                        return True
                    left = next(left_side)
        except StopIteration:
            pass
        return False

    def _evaluate_parenthesis(self, components, condition, score=False):
        raise NotImplemented

    def _evaluate_phrases(self, components, condition, score):
        # first we perform an AND by re-writing the query
        and_query = self._parser(' AND '.join([component[0] for component in components]))
        # additional condition through lambda _phrase_match on verification step of and performs the phrase check
        return self.evaluate(and_query[0], condition=self._phrase_match, score=score)

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
        distance = components[0]
        # first we perform an AND by re-writing the query
        and_query = self._parser(' AND '.join([component[0] for component in components[1:]]))
        # additional condition through lambda _phrase_match on verification step of and performs the phrase check
        check_proximity = lambda left, right: self._proximity_match(left, right, int(distance))
        return self.evaluate(and_query[0], condition=check_proximity, score=score)

    def _evaluate_term(self, components, condition, score):
        # score the docs
        scored_postings = []
        term = self._index.analyzer.process_token(components[0])
        if term is None:
            # we have a stop word - a special case - we use a doc id of 0 (this should never exist)
            return [Posting(0, stop_word=True)]
        term_postings = self._index.get_term(term)
        if not score:
            return term_postings

        for doc_posting in term_postings:
            score = (1 + math.log10(doc_posting.frequency)) * \
                    math.log10(self._index.number_of_docs / term_postings.doc_frequency)
            scored_postings.append(ScoredPosting(doc_posting, score=score))
        return scored_postings

    def _get_facets(self, facets, docs):
        facet_values = {}
        for facet in facets:
            if self._index.has_doc_id(facet.field):
                facet_values[facet.field] = {}
                for doc in docs:
                    values = self._index.get_doc_values(facet.field, doc.doc_id)
                    for value in values:
                        if value in facet_values[facet.field]:
                            facet_values[facet.field][value] = 1
                        else:
                            facet_values[facet.field][value] += 1
                facet_values[facet.field] = dict(
                    sorted(facet_values[facet.field].items(), key=itemgetter(1), reverse=True)[:facet.num_values])
        return facet_values

    def execute(self, query, score, max_results, offset, facets):
        parsed = self._parser(query)
        docs = self.evaluate(parsed[0], score=score)
        if len(docs) == 1 and docs[0].doc_id == 0:
            # a doc based on a stop word search
            return [], 0
        facet_values = {}
        if len(facets) > 0:
            facet_values = self._get_facets(facets, docs)
        # we would add pagination here
        if score:
            # if we have an offset we need offset + max_results
            return heapq.nlargest(max_results + offset, docs, key=lambda doc: doc.score)[
                   offset:offset + max_results], facet_values, len(docs)
        return heapq.nsmallest(max_results, docs, key=lambda doc: doc.doc_id)[offset:offset + max_results], facet_values, len(docs)
