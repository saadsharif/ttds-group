import itertools
import math
import pickle
import uuid

from search.analyzer import Analyzer


class Index:

    def __init__(self, analyzer=Analyzer(), index_id=uuid.uuid4()):
        self.analyzer = analyzer
        # auto generate an index id if not provided
        self._index_id = index_id
        # for now we use a hash for the term dictionary - in future we may want to use a tree here
        self._index = {}
        # we maintain to generate doc ids but use use this for NOT queries
        self._current_doc_id = 1
        # mapping of external and internal id - used to ensure we always use an incremental doc id and check external
        # ids are unique - we may want to move this to uniqueness to an external check in future
        self._id_mappings = {}
        # store of document id to their terms - used for Psuedo Relevance Feedback
        self._doc_store = {}

    # saves the index to disk - for now just pickle down given the sizes
    def save(self, output_path):
        with open(output_path, 'wb') as index_file:
            pickle.dump(self.__dict__, index_file)

    # dumps to readable format
    def dump(self):
        for term, postings in self._index.items():
            print(f'{term}:{postings.doc_frequency}')
            for posting in postings:
                print(f'\t{posting.doc_id}: {",".join([str(pos) for pos in posting])}')

    # loads an index from disk. Warning - overwrites any existing data in this index.
    def load(self, input_path):
        with open(input_path, 'rb') as index_file:
            index = pickle.load(index_file)
            self.__dict__.update(index)

    # merges this index to another index and returns a new one
    def merge(self, index):
        # check not same id
        pass

    @property
    def current_id(self):
        return self._current_doc_id

    @property
    def number_of_docs(self):
        return self.current_id - 1

    # this is an append only operation. We generated a new internal id for the document and store a mapping between the
    # the two. The passed id here must be unique - no updates supported, but can be anything.
    def add_document(self, id, text):
        if id in self._id_mappings:
            raise Exception(f'{id} already exists in index {self._index_id}')
        self._id_mappings[self._current_doc_id] = id
        terms = self.analyzer.process(text)
        self._doc_store[id] = terms
        p = 0
        for term in terms:
            if term not in self._index:
                self._index[term] = TermPostings()
            self._index[term].add_position(self._current_doc_id, p)
            p += 1
        self._current_doc_id += 1

    def search(self, query, score, max_results):
        docs = Query(self).execute(query, score,  max_results)
        # for now we just map the ids but in future we could fetch the docs from a store e.g. disk
        for doc in docs:
            yield self._id_mappings[doc.doc_id], doc.score

    def get_term(self, term):
        if term in self._index:
            return self._index[term]
        return []

    def get_terms(self, doc_id):
        if doc_id in self._doc_store:
            return self._doc_store[doc_id]
        return []

    def get_top_terms(self, doc_ids, count, filter_numeric=True, filter_terms=[]):
        terms = []
        for doc_id in doc_ids:
            terms = terms + self.get_terms(doc_id)
        freq = {}
        for term in terms:
            if (filter_numeric and term.isnumeric()) or term in filter_terms:
                continue
            freq[term] = freq[term] + 1 if term in freq else 1
        scores = {}
        for term, term_freq in freq.items():
            doc_freq = self._index[term].doc_frequency
            scores[term] = term_freq * math.log10(self.number_of_docs/doc_freq)
        sorted_terms = {k: v for k, v in sorted(scores.items(), key=lambda item: item[1], reverse=True)}
        return dict(itertools.islice(sorted_terms.items(), count))
