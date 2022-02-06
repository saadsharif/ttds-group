import itertools
import math
import os
import pickle
import uuid
from bidict import bidict
from search.analyzer import Analyzer
from search.exception import IndexException
from search.models import Result
from search.posting import TermPostings
from search.query import Query
from search.store import DocumentStore


class Index:

    def __init__(self, storage_path, analyzer=Analyzer(), index_id=uuid.uuid4()):
        # location of index files
        self._storage_path = storage_path
        self.analyzer = analyzer
        # auto generate an index id if not provided
        self._index_id = index_id
        # for now we use a hash for the term dictionary - in future we may want to use a tree here
        self._index = {}
        # we maintain to generate doc ids but use use this for NOT queries
        self._current_doc_id = 1
        # mapping of external and internal id - used to ensure we always use an incremental doc id and check external
        # ids are unique - we may want to move this to uniqueness to an external check in future. For this reason,
        # the dict is bi-directional. internal->external, whilst inverse is external to internal.
        self._id_mappings = bidict()
        # store of document id to their terms - used for Psuedo Relevance Feedback
        self._doc_term_store = {}
        # document store so we can return the original docs - flag c open if it exists
        self._doc_store = DocumentStore.open(os.path.join(self._storage_path, 'docs.db'), 'c')
        pass

    def _get_db_path(self):
        return os.path.join(self._storage_path, 'index.idb')

    # saves the index to disk - for now just pickle down given the sizes
    def save(self):
        self._doc_store.sync()
        with open(self._get_db_path(), 'wb') as index_file:
            state = self.__dict__.copy()
            del state['_doc_store']
            pickle.dump(state, index_file)

    def load(self):
        if os.path.isfile(self._get_db_path()):
            with open(self._get_db_path(), 'rb') as index_file:
                index = pickle.load(index_file)
                self.__dict__.update(index)

    # closes the index
    def close(self):
        self.save()
        self._doc_store.close()

    # dumps to readable format
    def dump(self):
        for term, postings in self._index.items():
            print(f'{term}:{postings.doc_frequency}')
            for posting in postings:
                print(f'\t{posting.doc_id}: {",".join([str(pos) for pos in posting])}')

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
    def add_document(self, document):
        if document.id in self._id_mappings.inverse:
            raise IndexException(f'{document.id} already exists in index {self._index_id}')
        self._id_mappings[self._current_doc_id] = document.id
        # TODO: no separate indices per field currently - we might want to add this
        terms = self.analyzer.process_document(document)
        # disabling doc_term_store - to save memory
        # self.doc_term_store[document.id] = terms
        p = 0
        for term in terms:
            if term not in self._index:
                self._index[term] = TermPostings()
            self._index[term].add_position(self._current_doc_id, p)
            p += 1
        doc_id = self._current_doc_id
        # persist to the bd
        self._doc_store[str(doc_id)] = document.fields
        self._current_doc_id += 1
        return doc_id

    def search(self, query):
        docs, total = Query(self).execute(query.query, query.score, query.max_results)
        # for now we just map the ids but in future we could fetch the docs from a store e.g. disk
        return [Result(self._id_mappings[doc.doc_id], doc.score, fields=self._doc_store.get(str(doc.doc_id))) for doc in docs], total

    def get_term(self, term):
        if term in self._index:
            return self._index[term]
        return []

    def get_terms(self, doc_id):
        if doc_id in self._doc_term_store:
            return self._doc_term_store[doc_id]
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
            scores[term] = term_freq * math.log10(self.number_of_docs / doc_freq)
        sorted_terms = {k: v for k, v in sorted(scores.items(), key=lambda item: item[1], reverse=True)}
        return dict(itertools.islice(sorted_terms.items(), count))
