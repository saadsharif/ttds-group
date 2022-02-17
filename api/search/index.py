import os
import pickle
import sys
import time
import uuid
from bidict import bidict
from search.analyzer import Analyzer
from search.bert import BERTModule
from search.exception import IndexException, SearchException
from search.lock import ReadWriteLock
from search.models import Result
from search.posting import TermPosting
from search.query import Query
from search.segment import Segment, _create_segment_id
from search.store import DocumentStore


class Index:

    def __init__(self, storage_path, analyzer=Analyzer(), doc_value_fields=[], index_id=uuid.uuid4()):
        # location of index files
        self._storage_path = storage_path
        self.analyzer = analyzer
        # auto generate an index id if not provided
        self._index_id = index_id
        # we currently just keep a list of segments and assume terms are in all segments - large segments (1000+docs)
        # should ensure this. Segments held in order of creation, latest first. When this is pickled its lightweight as
        # underlying data is not stored
        self._segments = []
        # we maintain to generate doc ids but use use this for NOT queries
        self._current_doc_id = 1
        # mapping of external and internal id - used to ensure we always use an incremental doc id and check external
        # ids are unique - we may want to move this to uniqueness to an external check in the future. For this reason,
        # the dict is bi-directional. internal->external, whilst inverse is external to internal.
        self._id_mappings = bidict()
        # document store so we can return the original docs - flag c open if it exists
        self._doc_store = DocumentStore.open(os.path.join(self._storage_path, 'docs.db'), 'c')
        # used to ensure single threaded indexing
        self._write_lock = ReadWriteLock()
        # bert model for vectors
        self._vector_model = BERTModule()
        # facet fields
        self._doc_value_fields = doc_value_fields
        # merge lock - only one merge at once
        self._merge_lock = ReadWriteLock()

    def _get_db_path(self):
        return os.path.join(self._storage_path, 'index.idb')

    # saves the index to disk - for now just pickle down given the sizes
    def save(self):
        # we need to lock as we shouldn't index during flushing or vise versa
        self._write_lock.acquire_write()
        print(f"Syncing document store...", end="")
        self._doc_store.sync()
        print("OK")
        with open(self._get_db_path(), 'wb') as index_file:
            print(f"Saving state file to {self._get_db_path()}...", end="")
            state = self.__dict__.copy()
            # we don't store the doc store
            del state['_doc_store']
            del state['_write_lock']
            del state['_merge_lock']
            del state['_vector_model']
            pickle.dump(state, index_file)
            print("OK")
        if len(self._segments) > 0:
            # flush the last segment if we need to
            most_recent = self._segments[-1]
            if not most_recent.is_flushed():
                print(f"Flushing last segment...", end="")
                most_recent.flush()
                print("OK")
        self._write_lock.release_write()

    def load(self):
        self._write_lock.acquire_write()
        if os.path.isfile(self._get_db_path()):
            with open(self._get_db_path(), 'rb') as index_file:
                print("Loading index...")
                index = pickle.load(index_file)
                self.__dict__.update(index)
                print("Index Loaded")
        self._write_lock.release_write()

    # closes the index
    def close(self):
        self.save()
        print(f"Closing all segments...", end="")
        for segment in self._segments:
            segment.close()
        print("OK")
        self._doc_store.close()

    # dumps to readable format
    def dump(self):
        for segment in self._segments:
            # TODO: Allow this to iterate
            for term, postings in segment.items():
                print(f'{term}:{postings.doc_frequency}')
                for posting in postings:
                    print(f'\t{posting.doc_id}: {",".join([str(pos) for pos in posting])}')

    # theoretically merges segments together to avoid too many files -
    # really an optimisation if we needed as non-trivial
    def merge(self, index):
        # check not same id
        pass

    @property
    def current_id(self):
        return self._current_doc_id

    @property
    def number_of_docs(self):
        return self.current_id - 1

    # IMPORTANT: This is not thread safe! it is for use in this class only within methods which lock
    def __get_writeable_segment(self):
        if len(self._segments) == 0:
            # starting case - create one with new id
            self._segments = [Segment(_create_segment_id(), self._storage_path, self._doc_value_fields)]
            return self._segments[-1]
        most_recent = self._segments[-1]
        if most_recent.is_flushed():
            # segment has been flushed, create a new one
            self._segments.append(Segment(_create_segment_id(), self._storage_path, self._doc_value_fields))
            return self._segments[-1]
        if not most_recent.has_buffer_capacity():
            # segment is open but has no capacity so flush
            most_recent.flush()
            # insert new
            self._segments.append(Segment(_create_segment_id(), self._storage_path, self._doc_value_fields))
        return self._segments[-1]

    # this is an append only operation. We generated a new internal id for the document and store a mapping between the
    # the two. The passed id here must be unique - no updates supported, but can be anything.
    def add_document(self, document):
        # enforce single threaded indexing
        self._write_lock.acquire_write()
        if document.id in self._id_mappings.inverse:
            self._write_lock.release_write()
            raise IndexException(f'{document.id} already exists in index {self._index_id}')
        try:
            self._id_mappings[self._current_doc_id] = document.id
            # TODO: no separate indices per field currently - we might want to add this
            terms = self.analyzer.process_document(document)
            self.__get_writeable_segment().add_document(self._current_doc_id, terms)
            # persist to the bd
            self._doc_store[str(self._current_doc_id)] = document.fields
            doc_id = self._current_doc_id
            self._current_doc_id += 1
            self._write_lock.release_write()
        except Exception as e:
            self._write_lock.release_write()
            raise IndexException(f"Unexpected exception during indexing - {e}")
        return document.id, doc_id

    # this is more efficient than single document addition
    def add_documents(self, documents):
        # enforce single threaded indexing
        failures = []
        doc_ids = []
        self._write_lock.acquire_write()
        try:
            # check all the ids
            docs_to_index = []
            for document in documents:
                if document.id in self._id_mappings.inverse:
                    failures.append(f'{document.id} already exists in index {self._index_id}')
                else:
                    docs_to_index.append(document)
            # this could be more efficient - i.e. we could optimise bulk additions - less locking and large write chunks
            doc_batch = {}
            for document in docs_to_index:
                self._id_mappings[self._current_doc_id] = document.id
                terms = self.analyzer.process_document(document)
                self.__get_writeable_segment().add_document(self._current_doc_id, terms)
                doc_ids.append((document.id, self._current_doc_id))
                doc_batch[str(self._current_doc_id)] = document.fields
                self._current_doc_id += 1
            # persists the batch to the db
            self._doc_store.update(doc_batch)
            self._write_lock.release_write()
        except Exception as e:
            self._write_lock.release_write()
            raise IndexException(f"Unexpected exception during indexing - {e}")
        return doc_ids, failures

    def _get_document(self, id, fields):
        doc = self._doc_store.get(str(id))
        return {field: doc[field] for field in fields if field in doc}

    def search(self, query):
        try:
            docs, total = Query(self).execute(query.query, query.score, query.max_results)
            fields = set(query.fields)
            return [Result(self._id_mappings[doc.doc_id], doc.score, fields=self._get_document(str(doc.doc_id), fields))
                    for
                    doc in
                    docs], total
        except Exception as e:
            raise SearchException(f"Unexpected exception during indexing - {e}")

    def get_term(self, term):
        # get all the matching docs in all the segments - currently we assume the term is in every segment
        # this should be improved e.g. using a bloom filter or some bit set, inside the segment though -i.e.
        # it should return [] quickly
        term_posting = TermPosting()
        if term:
            for segment in self._segments:
                termPosting = segment.get_term(term)
                if termPosting:
                    term_posting.add_term_info(termPosting)
        return term_posting

    def get_vector(self, query):
        return self._vector_model.embedding(query)

    # merges two segments (together and flushed) to produce a larger segment - with the aim of speeding up searches
    def optimize(self):
        self._merge_lock.acquire_write()
        num_segments = len(self._segments)
        if num_segments < 2:
            # we need at least 2
            print(f"Insufficient segments to merge - only {num_segments}")
            self._merge_lock.release_write()
            return num_segments, num_segments
        # find candidate segments
        # must be flushed
        smallest_combined_size = sys.maxsize
        smallest_pos = -1
        s = 0
        while s < num_segments - 1:
            if self._segments[s].is_flushed() and self._segments[s + 1].is_flushed():
                combined_size = self._segments[s].number_of_documents + self._segments[s + 1].number_of_documents
                if combined_size < smallest_combined_size:
                    smallest_combined_size = combined_size
                    smallest_pos = s
            s += 1
        if smallest_pos == -1:
            print(f"No candidate segments to merge - need atleast two flushed segments", flush=True)
            self._merge_lock.release_write()
            return num_segments, num_segments
        # we need segments at s and s + 1
        print(
            f"Initiating merge with segments {self._segments[smallest_pos].segment_id} and {self._segments[smallest_pos+1].segment_id}...",
            flush=True)
        start_time = time.time()
        # select the two smallest ADJACENT segments - this ensures doc ids are kept in order on disk
        # this could be improved - we could for example ensure our segments were sorted on flush to speed this up with
        # a linear merge to produce a combined term list
        # produce a combined term dump
        terms = set(self._segments[smallest_pos].keys()).union(set(self._segments[smallest_pos + 1].keys()))
        print(f"{len(terms)} terms in combined merge set", flush=True)
        # new_segment = Segment(_create_segment_id(), self._storage_path, self._doc_value_fields)
        for term in terms:
            postings = self._segments[smallest_pos].get_term(term)
            if postings:
                postings.add_term_info(self._segments[smallest_pos].get_term(term))
            else:
                postings = self._segments[smallest_pos].get_term(term)

        print(f"Merge completed in {time.time() - start_time}s")
        # now we need a stop the word event to modify the segments list, removing are two old ones and inserting our new one
        self._merge_lock.release_write()
        return num_segments, len(self._segments)
