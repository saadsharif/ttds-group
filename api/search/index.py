import os
import pickle
import uuid
from bidict import bidict
from search.analyzer import Analyzer
from search.exception import IndexException
from search.lock import ReadWriteLock
from search.models import Result
from search.posting import TermPosting
from search.query import Query
from search.segment import Segment, _create_segment_id
from search.store import DocumentStore


class Index:

    def __init__(self, storage_path, analyzer=Analyzer(), index_id=uuid.uuid4()):
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
        pass

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
            pickle.dump(state, index_file)
            print("OK")
        if len(self._segments) > 0:
            # flush the last segment if we need to
            most_recent = self._segments[0]
            if not most_recent.is_flushed():
                print(f"Flushing last segment...", end="")
                most_recent.flush()
                print("OK")
            print(f"Closing all segments...", end="")
            for segment in self._segments:
                segment.close()
            print("OK")
        self._write_lock.release_write()

    def load(self):
        self._write_lock.acquire_write()
        if os.path.isfile(self._get_db_path()):
            with open(self._get_db_path(), 'rb') as index_file:
                index = pickle.load(index_file)
                self.__dict__.update(index)
        self._write_lock.release_write()

    # closes the index
    def close(self):
        self.save()
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
            self._segments = [Segment(_create_segment_id(), self._storage_path)]
            return self._segments[0]
        most_recent = self._segments[0]
        if most_recent.is_flushed():
            # segment has been flushed, create a new one
            self._segments.index(0, Segment(_create_segment_id(),self._storage_path))
            return self._segments[0]
        if not most_recent.has_capacity():
            # segment is open but has no capacity so flush
            most_recent.flush()
            # insert new
            self._segments.index(0, Segment(_create_segment_id(), self._storage_path))
        return most_recent

    # this is an append only operation. We generated a new internal id for the document and store a mapping between the
    # the two. The passed id here must be unique - no updates supported, but can be anything.
    def add_document(self, document):
        # enforce single threaded indexing
        self._write_lock.acquire_write()
        if document.id in self._id_mappings.inverse:
            self._write_lock.release_write()
            raise IndexException(f'{document.id} already exists in index {self._index_id}')
        self._id_mappings[self._current_doc_id] = document.id
        # TODO: no separate indices per field currently - we might want to add this
        terms = self.analyzer.process_document(document)
        # TODO: Handle failures here!
        self.__get_writeable_segment().add_document(self._current_doc_id, terms)
        # persist to the bd
        self._doc_store[str(self._current_doc_id)] = document.fields
        doc_id = self._current_doc_id
        self._current_doc_id += 1
        self._write_lock.release_write()
        return document.id, doc_id

    # this is more efficient than single document addition
    def add_documents(self, documents):
        # enforce single threaded indexing
        failures = []
        self._write_lock.acquire_write()
        # check all the ids
        docs_to_index = []
        for document in documents:
            if document.id in self._id_mappings.inverse:
                failures.append(f'{document.id} already exists in index {self._index_id}')
            else:
                docs_to_index.append(document)
        # this could be more efficient - i.e. we could optimise bulk additions - less locking and large write chunks
        doc_ids = []
        doc_batch = {}
        # TODO: Handle failures here!
        for document in docs_to_index:
            self._id_mappings[self._current_doc_id] = document.id
            terms = self.analyzer.process_document(document)
            self.__get_writeable_segment().add_document(self._current_doc_id, terms)
            doc_ids.append((document.id, self._current_doc_id))
            doc_batch[str(self._current_doc_id)] = document.fields
            self._current_doc_id += 1
        self._doc_store.update(doc_batch)
        # persists the batch to the db
        self._write_lock.release_write()
        return doc_ids, failures

    def search(self, query):
        docs, total = Query(self).execute(query.query, query.score, query.max_results)
        return [Result(self._id_mappings[doc.doc_id], doc.score, fields=self._doc_store.get(str(doc.doc_id))) for doc in
                docs], total

    def get_term(self, term):
        # get all the matching docs in all the segments - currently we assume the term is in every segment
        # this should be improved e.g. using a bloom filter or some bit set, inside the segment though -i.e.
        # it should return [] quickly
        term_posting = TermPosting()
        for segment in self._segments:
            term_posting.add_term_info(segment.get_term(term))
        return term_posting
