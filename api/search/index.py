import os
import pickle
import sys
import time
import traceback
import uuid
from bidict import bidict
from utils.utils import print_progress
from search.analyzer import Analyzer
from search.bert import BERTModule
from search.exception import IndexException, SearchException, MergeException
from search.lock import ReadWriteLock
from search.models import Result
from search.posting import TermPosting
from search.query import Query
from search.segment import Segment, _create_segment_id
from search.store import DocumentStore
from search.suggestions import Suggester


class Index:

    def __init__(self, storage_path, analyzer=Analyzer(), doc_value_fields=[], index_id=uuid.uuid4()):
        # location of index files
        self._storage_path = storage_path
        self.analyzer = analyzer
        self._suggester = Suggester()
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
        self._vector_model = BERTModule(vmodel=3)
        # facet fields
        self._doc_value_fields = doc_value_fields
        # merge lock - only one merge at once
        self._merge_lock = ReadWriteLock()
        # merge update - this is used when we update the list of segments post merge - reads can't occur during this
        self._segment_update_lock = ReadWriteLock()
        # vector store
        self._vector_store = DocumentStore.open(os.path.join(self._storage_path, 'vectors.db'), 'c')

    def _get_db_path(self):
        return os.path.join(self._storage_path, 'index.idb')

    # not thread safe and pickles the index - mostly meta and doc ids
    def _store_index_meta(self):
        with open(self._get_db_path(), 'wb') as index_file:
            print(f"Saving state file to {self._get_db_path()}...", end="")
            state = self.__dict__.copy()
            # we don't store the doc store
            del state['_doc_store']
            del state['_vector_store']
            del state['_write_lock']
            del state['_merge_lock']
            del state['_segment_update_lock']
            # del state['_vector_model']
            pickle.dump(state, index_file)
            print("OK")

    # saves the index to disk - for now just pickle down given the sizes
    def save(self):
        # we need to lock as we shouldn't index during flushing or vise versa
        self._write_lock.acquire_write()
        print(f"Syncing document store...", end="")
        self._doc_store.sync()
        print("OK")
        print(f"Syncing vector store...", end="")
        self._vector_store.sync()
        print("OK")
        if len(self._segments) > 0:
            # flush the last segment if we need to
            most_recent = self._segments[-1]
            if not most_recent.is_flushed():
                print(f"Flushing last segment...", end="")
                self._suggester.add_from_segment_buffer()
                most_recent.flush()
                print("OK")
        self._store_index_meta()
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
        self._vector_store.close()

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

    # IMPORTANT: This assumes single threaded indexing
    def __get_writeable_segment(self):
        if len(self._segments) == 0:
            # starting case - create one with new id
            self._segments = [Segment(_create_segment_id(), self._storage_path, self._doc_value_fields)]
            return self._segments[-1]
        most_recent = self._segments[-1]
        if most_recent.is_flushed():
            self._segment_update_lock.acquire_write()
            # segment has been flushed, create a new one
            self._segments.append(Segment(_create_segment_id(), self._storage_path, self._doc_value_fields))
            self._segment_update_lock.release_write()
            return self._segments[-1]
        if not most_recent.has_buffer_capacity():
            self._suggester.add_from_segment_buffer()
            # segment is open but has no capacity so flush
            most_recent.flush()
            # insert new
            self._segment_update_lock.acquire_write()
            self._segments.append(Segment(_create_segment_id(), self._storage_path, self._doc_value_fields))
            self._segment_update_lock.release_write()
        return self._segments[-1]

    def process_document(self, document, flushTrie=False):
        self._id_mappings[self._current_doc_id] = document.id
        terms_and_tokens = self.analyzer.process_document(document, keepOriginal=True)
        # this allows exact matching on doc value fields TODO: really we should have a different index for this
        doc_value_terms = []
        doc_values = {}
        for field in self._doc_value_fields:
            if field in document.fields:
                # TODO: we assume all doc values are a list
                doc_values[field] = document.fields[field]
                doc_value_terms += [(f"{field}:{'_'.join(self.analyzer.tokenize(value))}", None) for value in
                                    document.fields[field]]
        terms_and_tokens = doc_value_terms + terms_and_tokens
        # Flush trie if flushing segment
        segment = self.__get_writeable_segment()
        self._suggester.copy_buffer(segment)
        segment.add_document(self._current_doc_id, terms_and_tokens, doc_values=doc_values)
        if flushTrie:
            self._suggester.add_from_segment_buffer()

    # this is an append only operation. We generated a new internal id for the document and store a mapping between the
    # the two. The passed id here must be unique - no updates supported, but can be anything.
    def add_document(self, document):
        # enforce single threaded indexing
        self._write_lock.acquire_write()
        if document.id in self._id_mappings.inverse:
            self._write_lock.release_write()
            raise IndexException(f'{document.id} already exists in index {self._index_id}')
        try:
            self.process_document(document, flushTrie=True)
            # persist to the db
            self._doc_store[str(self._current_doc_id)] = document.fields
            # persist vector
            if len(document.vector) > 0:
                self._vector_store[str(self._current_doc_id)] = document.vector
            doc_id = self._current_doc_id
            self._current_doc_id += 1
            self._write_lock.release_write()
        except Exception as e:
            traceback.print_exc()
            self._write_lock.release_write()
            raise IndexException(f"Unexpected exception during indexing - {e}")
        return document.id, doc_id

    # this is more efficient than single document addition
    def add_documents(self, documents, flushTrie=False):
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
            vector_batch = {}
            n = len(docs_to_index)
            for idx, document in enumerate(docs_to_index):
                self.process_document(document)
                doc_ids.append((document.id, self._current_doc_id))
                doc_batch[str(self._current_doc_id)] = document.fields
                if len(document.vector) > 0:
                    vector_batch[str(self._current_doc_id)] = document.vector
                print_progress(idx + 1, n, label="Adding documents")
                self._current_doc_id += 1
            print("")  # done with the progress
            # persists the batch to the db
            self._doc_store.update(doc_batch)
            # persists the vectors
            if len(vector_batch) > 0:
                self._vector_store.update(vector_batch)
            if flushTrie:
                self._suggester.add_from_segment_buffer()
            self._write_lock.release_write()
        except Exception as e:
            self._write_lock.release_write()
            raise IndexException(f"Unexpected exception during indexing - {e}")
        return doc_ids, failures

    def _get_document(self, id, fields):
        doc = self._doc_store.get(str(id))
        if not fields:
            return doc
        return {field: doc[field] for field in fields if field in doc}

    def get_document_vector(self, id):
        vector = self._vector_store.get(str(id))
        return [] if vector is None else vector

    def suggest(self, search):
        try:
            self._segment_update_lock.acquire_read()
            rets = []
            if search:
                matches = self._suggester.suggest(search)
                search_length = len(search.query)
                for term in matches:
                    [common, highlight] = [term[:search_length], term[search_length:]]
                    rets.append({
                        'suggestion': term,
                        'highlight': f"{common}<b>{highlight}</b>"})
            self._segment_update_lock.release_read()
            return rets
        except Exception as e:
            raise SearchException(f"Unexpected exception during querying - {e}")

    def search(self, query):
        # filters are currently appended as an AND phrase - this isn't ideal but these doc value fields are also indexed
        try:
            filters = [f"{filter.field}:{'_'.join(self.analyzer.tokenize(filter.value))}" for filter in query.filters]
            query_text = query.query
            for filter in filters:
                query_text = f"{query_text} AND {filter}"
            docs, facets, total = Query(self).execute(query_text, query.score, query.max_results, query.offset,
                                                      query.facets, vector_score=query.vector_score)

            fields = set(query.fields)
            return [Result(self._id_mappings[doc.doc_id], doc.score, fields=self._get_document(str(doc.doc_id), fields))
                    for
                    doc in
                    docs], facets, total
        except Exception as e:
            raise SearchException(f"Unexpected exception during querying - {e}")

    def get_term(self, term, with_positions=True, with_skips=True):
        # get all the matching docs in all the segments - currently we assume the term is in every segment
        # this should be improved e.g. using a bloom filter or some bit set, inside the segment though -i.e.
        # it should return [] quickly
        combined_posting = TermPosting()
        self._segment_update_lock.acquire_read()
        if term:
            for segment in self._segments:
                term_posting = segment.get_term(term, with_positions=with_positions, with_skips=with_skips)
                if term_posting:
                    combined_posting.add_term_info(term_posting, update_skips=True)
        self._segment_update_lock.release_read()
        return combined_posting

    def get_vector(self, query):
        return self._vector_model.embed(query, sentwise=False)

    def has_doc_id(self, field):
        return field in self._doc_value_fields

    def get_doc_values(self, field, doc_id):
        self._segment_update_lock.acquire_read()
        values = []
        if field in self._doc_value_fields:
            # once we find the segment we can break
            for segment in self._segments:
                values = segment.get_doc_values(field, doc_id)
                if values is not None:
                    break
        self._segment_update_lock.release_read()
        return values

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
                # number of documents is a crude metric of size but avg should be same across index
                combined_size = self._segments[s].number_of_documents + self._segments[s + 1].number_of_documents
                if combined_size < smallest_combined_size:
                    smallest_combined_size = combined_size
                    smallest_pos = s
            s += 1
        if smallest_pos == -1:
            print(f"No candidate segments to merge - need at least two flushed segments", flush=True)
            self._merge_lock.release_write()
            return num_segments, num_segments
        # we need segments at s and s + 1
        print(
            f"Initiating merge with segments {self._segments[smallest_pos].segment_id} and {self._segments[smallest_pos + 1].segment_id}...",
            flush=True)
        start_time = time.time()
        # select the two smallest ADJACENT segments - this ensures doc ids are kept in order on disk
        # this could be improved - we could for example ensure our segments were sorted on flush to speed this up with
        # a linear merge to produce a combined term list
        # produce a combined term dump
        l_segment = self._segments[smallest_pos]
        r_segment = self._segments[smallest_pos + 1]
        try:
            new_segment = Segment(_create_segment_id(), self._storage_path, self._doc_value_fields)
            new_segment.merge(l_segment, r_segment)
            # flush the new segment
            new_segment.flush()
            self._segment_update_lock.acquire_write()
            # a stop the word event to modify the segments list, removing are two old ones and inserting our new one
            new_segments = self._segments[:smallest_pos] + [new_segment] + self._segments[smallest_pos + 2:]
            self._segments = new_segments
            l_segment.delete()
            r_segment.delete()
            # we also need to write our new index file
            self._store_index_meta()
            print(f"Merge completed in {time.time() - start_time}s")
            self._segment_update_lock.release_write()
            self._merge_lock.release_write()
        except Exception as e:
            self._merge_lock.release_write()
            self._segment_update_lock.release_write()
            raise MergeException(f"Unexpected exception during merge - {e}")
        return num_segments, len(self._segments)
