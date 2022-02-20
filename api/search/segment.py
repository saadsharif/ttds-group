import ujson as json
import os.path
import sys
import time
import uuid

from search.lock import ReadWriteLock
from search.posting import TermPosting
from search.store import Store

# new segment rolled over on hitting this
DEFAULT_MAX_DOCS_PER_SEGMENT = 2000


def _create_segment_id():
    return f"{round(time.time() * 1000)}_{uuid.uuid1().hex[:4]}"


class Segment:

    def __init__(self, segment_id, storage_path, doc_value_fields, max_docs=DEFAULT_MAX_DOCS_PER_SEGMENT):
        # for now we use a hash for the term dictionary - in future we may want to use a tree here
        # this index persists the postings on disk to save disk - note that segments are immutable
        # the keys to this dict are the terms, the values offsets
        self._segment_id = segment_id
        self._posting_file = os.path.join(storage_path, f"{self._segment_id}.pos")
        self._index = Store(self._posting_file)
        self._buffer = {}
        self._is_flushed = False
        self._max_docs = max_docs
        # stores the doc_id range in this segment - useful for facet checks
        self._max_doc_id = 0
        self._min_doc_id = sys.maxsize
        self._doc_value_fields = {}
        self._doc_values = {}
        for field in doc_value_fields:
            doc_value_store_path = os.path.join(storage_path, f"{self._segment_id}-{field}.dv")
            self._doc_values[field] = Store(doc_value_store_path)
            self._doc_value_fields[field] = doc_value_store_path
        self._flush_lock = ReadWriteLock()
        self._indexing_lock = ReadWriteLock()

    def is_flushed(self):
        return self._is_flushed

    def has_buffer_capacity(self):
        return self.number_of_documents < self._max_docs

    @property
    def segment_id(self):
        return self._segment_id

    @property
    def number_of_documents(self):
        if self._max_doc_id == 0 or self._min_doc_id == sys.maxsize:
            return 0
        return self._max_doc_id - self._min_doc_id

    # this adds the document to the buffer only, the flush writes it to disk
    def add_document(self, doc_id, terms, doc_values={}):
        # IMPORTANT: we allow only require a lock on indexing - this means indexing could happen during querying. This
        # results in potentially dirty reads (not a big deal). We will be blocked by a flush though - rare!
        self._indexing_lock.acquire_write()
        if self.is_flushed():
            self._indexing_lock.release_write()
            raise IndexError(
                f"Segment {self._segment_id} has been flushed. Attempting to add docs to immutable segments.")
        p = 0
        try:
            for term in terms:
                if term not in self._buffer:
                    term_posting = TermPosting()
                    self._buffer[term] = term_posting
                self._buffer[term].add_position(doc_id, p)
                p += 1
            for field, values in doc_values.items():
                if field in self._doc_values:
                    self._doc_values[field][doc_id] = json.dumps(values)
            if doc_id > self._max_doc_id:
                self._max_doc_id = doc_id
            if doc_id < self._min_doc_id:
                self._min_doc_id = doc_id
            self._indexing_lock.release_write()
        except Exception as e:
            # not alot we can do here as our buffer will be modified and we have no rollback currently
            self._indexing_lock.release_write()
            raise e

    def get_doc_values(self, field, doc_id):
        # fast check to avoid lookups
        if not field in self._doc_values:
            return None
        if doc_id < self._min_doc_id:
            return None
        if doc_id > self._max_doc_id:
            return None
        if doc_id in self._doc_values[field]:
            return json.loads(self._doc_values[field][doc_id])
        return None

    def get_term(self, term):
        # use the buffer if this is an in memory segment
        # can't read whilst flushing the buffer - maybe not needed - prevents empty results basically
        self._flush_lock.acquire_read()
        pos = []
        if not self._is_flushed:
            if term in self._buffer:
                pos = self._buffer[term]
            self._flush_lock.release_read()
            return pos
        # don't need the read lock on an immutable segment
        self._flush_lock.release_read()
        # use the disk postings if this has been flushed
        if term in self._index:
            return TermPosting.from_store_format(self._index[term])

    # flushed the buffer to disk - this can be called manually and "closes" the segment to additions making it immutable
    def flush(self):
        # whilst we're flushing, reads can continue on the buffer. Indexing can't.
        try:
            start_time = time.time()
            self._indexing_lock.acquire_write()
            print(f"Flushing segment {self._segment_id}")
            # flush the term buffer in sorted term order
            for term in sorted(self._buffer):
                self._index[term] = self._buffer[term].to_store_format()
            # this flush is just to prevent queries from reading an empty buffer - might not be needed. Note we do this
            # only for the period of clearing the buffer - not during flushing - very short period
            self._flush_lock.acquire_write()
        except Exception as e:
            # make sure we release
            print(f"Failed to flush segment - {self._segment_id} - {e}")
            self._flush_lock.release_write()
            self._indexing_lock.release_write()
            # if this happens bad things have happened, reset our files
            self._index.clear()
            for field in self._doc_values.keys():
                self._doc_values[field].clear()
            raise e
        self._is_flushed = True
        # release the memory of the segment
        self._buffer.clear()
        self._flush_lock.release_write()
        print(f"Segment {self._segment_id} flushed in {time.time() - start_time}s")
        self._indexing_lock.release_write()

    def __getstate__(self):
        """Return state values to be pickled. Just a state file."""
        # note we ignore the heavy stuff here e.g. the buffer and index
        return self._segment_id, self._posting_file, self._is_flushed, self._max_docs, self._max_doc_id, self._min_doc_id, self._doc_value_fields

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""
        self._segment_id, self._posting_file, self._is_flushed, self._max_docs, self._max_doc_id, self._min_doc_id, self._doc_value_fields = state
        print(f"Loading segment {self._segment_id}")
        self._buffer = {}
        # if we're unpickling we're loading - unknown state potentially, close the segment
        self._is_flushed = True
        self._flush_lock = ReadWriteLock()
        self._indexing_lock = ReadWriteLock()
        # this will load the index off disk
        print(f"Loading index segment {self._segment_id} from {self._posting_file}...")
        self._index = Store(self._posting_file)
        print(f"Index loaded for {self._segment_id}")
        # load the doc values
        self._doc_values = {}
        for field, path in self._doc_value_fields.items():
            print(f"Loading field {field} in segment {self._segment_id}...")
            self._doc_values[field] = Store(path)
            print(f"Field {field} loaded for segment {self._segment_id}")
        print(f"Segment {self._segment_id} loaded")

    def __iter__(self):
        return iter(self.items())

    def __len__(self):
        return len(self.keys())

    def keys(self):
        self._flush_lock.acquire_read()
        if not self.is_flushed():
            keys = list(self._buffer.keys())
            self._flush_lock.release_read()
            return keys
        # don't need a read lock on immutable store
        self._flush_lock.release_read()
        return self._index.keys()

    def items(self):
        self._flush_lock.acquire_read()
        if not self.is_flushed():
            # this isn't really thread safe
            yield from self._buffer.items()
            self._flush_lock.release_read()
        else:
            # don't need a read lock on immutable store - it cant be changed
            self._flush_lock.release_read()
            for term, posting in self._index.items():
                yield term, TermPosting.from_store_format(posting)

    def doc_value_items(self):
        for field, doc_values in self._doc_values.items():
            for doc_id, doc_value in doc_values.items():
                yield field, doc_id, json.loads(doc_value)

    # this closes the segment on shutdown
    def close(self):
        self._index.close()
        for doc_values in self._doc_values.values():
            doc_values.close()

    # These methods support merging segments - no locking required - we assume this is called in a single thread with
    # no active reads of writing on it

    # this merges two segments into this segment. We assume the segments are adjacent to each other i.e. doc ids are sequential
    def merge(self, l_segment, r_segment):
        # set some meta on the segments - the doc id range in the new segment - segments are adjacent so we know they
        # cover the range entirely
        min_id_l, max_id_l = l_segment.get_doc_id_range()
        min_id_r, max_id_r = r_segment.get_doc_id_range()
        self._min_doc_id = min(min_id_l, min_id_r)
        self._max_doc_id = max(max_id_l, max_id_r)
        self._merge_postings(l_segment, r_segment)
        self._merge_doc_ids(l_segment, r_segment)

    def _merge_doc_ids(self, l_segment, r_segment):
        print(f"Merging doc ids into {self._segment_id}...")
        l_iter = iter(l_segment.doc_value_items())
        r_iter = iter(r_segment.doc_value_items())
        # we rely on the left segment having lower doc ids than the right
        for field, doc_id, doc_value in l_iter:
            self._doc_values[field][doc_id] = json.dumps(doc_value)
        for field, doc_id, doc_value in r_iter:
            self._doc_values[field][doc_id] = json.dumps(doc_value)
        print(f"Doc ids merged")

    # merge the postings together - note we skip the buffer as it is faster (given no seeking required)
    def _merge_postings(self, l_segment, r_segment):
        print(f"Merging terms into {self._segment_id}...")
        # we know the terms will be in sorted order so we can linear merge these to avoid lots of seeking
        l_iter = iter(l_segment)
        r_iter = iter(r_segment)
        left_term, left_posting = next(l_iter, (None, None))
        right_term, right_posting = next(r_iter, (None, None))
        while left_term and right_term:
            if left_term < right_term:
                self._index[left_term] = left_posting.to_store_format()
                left_term, left_posting = next(l_iter, (None, None))
            elif left_term > right_term:
                self._index[right_term] = right_posting.to_store_format()
                right_term, right_posting = next(r_iter, (None, None))
            else:
                left_posting.add_term_info(right_posting)
                self._index[left_term] = left_posting.to_store_format()
                left_term, left_posting = next(l_iter, (None, None))
                right_term, right_posting = next(r_iter, (None, None))
        if left_term:
            self._index[left_term] = left_posting.to_store_format()
            for left_term, left_posting in l_iter:
                self._index[left_term] = left_posting.to_store_format()
        if right_term:
            self._index[right_term] = right_posting.to_store_format()
            for right_term, right_posting in r_iter:
                self._index[right_term] = right_posting.to_store_format()
        print(f"Terms merged")

    def get_doc_id_range(self):
        return self._min_doc_id, self._max_doc_id

    def delete(self):
        self.close()
        if os.path.exists(self._posting_file):
            os.remove(self._posting_file)
        for path in self._doc_value_fields.values():
            if os.path.exists(path):
                os.remove(path)
