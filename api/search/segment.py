import os.path
import time
import uuid

from search.lock import ReadWriteLock
from search.posting import TermPosting
from search.store import SegmentStore

# new segment rolled over on hitting this
DEFAULT_MAX_DOCS_PER_SEGMENT = 1000


def _create_segment_id():
    return f"{round(time.time() * 1000)}_{uuid.uuid1().hex[:4]}"


class Segment:

    def __init__(self, segment_id, storage_path, max_docs=DEFAULT_MAX_DOCS_PER_SEGMENT):
        # for now we use a hash for the term dictionary - in future we may want to use a tree here
        # this index persists the postings on disk to save disk - note that segments are immutable
        # the keys to this dict are the terms, the values offsets
        self._segment_id = segment_id
        self._posting_file = os.path.join(storage_path, f"{self._segment_id}.pos")
        self._index = SegmentStore(self._posting_file)
        self._buffer = {}
        self._number_of_documents = 0
        self._is_flushed = False
        self._max_docs = max_docs
        self._flush_lock = ReadWriteLock()
        self._indexing_lock = ReadWriteLock()

    def is_flushed(self):
        return self._is_flushed

    def has_capacity(self):
        return self._number_of_documents < self._max_docs

    # this adds the document to the buffer only, the flush writes it to disk
    def add_document(self, doc_id, terms):
        # IMPORTANT: we allow only require a lock on indexing - this means indexing could happen during querying. This
        # results in potentially dirty reads (not a big deal). We will be blocked by a flush though - rare!
        self._indexing_lock.acquire_write()
        if self.is_flushed():
            raise IndexError(
                f"Segment {self._segment_id} has been flushed. Attempting to add docs to immutable segments.")
        p = 0
        for term in terms:
            if term not in self._buffer:
                term_posting = TermPosting()
                self._buffer[term] = term_posting
            self._buffer[term].add_position(doc_id, p)
            p += 1
        self._number_of_documents += 1
        self._indexing_lock.release_write()

    def get_term(self, term):
        # use the buffer if this is an in memory segment
        # can't read whilst flushing the buffer - maybe not needed - prevents empty results basically
        self._flush_lock.acquire_read()
        pos = []
        if not self._is_flushed:
            if term in self._buffer:
                pos = self._buffer[term]
            self._flush_lock.release_read()
        else:
            # don't need the read lock on an immutable segment
            self._flush_lock.release_read()
            # use the disk postings if this has been flushed
            if term in self._index:
                pos = self._index[term]
        return pos

    # flushed the buffer to disk - this can be called manually and "closes" the segment to additions making it
    # immutable
    def flush(self):
        # whilst we're flushing, reads can continue on the buffer. Indexing can't.
        self._indexing_lock.acquire_write()
        for key, values in self._buffer.items():
            self._index[key] = values
        # this flush is just to prevent queries from reading an empty buffer - might not be needed. Note we do this
        # only for the period of clearing the buffer - not during flushing - very short period
        self._flush_lock.acquire_write()
        self._is_flushed = True
        # release the memory of the segment
        self._buffer.clear()
        self._flush_lock.release_write()
        self._indexing_lock.release_write()

    def __getstate__(self):
        """Return state values to be pickled. Just a state file."""
        # note we ignore the heavy stuff here e.g. the buffer and index
        return self._segment_id, self._posting_file, self._number_of_documents, self._is_flushed, self._max_docs

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""
        self._segment_id, self._posting_file, self._number_of_documents, self._is_flushed, self._max_docs = state
        self._buffer = {}
        # if we're unpickling we're loading - unknown state potentially, close the segment
        self._is_flushed = True
        self._flush_lock = ReadWriteLock()
        self._indexing_lock = ReadWriteLock()
        # this will load the index off disk
        self._index = SegmentStore(self._posting_file)

    def __iter__(self):
        return self.keys()

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
            yield from self._buffer.items()
        # don't need a read lock on immutable store - it cant reopen
        self._flush_lock.release_read()
        yield from self._index.items()

    # this closes the segment on shutdown
    def close(self):
        self._index.close()
