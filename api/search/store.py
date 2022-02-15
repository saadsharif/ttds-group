import ujson as json
import io
import os.path
import bisect
import logging

from lmdbm import Lmdb

from search.exception import StoreException
from search.posting import TermPosting

logger = logging.getLogger(__name__)


# this represents the document store. Currently, we use lighting db so documents do not need to be held in
# memory, subject to change
class DocumentStore(Lmdb):
    def _pre_key(self, value):
        return value.encode("utf-8")

    def _post_key(self, value):
        return value.decode("utf-8")

    def _pre_value(self, value):
        return json.dumps(value).encode("utf-8")

    def _post_value(self, value):
        return json.loads(value.decode("utf-8"))


def parse_line(line):
    (left, sep, right) = line.partition(b'\t')
    term = json.loads(left.decode('utf8'))
    posting = TermPosting.from_store_format(right.decode('utf8'))
    return term, posting


def parse_term(line):
    (left, sep, right) = line.partition(b'\t')
    return json.loads(left.decode('utf8'))


def parse_posting(line):
    (left, sep, right) = line.partition(b'\t')
    return TermPosting.from_store_format(right.decode('utf8'))


# this is not thread - we assume locking at higher levels
# TODO: currently we store positions and postings together - meaning positions are unnecessarily read for queries that
#  dont need them - we should separate
class SegmentStore(dict):
    START_FLAG = b'# FILE-DICT v1\n'

    def __init__(self, path):
        self.path = path

        if os.path.exists(path):
            file = io.open(path, 'r+b')
        else:
            file = io.open(path, 'w+b')
            file.write(self.START_FLAG)
            file.flush()

        self._file = file
        self._offsets = {}  # the (size, offset) of the lines, where size is in bytes, including the trailing \n
        self._free_lines = []

        offset = 0
        while True:
            line = file.readline()
            if line == b'':  # end of file
                break

            # ignore empty lines
            if line == b'\n':
                offset += len(line)
                continue

            if line.startswith(b'#'):  # skip comments but add to free list
                if len(line) > 5:
                    self._free_lines.append((len(line), offset))
            else:
                # let's parse the value as well to be sure the data is ok
                key = parse_term(line)
                self._offsets[key] = offset

            offset += len(line)

        self._free_lines.sort()
        logger.info(f"Created pysos dict '{self.path}' with {len(self)} items")
        logger.debug("free lines: " + str(len(self._free_lines)))

    def _freeLine(self, offset):
        self._file.seek(offset)
        self._file.write(b'#')
        self._file.flush()

        line = self._file.readline()
        size = len(line) + 1  # one character was written beforehand

        if size > 5:
            bisect.insort(self._free_lines, (len(line) + 1, offset))

    def _findLine(self, size):
        index = bisect.bisect(self._free_lines, (size, 0))
        if index >= len(self._free_lines):
            return None
        else:
            return self._free_lines.pop(index)

    def _isWorthIt(self, size):
        # determines if it's worth to add the free line to the list
        # we don't want to clutter this list with a large amount of tiny gaps
        return (size > 5 + len(self._free_lines))

    def __getitem__(self, key):
        offset = self._offsets[key]
        self._file.seek(offset)
        line = self._file.readline()
        return parse_posting(line)
    """
        # TODO: we should allow a reader to opened on this and reused for each query thread
            with open(self.path, 'r+b') as reader:
                reader.seek(offset)
                line = reader.readline()
            return parse_posting(line)"""

    def __setitem__(self, key, term_posting):
        if key in self._offsets:
           raise StoreException("Segment store is append only")

        # we have a term posting we need to store,
        line = json.dumps(key, ensure_ascii=False) + '\t' + term_posting.to_store_format() + '\n'
        line = line.encode('UTF-8')
        # we seek to the end immediately - no updates, not deletes
        self._file.seek(0, os.SEEK_END)
        offset = self._file.tell()

        # if it's a really big line, it won't be written at once on the disk
        # so until it's done, let's consider it a comment
        self._file.write(b'#' + line[1:])
        if line[-1] == 35:
            # if it ends with a "comment" (bytes to recycle),
            # let's be clean and avoid cutting unicode chars in the middle
            while self._file.peek(1)[0] & 0x80 == 0x80:  # it's a continuation byte
                self._file.write(b'.')
        self._file.flush()
        # now that everything has been written...
        self._file.seek(offset)
        self._file.write(line[0:1])
        self._file.flush()
        self._offsets[key] = offset

    def __delitem__(self, key):
        offset = self._offsets[key]
        self._freeLine(offset)
        del self._offsets[key]

    def __contains__(self, key):
        return (key in self._offsets)

    def setdefault(self, key, val):
        # See https://github.com/dagnelies/pysos/issues/3
        raise UserWarning('Operation not available')

    def keys(self):
        return self._offsets.keys()

    def clear(self):
        self._file.truncate(0)
        self._file.flush()
        self._offsets = {}
        self._free_lines = []

    def items(self):
        offset = 0
        while True:
            # if something was read/written while iterating, the stream might be positioned elsewhere
            if self._file.tell() != offset:
                self._file.seek(offset)  # put it back on track

            line = self._file.readline()
            if line == b'':  # end of file
                break

            offset += len(line)
            # ignore empty and commented lines
            if line == b'\n' or line[0] == 35:
                continue
            yield parse_line(line)

    def __iter__(self):
        return self.keys()

    def values(self):
        for item in self.items():
            yield item[1]

    def __len__(self):
        return len(self._offsets)

    def size(self):
        self._file.size()

    def close(self):
        self._file.close()
        logger.info(f"Closed pysos dict '{self.path}' with {len(self)} items'")
        logger.debug("free lines: " + str(len(self._free_lines)))


class List(list):
    START_FLAG = b'# FILE-LIST v1\n'

    def __init__(self, path):
        self._dict = SegmentStore(path)
        self._indexes = sorted(self._dict.keys())

    def __getitem__(self, i):
        key = self._indexes[i]
        return self._dict[key]

    def __setitem__(self, i, value):
        key = self._indexes[i]
        self._dict[key] = value

    def append(self, value):

        if len(self._indexes) == 0:
            key = 0
        else:
            key = self._indexes[-1] + 1

        self._dict[key] = value
        self._indexes.append(key)

    def __delitem__(self, i):
        key = self._indexes[i]
        del self._dict[key]
        del self._indexes[i]

    def __len__(self):
        return len(self._indexes)

    def __contains__(self, key):
        raise Exception('Operation not supported for lists')

    # this must be overriden in order to provide the correct order
    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def clear(self):
        self._dict.clear()
        self._indexes = []

    def size(self):
        self._dict.size()

    def close(self):
        self._dict.close()


def load(path):
    file = open(path, 'rb')
    first = file.readline()

    if first == SegmentStore.START_FLAG:
        file.close()
        return SegmentStore(path)
    if first == List.START_FLAG:
        file.close()
        return List(path)

    for line in file:
        if line[0] == 0x23:
            continue
        key = parse_term(line)
        if isinstance(key, int):
            file.close()
            return List(path)
        else:
            file.close()
            return SegmentStore(path)
    raise Exception("Empty collection without header. Cannot determine whether it is a list or a dict.")
