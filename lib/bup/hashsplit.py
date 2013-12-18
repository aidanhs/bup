import math
from bup import _helpers
from bup.helpers import *

BLOB_MAX = 8192*4   # 8192 is the "typical" blob size for bupsplit
BLOB_READ_SIZE = 1024*1024
MAX_PER_TREE = 256
progress_callback = None
fanout = 16

GIT_MODE_FILE = 0100644
GIT_MODE_TREE = 040000
GIT_MODE_SYMLINK = 0120000
assert(GIT_MODE_TREE != 40000)  # 0xxx should be treated as octal

class ZCBuf:
    def __init__(self):
        self.size = 2*BLOB_READ_SIZE
        self.data = bytearray(self.size)
        self.start = 0
        self.length = 0

    def peek(self, count):
        if count > self.length: count = self.length
        return buffer(self.data, self.start, count)

    def eat(self, count):
        self.start += count
        self.length -= count

    def used(self):
        return self.length

    def prepput(self, posslen):
        # If new data would overflow off end of buffer, move current data to
        # beginning of buffer
        end = self.start + self.length
        if end + posslen > self.size:
            assert(self.length + posslen < self.size)
            self.data[:self.length] = self.data[self.start:end]
            self.start = 0
        return memoryview(self.data)[end:end+posslen]

    def haveput(self, putlen):
        self.length += putlen

# The purpose of this type of buffer is to avoid copying on peek(), get(),
# and eat().  We do copy the buffer contents on put(), but that should
# be ok if we always only put() large amounts of data at a time.
class Buf:
    def __init__(self):
        self.data = ''
        self.start = 0

    def put(self, s):
        if s:
            self.data = buffer(self.data, self.start) + s
            self.start = 0
            
    def peek(self, count):
        return buffer(self.data, self.start, count)
    
    def eat(self, count):
        self.start += count

    def get(self, count):
        v = buffer(self.data, self.start, count)
        self.start += count
        return v

    def used(self):
        return len(self.data) - self.start


def readfile_iter(files, progress=None):
    for filenum,f in enumerate(files):
        ofs = 0
        b = ''
        while 1:
            if progress:
                progress(filenum, len(b))
            if ofs > 1024*1024:
                fadvise_done(f, ofs - 1024*1024)
            b = f.read(BLOB_READ_SIZE)
            ofs += len(b)
            if not b:
                fadvise_done(f, ofs)
                break
            yield b


def _splitbuf(buf, basebits, fanbits):
    while 1:
        b = buf.peek(buf.used())
        (ofs, bits) = _helpers.splitbuf(b)
        if ofs > BLOB_MAX:
            ofs = BLOB_MAX
        if ofs:
            buf.eat(ofs)
            level = (bits-basebits)//fanbits  # integer division
            yield buffer(b, 0, ofs), level
        else:
            break
    while buf.used() >= BLOB_MAX:
        # limit max blob size
        yield buf.get(BLOB_MAX), 0


def _hashsplit_iter(files, progress):
    assert(BLOB_READ_SIZE > BLOB_MAX)
    basebits = _helpers.blobbits()
    fanbits = int(math.log(fanout or 128, 2))
    buf = Buf()
    for inblock in readfile_iter(files, progress):
        buf.put(inblock)
        for buf_and_level in _splitbuf(buf, basebits, fanbits):
            yield buf_and_level
    if buf.used():
        yield buf.get(buf.used()), 0

#####################################

def zcreadfile_iter(files, buf, progress=None):
    for filenum,f in enumerate(files):
        ofs = 0
        n = 0
        while 1:
            if progress:
                progress(filenum, n)
            if ofs > 1024*1024:
                fadvise_done(f, ofs - 1024*1024)
            putbuf = buf.prepput(BLOB_READ_SIZE)
            # TODO: readinto is technically only supported for the io library,
            # so need to convert open calls everywhere to io.FileIO
            n = f.readinto(putbuf)
            if n:
                buf.haveput(n)
                ofs += n
                yield False
            else:
                fadvise_done(f, ofs)
                break

def _zcsplitbuf(buf, basebits, fanbits):
    b = buf.peek(BLOB_MAX)
    (ofs, bits) = _helpers.splitbuf(b)
    # Didn't find an splitpoint in the given buffer
    if ofs == 0:
        ofs = len(b)
        level = 0
        bits = 0
    # If did find the offset, determine the level
    if bits:
        level = (bits-basebits)//fanbits  # integer division
    buf.eat(ofs)
    return buffer(b, 0, ofs), level

import __builtin__
next = __builtin__.next
def _zchashsplit_iter(files, progress):
    assert(BLOB_READ_SIZE > BLOB_MAX)
    basebits = _helpers.blobbits()
    fanbits = int(math.log(fanout or 128, 2))
    buf = ZCBuf()
    readiter = zcreadfile_iter(files, buf, progress)
    readfinished = False
    while True:
        while buf.used() < BLOB_MAX and not readfinished:
            if next(readiter, True):
                readfinished = True
                break
        yield _zcsplitbuf(buf, basebits, fanbits)
        if buf.used() == 0:
            break

def _hashsplit_iter_keep_boundaries(files, progress):
    for real_filenum,f in enumerate(files):
        if progress:
            def prog(filenum, nbytes):
                # the inner _hashsplit_iter doesn't know the real file count,
                # so we'll replace it here.
                return progress(real_filenum, nbytes)
        else:
            prog = None
        for buf_and_level in _hashsplit_iter([f], progress=prog):
            yield buf_and_level


def hashsplit_iter(files, keep_boundaries, progress):
    if keep_boundaries:
        return _hashsplit_iter_keep_boundaries(files, progress)
    else:
        return _hashsplit_iter(files, progress)


total_split = 0
def split_to_blobs(makeblob, files, keep_boundaries, progress):
    global total_split
    for (blob, level) in hashsplit_iter(files, keep_boundaries, progress):
        sha = makeblob(blob)
        total_split += len(blob)
        if progress_callback:
            progress_callback(len(blob))
        yield (sha, len(blob), level)


def _make_shalist(l):
    ofs = 0
    l = list(l)
    total = sum(size for mode,sha,size, in l)
    vlen = len('%x' % total)
    shalist = []
    for (mode, sha, size) in l:
        shalist.append((mode, '%0*x' % (vlen,ofs), sha))
        ofs += size
    assert(ofs == total)
    return (shalist, total)


def _squish(maketree, stacks, n):
    i = 0
    while i < n or len(stacks[i]) >= MAX_PER_TREE:
        while len(stacks) <= i+1:
            stacks.append([])
        if len(stacks[i]) == 1:
            stacks[i+1] += stacks[i]
        elif stacks[i]:
            (shalist, size) = _make_shalist(stacks[i])
            tree = maketree(shalist)
            stacks[i+1].append((GIT_MODE_TREE, tree, size))
        stacks[i] = []
        i += 1


def split_to_shalist(makeblob, maketree, files,
                     keep_boundaries, progress=None):
    sl = split_to_blobs(makeblob, files, keep_boundaries, progress)
    assert(fanout != 0)
    if not fanout:
        shal = []
        for (sha,size,level) in sl:
            shal.append((GIT_MODE_FILE, sha, size))
        return _make_shalist(shal)[0]
    else:
        stacks = [[]]
        for (sha,size,level) in sl:
            stacks[0].append((GIT_MODE_FILE, sha, size))
            _squish(maketree, stacks, level)
        #log('stacks: %r\n' % [len(i) for i in stacks])
        _squish(maketree, stacks, len(stacks)-1)
        #log('stacks: %r\n' % [len(i) for i in stacks])
        return _make_shalist(stacks[-1])[0]


def split_to_blob_or_tree(makeblob, maketree, files, keep_boundaries):
    shalist = list(split_to_shalist(makeblob, maketree,
                                    files, keep_boundaries))
    if len(shalist) == 1:
        return (shalist[0][0], shalist[0][2])
    elif len(shalist) == 0:
        return (GIT_MODE_FILE, makeblob(''))
    else:
        return (GIT_MODE_TREE, maketree(shalist))


def open_noatime(name):
    fd = _helpers.open_noatime(name)
    try:
        return os.fdopen(fd, 'rb', 1024*1024)
    except:
        try:
            os.close(fd)
        except:
            pass
        raise


def fadvise_done(f, ofs):
    assert(ofs >= 0)
    if ofs > 0 and hasattr(f, 'fileno'):
        _helpers.fadvise_done(f.fileno(), ofs)
