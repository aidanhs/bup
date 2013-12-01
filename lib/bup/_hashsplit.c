#define _LARGEFILE64_SOURCE 1
#define PY_SSIZE_T_CLEAN 1
#undef NDEBUG
#include "../../config/config.h"

// According to Python, its header has to go first:
//   http://docs.python.org/2/c-api/intro.html#include-files
#include <Python.h>

#include "bupsplit.h"

// TODO: these shouldn't be #defined, get them from hashsplit module
#define BLOB_READ_SIZE 1024*1024
#define BLOB_MAX 8192*4

// Module-wide imports, never collected
static struct {
    PyObject *math;
    PyObject *helpers;
    PyObject *hashsplit; // The python module
} imports;


/********************************************************/
/********************************************************/


// https://github.com/cloudwu/pbc - A protocol buffers library for C
// http://stackoverflow.com/questions/11334226/find-good-buffer-library-in-c
// http://contiki.sourceforge.net/docs/2.6/a01686.html
// http://www.embedded.com/electronics-blogs/embedded-round-table/4419407/The-ring-buffer
// http://www.fourwalledcubicle.com/files/LightweightRingBuff.h
// http://atastypixel.com/blog/a-simple-fast-circular-buffer-implementation-for-audio-processing/
// http://nadeausoftware.com/articles/2012/05/c_c_tip_how_copy_memory_quickly
// http://nadeausoftware.com/articles/2012/03/c_c_tip_how_measure_cpu_time_benchmarking
typedef struct {
    unsigned char *buf;
    size_t size; // actual size of buf
    unsigned char *start;
    size_t len; // length of buffer with a value
} Buf;
static Buf *Buf_new(void)
{
    Buf *bufobj = malloc(sizeof(Buf));
    bufobj->buf = malloc(BLOB_READ_SIZE*2);
    bufobj->size = BLOB_READ_SIZE*2;
    bufobj->start = bufobj->buf;
    bufobj->len = 0;
    return bufobj;
}
static void Buf_del(Buf *b)
{
    free(b->buf);
    free(b);
}
static size_t Buf_used (Buf *b)
{
    return b->len;
}
static void Buf_eat (Buf *b, size_t count)
{
    b->start += count;
    b->len -= count;
}
static void Buf_peek (Buf *b, size_t count, unsigned char **target, size_t *got)
{
    *got = count < b->len ? count : b->len;
    *target = b->start;
}
static void Buf_prepput (Buf *b, size_t posslen, unsigned char **putbuf)
{
    if (b->start + b->len + posslen > b->buf + b->size) {
        assert(b->len + posslen <= b->size);
        memmove(b->buf, b->start, b->len);
        b->start = b->buf;
    }
    *putbuf = b->start + b->len;
}
static void Buf_haveput (Buf *b, size_t putlen)
{
    b->len += putlen;
}


/********************************************************/
/********************************************************/


typedef struct {
    PyObject_HEAD
    ssize_t ofs;
    Py_ssize_t filenum;
    /* Make sure python doesn't garbage collect and close the files */
    PyObject *curfile;
    int fd;
    PyObject *fileiter;
    PyObject *progressfn;
    Py_ssize_t prevread;
} readiter_state;

// Return -1 if the next file cannot be obtained (end of iter or error)
static int next_file(readiter_state *s)
{
    PyObject *fdobj;
    Py_XDECREF(s->curfile);
    s->curfile = PyIter_Next(s->fileiter);
    if (s->curfile == NULL)
        return -1;
    if (PyObject_HasAttrString(s->curfile, "fileno")) {
        fdobj = PyObject_CallMethod(s->curfile, "fileno", NULL);
        if (fdobj == NULL)
            return -1;
        s->fd = (int)PyInt_AsLong(fdobj);
        Py_DECREF(fdobj);
        /* fd can never be -1 */
        if (s->fd == -1) {
            if (PyErr_Occurred() == NULL)
                PyErr_SetString(PyExc_ValueError, "invalid file descriptor");
            return -1;
        }
    } else {
        s->fd = -1;
    }
    return 0;
}

static void fadvise_done(int fd, ssize_t ofs)
{
#ifdef POSIX_FADV_DONTNEED
    posix_fadvise(fd, 0, ofs, POSIX_FADV_DONTNEED);
#endif
}

/* Note we don't report progress when looping as we won't have read any bytes */
static int readiter_iternext(readiter_state *s, Buf *buf)
{
    PyObject *bytesobj = NULL;

    if (s->progressfn != NULL) {
        PyObject *tmp = PyObject_CallFunction(
            s->progressfn, "nn", s->filenum, s->prevread);
        if (tmp == NULL)
            return 0;
        Py_DECREF(tmp);
    }

    int realfile = (s->fd != -1);

    if (s->ofs > 1024*1024 && realfile)
        fadvise_done(s->fd, s->ofs - 1024*1024);

    // For reading into the Buf
    unsigned char *rbuf;
    ssize_t rlen;
    // For dealing with python file-like objects
    char *pybuf;
    Py_ssize_t pylen;
    while (1) {
        Buf_prepput(buf, BLOB_READ_SIZE, &rbuf);
        if (realfile) {
            rlen = read(s->fd, rbuf, BLOB_READ_SIZE);
        } else { /* Not a real file TODO: use PyObject_CallMethodObjArgs */
            bytesobj = PyObject_CallMethod(
                s->curfile, "read", "n", BLOB_READ_SIZE, NULL);
            if (bytesobj == NULL)
                return 0;
            rlen = PyString_GET_SIZE(bytesobj);
        }

        if (rlen > 0) {
            s->prevread = rlen;
            s->ofs += rlen;
            Buf_haveput(buf, rlen);
            if (!realfile) {
                if (PyString_AsStringAndSize(bytesobj, &pybuf, &pylen) == -1) {
                    Py_DECREF(bytesobj);
                    return 0;
                }
                memcpy(rbuf, pybuf, pylen);
                Py_DECREF(bytesobj);
                assert(pylen == rlen);
            }
            return 1;
        } else if (rlen == 0) {
            if (realfile) {
                fadvise_done(s->fd, s->ofs);
            } else {
                Py_DECREF(bytesobj);
            }
            if (next_file(s) == -1)
                return 0;
            s->ofs = 0;
            s->filenum++;
            realfile = (s->fd != -1);
            /* Don't recurse to avoid stack overflow, loop instead */
        } else {
            if (!realfile)
                Py_DECREF(bytesobj);
            PyErr_SetString(PyExc_IOError, "failed to read file");
            return 0;
        }
    }
}

static readiter_state *readiter_new(PyObject *fileiter, PyObject *progressfn)
{
    readiter_state *s = calloc(1, sizeof(readiter_state));

    s->fileiter = fileiter;
    s->ofs = 0;
    s->filenum = 0;
    s->progressfn = progressfn;
    s->prevread = 0;
    s->curfile = NULL;
    /* Will initialise fd and curfile with the first file */
    if (next_file(s) == -1)
        return NULL;

    Py_INCREF(fileiter);
    Py_XINCREF(progressfn);

    return s;
}

static void readiter_del(readiter_state *s)
{
    Py_DECREF(s->fileiter);
    Py_XDECREF(s->curfile);
    Py_XDECREF(s->progressfn);
}


/********************************************************/
/********************************************************/


typedef struct {
    unsigned char *buf;
    size_t len;
    int level;
} buf_and_level;

typedef struct {
    Buf *bufobj;
    int basebits;
    int fanbits;
    Rollsum roll;
    // For getting new data to roll over
    readiter_state *readiter;
    int readiter_done;
} splitbuf_state;

static int splitbuf_iternext(splitbuf_state *s, buf_and_level *retval)
{
    int bits = -1;
    int level;

    // Fill up buffer to ensure we never terminate our roll before finding
    // an ofs
    while (Buf_used(s->bufobj) < BLOB_MAX && !s->readiter_done) {
        if (!readiter_iternext(s->readiter, s->bufobj)) {
            if (PyErr_Occurred()) {
                return 0;
            } else {
                s->readiter_done = 1;
            }
        }
    }

    // Find next split point, stopping at BLOB_MAX or end of buffer (whichever
    // comes first) if we don't find one
    size_t bufpeeklen;
    Buf_peek(s->bufobj, BLOB_MAX, &retval->buf, &bufpeeklen);
    if (!bufpeeklen)
        return 0;
    rollsum_init(&s->roll);
    retval->len = bupsplit_next_ofs(&s->roll, retval->buf, bufpeeklen, &bits);

    // Didn't find a split point, i.e. hit BLOB_MAX bytes or end of buffer
    if (!retval->len) {
        retval->len = bufpeeklen;
        bits = 0;
        level = 0;
    }

    Buf_eat(s->bufobj, retval->len);
    if (bits) {
        assert(bits >= BUP_BLOBBITS);
        level = (bits - s->basebits) / s->fanbits;
    }
    retval->level = level;
    return 1;
}

static splitbuf_state *
splitbuf_new(readiter_state *readiter, int basebits, int fanbits)
{
    assert(BLOB_MAX <= INT_MAX);
    splitbuf_state *s = calloc(1, sizeof(splitbuf_state));
    Buf *bufobj = Buf_new();
    s->bufobj = bufobj;
    s->readiter = readiter;
    s->basebits = basebits;
    s->fanbits = fanbits;
    s->readiter_done = 0;
    return s;
}

static void splitbuf_del(splitbuf_state *s)
{
    readiter_del(s->readiter);
    Buf_del(s->bufobj);
    free(s);
}


/********************************************************/
/********************************************************/


typedef struct {
    PyObject_HEAD
    PyObject *progressfn;
    PyObject *files;
    splitbuf_state *splitbuf;
    buf_and_level ret;
} hashsplit_iter_state;

static PyObject* hashsplit_iter_iternext(PyObject *self)
{
    hashsplit_iter_state *s = (hashsplit_iter_state *)self;

    while (1) {
        if (!splitbuf_iternext(s->splitbuf, &s->ret)) {
            /* Don't recurse (to avoid stack overflow), loop instead */
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;
        }
        PyObject *tmpbuf = PyBuffer_FromMemory(s->ret.buf, s->ret.len);
        return Py_BuildValue("Ni", tmpbuf, s->ret.level);
        //return Py_BuildValue("s#i", s->ret.buf, s->ret.len, s->ret.level);
    }
}

static PyObject *
hashsplit_iter_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *progressfn;
    PyObject *files;

    char *kw[] = {"files", "progress", NULL};
    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "OO:_hashsplit_iter", kw, &files, &progressfn))
        return NULL;

    hashsplit_iter_state *state = (hashsplit_iter_state *)type->tp_alloc(type, 0);
    if (!state)
        return NULL;

    PyObject *basebitsobj = PyObject_CallMethod(imports.helpers, "blobbits", NULL);
    if (basebitsobj == NULL)
        return NULL;
    long basebits = PyInt_AsLong(basebitsobj);
    Py_DECREF(basebitsobj);
    if (basebits == -1)
        return NULL;

    PyObject *fanoutobj = PyObject_GetAttrString(imports.hashsplit, "fanout");
    if (fanoutobj == NULL)
        return NULL;
    long fanout = PyInt_AsLong(fanoutobj);
    Py_DECREF(fanoutobj);
    if (fanout == -1)
        return NULL;
    if (!fanout)
        fanout = 128;
    PyObject *fanbitsobj = PyObject_CallMethod(imports.math, "log", "ii", fanout, 2);
    if (fanbitsobj == NULL)
        return NULL;
    PyObject *fanbitsintobj = PyNumber_Int(fanbitsobj);
    Py_DECREF(fanbitsobj);
    if (fanbitsintobj == NULL)
        return NULL;
    long fanbits = PyInt_AsLong(fanbitsintobj);
    Py_DECREF(fanbitsintobj);
    if (fanbits == -1)
        return NULL;

    // INITIALISE THE READFILE ITERATOR

    /* We expect an argument that supports the iterator protocol, but we might
     * need to convert it from e.g. a list first
     */
    PyObject *fileiter = PyObject_GetIter(files);
    if (fileiter == NULL)
        return NULL;
    if (!PyIter_Check(fileiter)) {
        PyErr_SetString(PyExc_TypeError,
            "readfile_iter() expects an iterator of files");
        return NULL;
    }
    if (progressfn == Py_None) {
        progressfn = NULL;
    }
    if (progressfn != NULL && !PyCallable_Check(progressfn)) {
        PyErr_SetString(PyExc_TypeError,
            "readfile_iter() expects a callable progress function");
        return NULL;
    }
    // readiter now owns fileiter and progressfn
    readiter_state *readiter = readiter_new(fileiter, progressfn);
    if (readiter == NULL)
        return NULL;

    // The splitbuf owns the readiter now
    state->splitbuf = splitbuf_new(readiter, basebits, fanbits);

    return (PyObject *)state;
}

static void hashsplit_iter_dealloc(PyObject *iterstate)
{
    hashsplit_iter_state *state = (hashsplit_iter_state *)iterstate;
    splitbuf_del(state->splitbuf);
    Py_TYPE(iterstate)->tp_free(iterstate);
}

static PyTypeObject hashsplit_iter = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_hashsplit_iter",         /* tp_name */
    sizeof(hashsplit_iter_state), /* tp_basicsize */
    0,                         /* tp_itemsize */
    hashsplit_iter_dealloc,    /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    0,                         /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    PyObject_SelfIter,         /* tp_iter */
    hashsplit_iter_iternext,   /* tp_iternext */
    0,                         /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    PyType_GenericAlloc,       /* tp_alloc */
    hashsplit_iter_new         /* tp_new */
};


/********************************************************/
/********************************************************/


static PyMethodDef hashsplit_methods[] = {
    { NULL, NULL, 0, NULL },  // sentinel
};

PyMODINIT_FUNC init_hashsplit(void)
{
    PyObject *m = Py_InitModule("_hashsplit", hashsplit_methods);
    if (m == NULL)
        return;

    PyObject *helpersstr = PyString_FromString("bup._helpers");
    if (helpersstr == NULL)
        return;
    PyObject *helpersmod = PyImport_Import(helpersstr);
    Py_DECREF(helpersstr);
    if (helpersmod == NULL)
        return;
    imports.helpers = helpersmod;

    PyObject *mathstr = PyString_FromString("math");
    if (mathstr == NULL)
        return;
    PyObject *mathmod = PyImport_Import(mathstr);
    Py_DECREF(mathstr);
    if (mathmod == NULL)
        return;
    imports.math = mathmod;

    PyObject *hsstr = PyString_FromString("bup.hashsplit");
    if (hsstr == NULL)
        return;
    PyObject *hsmod = PyImport_Import(hsstr);
    Py_DECREF(hsstr);
    if (hsmod == NULL)
        return;
    imports.hashsplit = hsmod;

    if (PyType_Ready(&hashsplit_iter) < 0)
        return;
    Py_INCREF((PyObject *)&hashsplit_iter);
    // TODO: check for error below
    // TODO: double check all Py_INCREFs look reasonable
    PyModule_AddObject(m, "_hashsplit_iter", (PyObject *)&hashsplit_iter);
}
