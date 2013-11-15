#define _LARGEFILE64_SOURCE 1
#define PY_SSIZE_T_CLEAN 1
#undef NDEBUG
#include "../../config/config.h"

// According to Python, its header has to go first:
//   http://docs.python.org/2/c-api/intro.html#include-files
#include <Python.h>

#include "bupsplit.h"

#define BLOB_READ_SIZE 1024*1024
#define BLOB_MAX 8192*4
#define FANOUT 16

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
    char buf[BLOB_READ_SIZE];
} readfile_iter_state;

// Return -1 if the next file cannot be obtained (end of iter or error)
static int next_file(readfile_iter_state *s)
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
static PyObject* readfile_iter_iternext(PyObject *self)
{
    readfile_iter_state *s;
    ssize_t bytes_read;
    PyObject *bytesobj = NULL;

    s = (readfile_iter_state *)self;
    if (s->progressfn != NULL)
        PyObject_CallFunction(s->progressfn, "nn", s->filenum, s->prevread);
    if (s->ofs > 1024*1024)
        fadvise_done(s->fd, s->ofs - 1024*1024);

    while (1) {
        int realfile = (s->fd != -1);
        if (realfile) {
            bytes_read = read(s->fd, s->buf, BLOB_READ_SIZE);
        } else { /* Not a real file TODO: use PyObject_CallMethodObjArgs */
            bytesobj = PyObject_CallMethod(
                s->curfile, "read", "n", BLOB_READ_SIZE, NULL);
            if (bytesobj == NULL)
                return NULL;
            bytes_read = PyString_GET_SIZE(bytesobj);
        }
        if (bytes_read > 0) {
            s->prevread = bytes_read;
            s->ofs += bytes_read;
            if (realfile) {
                return Py_BuildValue("s#", s->buf, bytes_read);
            } else {
                return bytesobj;
            }
        } else if (bytes_read == 0) {
            if (realfile)
                fadvise_done(s->fd, s->ofs);
            if (next_file(s) != -1) { /* End of file */
                s->ofs = 0;
                s->filenum++;
                /* Don't recurse to avoid stack overflow, loop instead */
            } else {
                // TODO: do we need to check for error?
                if (PyErr_Occurred() == NULL)
                    PyErr_SetNone(PyExc_StopIteration);
                return NULL;
            }
        } else {
            PyErr_SetString(PyExc_IOError, "failed to read file");
            return NULL;
        }
    }
}

static PyObject *
readfile_iter_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *iterarg;
    PyObject *progressfn = NULL;

    char *kw[] = {"files", "progress", NULL};
    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "O|O:readfile_iter", kw, &iterarg, &progressfn))
        return NULL;

    /* We expect an argument that supports the iterator protocol, but we might
     * need to convert it from e.g. a list first
     */
    PyObject *fileiter = PyObject_GetIter(iterarg);
    if (fileiter == NULL)
        return NULL;
    if (!PyIter_Check(fileiter)) {
        PyErr_SetString(PyExc_TypeError,
            "readfile_iter() expects an iterator of files");
        return NULL;
    }

    if (progressfn == Py_None) {
        Py_DECREF(progressfn);
        progressfn = NULL;
    }
    if (progressfn != NULL && !PyCallable_Check(progressfn)) {
        PyErr_SetString(PyExc_TypeError,
            "readfile_iter() expects a callable progress function");
        return NULL;
    }

    readfile_iter_state *state =
        (readfile_iter_state *)type->tp_alloc(type, 0);
    if (!state)
        return NULL;

    state->fileiter = fileiter;
    state->ofs = 0;
    state->filenum = 0;
    state->progressfn = progressfn;
    state->prevread = 0;
    state->curfile = NULL;
    /* Will initialise fd and curfile with the first file */
    if (next_file(state) == -1)
        return NULL;

    Py_INCREF(fileiter);
    Py_XINCREF(progressfn);

    return (PyObject *)state;
}

static void
readfile_iter_dealloc(PyObject *iterstate)
{
    readfile_iter_state *state = (readfile_iter_state *)iterstate;
    Py_DECREF(state->fileiter);
    Py_XDECREF(state->curfile);
    Py_XDECREF(state->progressfn);
    Py_TYPE(iterstate)->tp_free(iterstate);
}

static PyTypeObject readfile_iter = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
    "readfile_iter",           /* tp_name */
    sizeof(readfile_iter_state), /* tp_basicsize */
    0,                         /* tp_itemsize */
    readfile_iter_dealloc,     /* tp_dealloc */
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
    readfile_iter_iternext,    /* tp_iternext */
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
    readfile_iter_new          /* tp_new */
};

/********************************************************/
/********************************************************/

typedef struct {
    PyObject_HEAD
    PyObject *bufobj;
    int basebits;
    int fanbits;
    unsigned char prevbuf[BLOB_MAX];
    int consumeend;
} splitbuf_state;

static int splitbuf_actual(
    const unsigned char *buf, Py_ssize_t len, int *ofsptr, int *bitsptr)
{
    int ofs = 0, bits = -1;

    assert(len <= INT_MAX);
    ofs = bupsplit_find_ofs(buf, len, &bits);
    if (ofs)
        assert(bits >= BUP_BLOBBITS);
    *ofsptr = ofs;
    *bitsptr = bits;
    return 0;
}

static PyObject* splitbuf_consumeend(PyObject *self)
{
    splitbuf_state *s = (splitbuf_state *)self;
    PyObject *retbuf, *bufused;

    bufused = PyObject_CallMethod(s->bufobj, "used", NULL);
    if (bufused == NULL)
        return NULL;
    Py_ssize_t used = PyInt_AsSsize_t(bufused);
    Py_DECREF(bufused);
    // There can never be a negative size in the buffer
    if (used == -1)
        return NULL;
    if (used >= BLOB_MAX) {
        retbuf = PyObject_CallMethod(s->bufobj, "get", "i", BLOB_MAX, NULL);
        return Py_BuildValue("Ni", retbuf, 0);
    } else {
        // TODO: do we need to check if error occurred?
        if (PyErr_Occurred() == NULL)
            PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
}

static PyObject* splitbuf_iternext(PyObject *self)
{
    splitbuf_state *s = (splitbuf_state *)self;
    PyObject *bufused;
    PyObject *bufpeekobj;
    const unsigned char *bufpeekbytes;
    Py_ssize_t bufpeeklen;
    int ofs, bits;
    int level;

    if (s->consumeend)
        return splitbuf_consumeend(self);

    bufused = PyObject_CallMethod(s->bufobj, "used", NULL);
    if (bufused == NULL)
        return NULL;
    bufpeekobj = PyObject_CallMethod(s->bufobj, "peek", "O", bufused);
    Py_DECREF(bufused);
    // Cleaned up when bufpeekobj is
    if (PyObject_AsCharBuffer(
            bufpeekobj, ((const char **)&bufpeekbytes), &bufpeeklen) == -1)
        return NULL;
    if (splitbuf_actual(bufpeekbytes, bufpeeklen, &ofs, &bits) == -1)
        return NULL;
    if (ofs > BLOB_MAX)
        ofs = BLOB_MAX;
    if (ofs) {
        PyObject *tmp = PyObject_CallMethod(s->bufobj, "eat", "i", ofs);
        if (tmp == NULL)
            return NULL;
        Py_DECREF(tmp);
        level = (bits - s->basebits) / s->fanbits;
        memcpy(s->prevbuf, bufpeekbytes, ofs);
        Py_DECREF(bufpeekobj);
        PyObject *retbuf = PyBuffer_FromMemory(s->prevbuf, ofs);
        if (retbuf == NULL)
            return NULL;
        return Py_BuildValue("Ni", retbuf, level);
    } else {
        Py_DECREF(bufpeekobj);
        s->consumeend = 1;
        return splitbuf_consumeend(self);
    }
}

static PyObject *
splitbuf_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *bufobj;
    int basebits;
    int fanbits;

    if (!PyArg_ParseTuple(args, "Oii:_splitbuf", &bufobj, &basebits, &fanbits))
        return NULL;

    /* TODO: don't assume this is a buf object */

    splitbuf_state *state = (splitbuf_state *)type->tp_alloc(type, 0);
    if (!state)
        return NULL;

    Py_INCREF(bufobj);
    state->bufobj = bufobj;
    state->basebits = basebits;
    state->fanbits = fanbits;
    state->consumeend = 0;

    return (PyObject *)state;
}

static void
splitbuf_dealloc(PyObject *iterstate)
{
    splitbuf_state *state = (splitbuf_state *)iterstate;
    Py_DECREF(state->bufobj);
    Py_TYPE(iterstate)->tp_free(iterstate);
}

static PyTypeObject splitbuf = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
    "_splitbuf",               /* tp_name */
    sizeof(splitbuf_state),    /* tp_basicsize */
    0,                         /* tp_itemsize */
    splitbuf_dealloc,          /* tp_dealloc */
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
    splitbuf_iternext,         /* tp_iternext */
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
    splitbuf_new               /* tp_new */
};

/********************************************************/
/********************************************************/


typedef struct {
    PyObject_HEAD
    PyObject *progressfn;
    PyObject *files;
    PyObject *basebitsobj;
    PyObject *fanbitsobj;
    PyObject *bufobj;
    // For holding iterator states
    PyObject *readfile_iter;
    PyObject *splitbuf;
    // To create new iterators
    PyObject *splitbuffn;
} hashsplit_iter_state;

static PyObject* hashsplit_iter_iternext(PyObject *self)
{
    hashsplit_iter_state *s = (hashsplit_iter_state *)self;

    while (1) {

        // End of the line for this iterator
        if (s->readfile_iter == NULL) {
            if (s->bufobj == NULL) {
                // TODO: do we need to check for error?
                if (PyErr_Occurred() == NULL) {
                    PyErr_SetNone(PyExc_StopIteration);
                }
                return NULL;
            }
            PyObject *usedobj = PyObject_CallMethod(s->bufobj, "used", NULL);
            if (usedobj == NULL)
                return NULL;
            int used = PyInt_AsLong(usedobj);
            if (used == -1)
                return NULL;
            PyObject *retbuf = PyObject_CallMethod(s->bufobj, "get", "O", usedobj);
            Py_DECREF(usedobj);
            if (retbuf == NULL)
                return NULL;
            Py_DECREF(s->bufobj);
            s->bufobj = NULL;
            return Py_BuildValue("Ni", retbuf, 0);
        }

        // Start up a new splitbuf iterator
        if (s->splitbuf == NULL) {
            PyObject *inblock = PyIter_Next(s->readfile_iter);
            if (inblock == NULL) {
                if (PyErr_Occurred()) {
                    return NULL;
                } else {
                    // Finished main method loop, time to wrap up
                    Py_DECREF(s->readfile_iter);
                    s->readfile_iter = NULL;
                    continue;
                }
            } else {
                PyObject *tmpobj = PyObject_CallMethod(
                    s->bufobj, "put", "O", inblock);
                Py_DECREF(inblock);
                if (tmpobj == NULL)
                    return NULL;
                Py_DECREF(tmpobj);
                s->splitbuf = PyObject_CallFunctionObjArgs(
                    s->splitbuffn, s->bufobj, s->basebitsobj, s->fanbitsobj, NULL);
                if (s->splitbuf == NULL)
                    return NULL;
            }
        }

        PyObject *bufAndLevel = PyIter_Next(s->splitbuf);
        if (bufAndLevel == NULL) {
            if (PyErr_Occurred()) {
                return NULL;
            } else {
                /* Don't recurse to avoid stack overflow, loop instead */
                Py_DECREF(s->splitbuf);
                s->splitbuf = NULL;
            }
        } else {
            return bufAndLevel;
        }
    }
}

static PyObject *
hashsplit_iter_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *progressfn;
    PyObject *files;
    if (!PyArg_ParseTuple(args, "OO:hashsplit_iter", &files, &progressfn))
        return NULL;

    hashsplit_iter_state *state = (hashsplit_iter_state *)type->tp_alloc(type, 0);
    if (!state)
        return NULL;

    PyObject *helpersstr = PyString_FromString("bup._helpers");
    if (helpersstr == NULL)
        return NULL;
    PyObject *helpersmod = PyImport_Import(helpersstr);
    if (helpersmod == NULL)
        return NULL;
    PyObject *basebitsobj = PyObject_CallMethod(helpersmod, "blobbits", NULL);
    if (basebitsobj == NULL)
        return NULL;
    Py_DECREF(helpersstr);
    Py_DECREF(helpersmod);

    PyObject *mathstr = PyString_FromString("math");
    if (mathstr == NULL)
        return NULL;
    PyObject *mathmod = PyImport_Import(mathstr);
    if (mathmod == NULL)
        return NULL;
    PyObject *fanbitsobj = PyObject_CallMethod(mathmod, "log", "ii", FANOUT, 2);
    if (fanbitsobj == NULL)
        return NULL;
    PyObject *fanbitsintobj = PyNumber_Int(fanbitsobj);
    if (fanbitsintobj == NULL)
        return NULL;
    Py_DECREF(fanbitsobj);
    Py_DECREF(mathstr);
    Py_DECREF(mathmod);

    PyObject *hsstr = PyString_FromString("bup.hashsplit");
    if (hsstr == NULL)
        return NULL;
    PyObject *hsmod = PyImport_Import(hsstr);
    if (hsmod == NULL)
        return NULL;
    PyObject *bufobj = PyObject_CallMethod(hsmod, "Buf", NULL);
    if (bufobj == NULL)
        return NULL;
    PyObject *readfile_iterobj = PyObject_CallMethod(
        hsmod, "readfile_iter", "OO", files, progressfn);
    if (readfile_iterobj == NULL)
        return NULL;
    PyObject *splitbuffn = PyObject_GetAttrString(hsmod, "_splitbuf");
    if (splitbuffn == NULL)
        return NULL;
    Py_DECREF(hsstr);
    Py_DECREF(hsmod);

    Py_INCREF(progressfn);
    state->progressfn = progressfn;
    state->files = files;
    state->basebitsobj = basebitsobj;
    state->fanbitsobj = fanbitsintobj;
    state->bufobj = bufobj;
    state->splitbuf = NULL;
    state->splitbuffn = splitbuffn;
    state->readfile_iter = readfile_iterobj;

    return (PyObject *)state;
}

static void
hashsplit_iter_dealloc(PyObject *iterstate)
{
    hashsplit_iter_state *state = (hashsplit_iter_state *)iterstate;
    Py_DECREF(state->progressfn);
    Py_DECREF(state->files);
    Py_DECREF(state->basebitsobj);
    Py_DECREF(state->fanbitsobj);
    Py_DECREF(state->splitbuffn);
    Py_XDECREF(state->bufobj);
    Py_XDECREF(state->splitbuf);
    Py_XDECREF(state->readfile_iter);
    Py_TYPE(iterstate)->tp_free(iterstate);
}

static PyTypeObject hashsplit_iter = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
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

    if (PyType_Ready(&readfile_iter) < 0)
        return;
    if (PyType_Ready(&splitbuf) < 0)
        return;
    if (PyType_Ready(&hashsplit_iter) < 0)
        return;
    Py_INCREF((PyObject *)&readfile_iter);
    Py_INCREF((PyObject *)&splitbuf);
    Py_INCREF((PyObject *)&hashsplit_iter);
    // TODO: check for error below
    // TODO: double check all Py_INCREFs look reasonable
    PyModule_AddObject(m, "readfile_iter", (PyObject *)&readfile_iter);
    PyModule_AddObject(m, "_splitbuf", (PyObject *)&splitbuf);
    PyModule_AddObject(m, "_hashsplit_iter", (PyObject *)&hashsplit_iter);
}
