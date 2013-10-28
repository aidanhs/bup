#define _LARGEFILE64_SOURCE 1
#define PY_SSIZE_T_CLEAN 1
#undef NDEBUG
#include "../../config/config.h"

// According to Python, its header has to go first:
//   http://docs.python.org/2/c-api/intro.html#include-files
#include <Python.h>

typedef struct {
    PyObject_HEAD
    long *fds;
    long long ofs;
    Py_ssize_t curfile;
    Py_ssize_t numfiles;
    PyObject *files;
    char buf[1024*1024];
} readfile_iter_state;

PyObject* readfile_iter_iternext(PyObject *self)
{
    readfile_iter_state *p = (readfile_iter_state *)self;
    ssize_t num = read(p->fds[p->curfile], p->buf, 1024*1024);
    if (num > 0) {
        p->ofs += num;
        PyObject *tmp = Py_BuildValue("s#", p->buf, num);
        return tmp;
    } else if (num == 0 && p->curfile != p->numfiles - 1) { /* End of file */
        p->ofs = 0;
        p->curfile = p->curfile + 1;
        return readfile_iter_iternext(self);
    } else if (num == 0) { /* End of the file */
        /* Raising of standard StopIteration exception with empty value. */
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    } else {
        // TODO: raise exception
        return NULL;
    }
}

static PyObject *
readfile_iter_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *files;

    if (!PyArg_ParseTuple(args, "O:fread", &files))
        return NULL;

    /* We expect an argument that supports the sequence protocol */
    if (!PySequence_Check(files)) {
        PyErr_SetString(PyExc_TypeError, "readfile_iter() expects a sequence");
        return NULL;
    }

    Py_ssize_t len = PySequence_Length(files);
    if (len == -1)
        return NULL;

    long *fds = calloc(sizeof(long), len);
    Py_ssize_t i;
    PyObject *fileno;
    PyObject *file;
    for (i = 0; i < len; i++) {
        file = PySequence_GetItem(files, i);
        if (file == NULL)
            return NULL;
        fileno = PyObject_CallMethod(file, "fileno", NULL);
        Py_DECREF(file);
        if (fileno == NULL)
            return NULL;
        fds[i] = PyInt_AsLong(fileno);
        Py_DECREF(fileno);
        /* Assume error on -1 because this is a file descriptor */
        if (fds[i] == -1)
            return NULL;
    }

    readfile_iter_state *freadstate =
        (readfile_iter_state *)type->tp_alloc(type, 0);
    if (!freadstate)
        return NULL;

    /* Make sure python doesn't garbage collect and close the files */
    Py_INCREF(files);
    freadstate->files = files;
    freadstate->fds = fds;
    freadstate->ofs = 0;
    freadstate->curfile = 0;
    freadstate->numfiles = len;

    return (PyObject *)freadstate;
}

static void
fread_dealloc(PyObject *freadstate)
{
    readfile_iter_state *state = (readfile_iter_state *)freadstate;
    free(state->fds);
    Py_DECREF(state->files);
    Py_TYPE(freadstate)->tp_free(freadstate);
}

static PyTypeObject readfile_iter = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
    "readfile_iter",           /* tp_name */
    sizeof(readfile_iter_state), /* tp_basicsize */
    0,                         /* tp_itemsize */
    fread_dealloc,             /* tp_dealloc */
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
    Py_INCREF((PyObject *)&readfile_iter);
    PyModule_AddObject(m, "readfile_iter", (PyObject *)&readfile_iter);
}
