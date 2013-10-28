#define _LARGEFILE64_SOURCE 1
#define PY_SSIZE_T_CLEAN 1
#undef NDEBUG
#include "../../config/config.h"

// According to Python, its header has to go first:
//   http://docs.python.org/2/c-api/intro.html#include-files
#include <Python.h>

typedef struct {
    PyObject_HEAD
    long fd;
    long long ofs;
    PyObject *file;
    char buf[1024*1024];
} readfile_iter_state;

PyObject* readfile_iter_iternext(PyObject *self)
{
    readfile_iter_state *p = (readfile_iter_state *)self;
    ssize_t num = read(p->fd, p->buf, 1024*1024);
    if (num > 0) {
        p->ofs += num;
        PyObject *tmp = Py_BuildValue("s#", p->buf, num);
        return tmp;
    } else if (num == 0) {
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
    PyObject *file;
    long fd = -1;

    if (!PyArg_ParseTuple(args, "O:fread", &file))
        return NULL;

    PyObject *fileno = PyObject_CallMethod(file, "fileno", NULL);
    if (fileno == NULL)
        return NULL;

    fd = PyInt_AsLong(fileno);
    Py_DECREF(fileno);
    if (fd == -1)
        return NULL;

    readfile_iter_state *freadstate = (readfile_iter_state *)type->tp_alloc(type, 0);
    if (!freadstate)
        return NULL;

    Py_INCREF(file);
    freadstate->file = file;
    freadstate->fd = fd;
    freadstate->ofs = 0;

    return (PyObject *)freadstate;
}

static void
fread_dealloc(PyObject *freadstate)
{
    Py_DECREF(((readfile_iter_state *)freadstate)->file);
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
