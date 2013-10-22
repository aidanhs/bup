#define _LARGEFILE64_SOURCE 1
#define PY_SSIZE_T_CLEAN 1
#undef NDEBUG
#include "../../config/config.h"

// According to Python, its header has to go first:
//   http://docs.python.org/2/c-api/intro.html#include-files
#include <Python.h>

typedef struct {
    PyObject_HEAD
    int fd;
    long long ofs;
    char buf[1024*1024];
} spam_MyIter;

PyObject* spam_MyIter_iter(PyObject *self)
{
    Py_INCREF(self);
    return self;
}

PyObject* spam_MyIter_iternext(PyObject *self)
{
    spam_MyIter *p = (spam_MyIter *)self;
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

static PyTypeObject spam_MyIterType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "_hashsplit._MyIter",        /*tp_name*/
    sizeof(spam_MyIter),       /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    0,                         /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_ITER,
      /* tp_flags: Py_TPFLAGS_HAVE_ITER tells python to
         use tp_iter and tp_iternext fields. */
    "Internal myiter iterator object.",           /* tp_doc */
    0,  /* tp_traverse */
    0,  /* tp_clear */
    0,  /* tp_richcompare */
    0,  /* tp_weaklistoffset */
    spam_MyIter_iter,  /* tp_iter: __iter__() method */
    spam_MyIter_iternext  /* tp_iternext: next() method */
};

static PyObject *
spam_myiter(PyObject *self, PyObject *args)
{
    spam_MyIter *p;

    int fd = -1;
    if (!PyArg_ParseTuple(args, "i", &fd))
        return NULL;

    /* I don't need python callable __init__() method for this iterator,
       so I'll simply allocate it as PyObject and initialize it by hand. */

    p = PyObject_New(spam_MyIter, &spam_MyIterType);
    if (!p) return NULL;

    /* I'm not sure if it's strictly necessary. */
    if (!PyObject_Init((PyObject *)p, &spam_MyIterType)) {
        Py_DECREF(p);
        return NULL;
    }

    p->fd = fd;
    p->ofs = 0;
    return (PyObject *)p;
}

static PyMethodDef hashsplit_methods[] = {
    {"myiter",  spam_myiter, METH_VARARGS, "Iterate from i=0 while i<m."},
    { NULL, NULL, 0, NULL },  // sentinel
};


PyMODINIT_FUNC init_hashsplit(void)
{
    spam_MyIterType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&spam_MyIterType) < 0)  return;

    PyObject *m = Py_InitModule("_hashsplit", hashsplit_methods);
    if (m == NULL)
        return;

    Py_INCREF(&spam_MyIterType);
    PyModule_AddObject(m, "_MyIter", (PyObject *)&spam_MyIterType);
}
