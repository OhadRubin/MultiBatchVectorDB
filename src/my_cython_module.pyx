

from libcpp.vector cimport vector
from libc.stdlib cimport malloc, free
from multiprocessing import cpu_count
import numpy as np
import torch

# void add(float* key_ptr, float* value_ptr, int embedding_size, int n_keys) {
# in cython  is a pointer to a float and we use the memoryview to access the data
# in function definitons and cdef instead of float* we write vector[float] which is a memoryview
cdef extern from "DatabaseWrapper.h":
    cdef cppclass DatabaseWrapper:
        DatabaseWrapper()
        void add(float*, float*, int, int)
        void query(float*, int, float*, int, int)

cdef class VectorDatabase:
    cdef int _batch_size, _dim
    cdef int _n_vecs_per_consumer
    cdef vector[DatabaseWrapper] _dbs

    def __cinit__(self, int batch_size, int dim):
        self._batch_size = batch_size
        self._dim = dim
        self._dbs.resize(batch_size)  # Initialize the C++ vector
        for i in range(batch_size):
            self._dbs[i] = DatabaseWrapper()  # Populate the C++ vector


    def batch_insert(self, key_vectors, value_vectors):
        key_vectors = torch.from_numpy(key_vectors.astype(np.float32))
        value_vectors = torch.from_numpy(value_vectors.astype(np.float32))

        assert self._batch_size==key_vectors.shape[0] 
        assert self._dim==key_vectors.shape[2] 

        n_keys = key_vectors.shape[1] 
        cdef unsigned long addr_1
        cdef unsigned long addr_2
        
        for i in range(self._batch_size):
            addr_1 = key_vectors[i].data_ptr()
            addr_2 = value_vectors[i].data_ptr()
            self._dbs[i].add(<float*> addr_1,
                            <float*> addr_2,
                            self._dim, n_keys)

    def batch_query(self, query_vectors, K):

        _query_vectors = torch.from_numpy(query_vectors.astype(np.float32))
        batch_size, n_queries, emb_dim = _query_vectors.shape
        assert self._batch_size==batch_size
        assert self._dim==emb_dim
        result = torch.from_numpy(np.zeros((self._batch_size, n_queries, K, self._dim), dtype=np.float32))
        cdef unsigned long addr_1
        cdef unsigned long addr_2
        cdef vector[float*] q_L_vec 
        cdef vector[float*] r_L_vec 
        for k in range(self._batch_size):
            addr_1 = _query_vectors[k].data_ptr()
            addr_2 = result[k].data_ptr()
            q_L_vec.push_back(<float*> addr_1)
            r_L_vec.push_back(<float*> addr_2)

        for k in range(self._batch_size):
            self._dbs[k].query(q_L_vec[k],
                                K,
                                r_L_vec[k],
                                n_queries, emb_dim)
        return result
    @property
    def batch_size(self):
        return self._batch_size
