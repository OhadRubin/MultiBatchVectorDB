

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
        void add(float*, float*,int,int)
        void query(float*, int, float*, int, int)

cdef class VectorDatabase:
    cdef int _num_consumers, _num_results, _emb_dim
    cdef int _n_vecs_per_consumer
    cdef vector[DatabaseWrapper] _dbs

    def __cinit__(self, int num_consumers, int num_results):
        self._num_consumers = num_consumers
        self._num_results = num_results
        self._dbs.resize(num_consumers)  # Initialize the C++ vector
        for i in range(num_consumers):
            self._dbs[i] = DatabaseWrapper()  # Populate the C++ vector


    def batch_insert(self, key_vectors, value_vectors):
        key_vectors= key_vectors.astype(np.float32)
        value_vectors= value_vectors.astype(np.float32)
        key_vectors = torch.from_numpy(key_vectors)
        value_vectors = torch.from_numpy(value_vectors)
        batch_size = key_vectors.shape[0] 
        n_keys = key_vectors.shape[1] 
        emb_dim = key_vectors.shape[2] 
        cdef unsigned long addr_1
        cdef unsigned long addr_2
        
        
        for i in range(self._num_consumers):
            addr_1 = key_vectors[i].data_ptr()
            addr_2 = value_vectors[i].data_ptr()
            self._dbs[i].add(<float*> addr_1,
                            <float*> addr_2,
                            emb_dim, n_keys)

    def batch_query(self, query_vectors):
        query_vectors = query_vectors.astype(np.float32)
        _query_vectors = torch.from_numpy(query_vectors)
        n_queries = _query_vectors.shape[0]
        emb_dim = _query_vectors.shape[1]
        result = np.zeros((self._num_consumers, n_queries, self._num_results), dtype=np.float32)
        result = torch.from_numpy(result)
        cdef unsigned long addr_1
        cdef unsigned long addr_2

        for k in range(self._num_consumers * n_queries):
            addr_1 = _query_vectors[k].data_ptr()
            addr_2 = result[k].data_ptr()
            self._dbs[k].query(<float*>  addr_1,
                                self._num_results,
                                <float*> addr_2,
                                n_queries, emb_dim)
        return result
    @property
    def num_consumers(self):
        return self._num_consumers
    @property
    def num_results(self):
        return self._num_results
    @property
    def emb_dim(self):
        return self._emb_dim
    @property
    def n_vecs_per_consumer(self):
        return self._n_vecs_per_consumer

