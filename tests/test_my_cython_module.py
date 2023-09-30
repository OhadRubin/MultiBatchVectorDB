# python3 -m unittest /home/ohadr/mutliDB/tests/test_my_cython_module.py
import unittest
import numpy as np
from my_cython_module import VectorDatabase
import torch

class TestVectorDatabase(unittest.TestCase):

    # def test_init(self):
    #     vec_db = VectorDatabase(num_consumers=2, num_results=5, emb_dim=3, n_vecs_per_consumer=1)

    #     self.assertEqual(vec_db.num_consumers, 2)
    #     self.assertEqual(vec_db.num_results, 5)
    #     self.assertEqual(vec_db.emb_dim, 3)

    # def test_batch_reset(self):
    #     vec_db = VectorDatabase(num_consumers=2, num_results=5, emb_dim=3)
    #     vec_db.batch_reset([0])
        # Add test to confirm if the database at index 0 is reset

    def test_batch_insert(self):
        vec_db = VectorDatabase(batch_size=1, dim=3)
        key_vectors = np.diag(np.arange(3,dtype=float)+1).reshape(1,3,3)
        query = np.array([[0, 0, 1],[1, 0, 0],[0, 1, 0]]).reshape([1, 3, 3])
        key_vectors = np.concatenate([key_vectors, query], axis=1)
        value_vectors = np.copy(key_vectors)
        vec_db.batch_insert(key_vectors, value_vectors)
        print(f"{value_vectors=}")
        print(key_vectors)
        print(vec_db.batch_query(query,K=2))
        
if __name__ == '__main__':
    unittest.main()

