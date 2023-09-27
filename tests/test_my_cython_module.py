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
        vec_db = VectorDatabase(num_consumers=1, num_results=1)
        key_vectors = np.diag(np.ones(3)).reshape(1,3,3)
        value_vectors = np.diag(np.arange(3,dtype=float)+1).reshape(1,3,3)
        print(key_vectors)
        print(value_vectors)
        vec_db.batch_insert(key_vectors, value_vectors)
        query = np.array([1.0, 1.0, 1.0]).reshape(1,3)
        print(vec_db.batch_query(query))
    # Additional tests for batch_query, etc.
        
if __name__ == '__main__':
    unittest.main()


# import my_cython_module  # Import your compiled Cython module
# import numpy as np  # To create example data

# # Number of consumers, number of results, embedding dimensions, and number of vectors per consumer
# num_consumers = 4
# num_results = 10
# emb_dim = 128
# n_vecs_per_consumer = 1000

# # Initialize VectorDatabase object
# db = my_cython_module.VectorDatabase(num_consumers, num_results, emb_dim, n_vecs_per_consumer)

# # Create example data for batch insert
# # Assuming each consumer has 'n_vecs_per_consumer' number of vectors
# key_vectors = np.random.rand(num_consumers, n_vecs_per_consumer, emb_dim)
# value_vectors = np.random.rand(num_consumers, n_vecs_per_consumer, emb_dim)

# # Perform batch insert
# db.batch_insert(key_vectors, value_vectors)

# # Create example query data
# query_vectors = np.random.rand(num_consumers, n_vecs_per_consumer, emb_dim)

# # Perform batch query
# results = db.batch_query(query_vectors)

# # Assume handle_results is a function you've written to handle results
# # Assuming the handle_results function will free the allocated memory for results
# # handle_results(results)
# print(results)

# # Reset specific consumers if needed
# consumer_ids_to_delete = [0, 2]  # For example, reset consumers with IDs 0 and 2
# db.batch_reset(consumer_ids_to_delete)
