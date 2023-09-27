import numpy as np

class Database:
    def __init__(self, emb_dim):
        self.emb_dim = emb_dim
        self.keys = np.zeros((0, emb_dim))
        self.values = np.zeros((0, emb_dim))

    def add(self, key_vector, value_vector):
        self.keys = np.vstack([self.keys, key_vector])
        self.values = np.vstack([self.values, value_vector])

    def query(self, query_vector, num_results):
        dot_products = np.dot(self.keys, query_vector)
        sorted_indices = np.argsort(dot_products)[::-1]
        closest_indices = sorted_indices[:num_results]
        return self.values[closest_indices]
