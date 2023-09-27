import unittest
import numpy as np
from src.database import Database

class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.db = Database(emb_dim=3)
        self.keys = np.diag([1, 1, 1])
        self.values = np.array([[1, 1, 1],
                                [2, 2, 2],
                                [3, 3, 3]])

    def test_add(self):
        for i in range(3):
            self.db.add(self.keys[i], self.values[i])
        np.testing.assert_array_equal(self.db.keys, self.keys)
        np.testing.assert_array_equal(self.db.values, self.values)

    def test_query(self):
        for i in range(3):
            self.db.add(self.keys[i], self.values[i])
        
        query_vector = np.array([1, 0, 0])
        expected_result = np.array([[1, 1, 1]])
        result = self.db.query(query_vector, 1)
        np.testing.assert_array_equal(result, expected_result)

# python3 -m unittest tests/test_database.py
if __name__ == '__main__':
    unittest.main()
