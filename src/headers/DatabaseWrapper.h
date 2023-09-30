#include <iostream>
#include <vector>
#include <torch/torch.h>

class DatabaseWrapper {
public:
    torch::Tensor keys;
    torch::Tensor values;
    bool is_empty;

    DatabaseWrapper() : keys(),values(),is_empty(true) {}

    void add(float* key_ptr, float* value_ptr, int embedding_size, int n_keys) {
        auto _keys = torch::from_blob(key_ptr, {n_keys, embedding_size}).clone();
        auto _values = torch::from_blob(value_ptr, {n_keys, embedding_size}).clone();
        if (is_empty){
            keys = _keys;
            values = _values;
            is_empty = false;
            return;
        }
        else{
            keys = torch::cat({keys, _keys}, 0);
            values = torch::cat({values, _values}, 0);
        }
    }

    void query(float* query_ptr, int num_results, float* result_ptr, int n_queries, int emb_dim) {
        auto query_tensor = torch::from_blob(query_ptr, {n_queries, emb_dim});
        auto result = torch::from_blob(result_ptr, {n_queries, num_results, emb_dim});

        auto dot_prods = query_tensor.matmul(keys.transpose(0,1));
        //dot_prods is of shape [n_queries, n_keys]

        //Using the following we will calculate the top num_results dot products
        // inline ::std::tuple<torch::Tensor, torch::Tensor> topk(intf64_t k, intf64_t dim = -1, bool largest = true, bool sorted = true) const
        torch::Tensor topk_values, topk_indices;
        std::tie(topk_values, topk_indices) = dot_prods.topk(num_results, 1, true, true);
        for (int query_idx = 0; query_idx < n_queries; query_idx++){
            for (int result_idx = 0; result_idx < num_results; result_idx++){
                int key_idx = topk_indices[query_idx][result_idx].item<int>();
                result[query_idx][result_idx] = values[key_idx];
            }
        }
        std::cout << "result" << std::endl;
        std::cout << result << std::endl;
    }
    
};
