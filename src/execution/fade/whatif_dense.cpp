#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include <fstream>
#include <dlfcn.h>
#include <immintrin.h>
#include <thread>
#include <vector>

namespace duckdb {


int join_single(int thread_id, int* __restrict__ lhs_lineage, int* __restrict__  rhs_lineage,
               int8_t* __restrict__ lhs_var,  int8_t* __restrict__ rhs_var,
               int8_t* __restrict__ out, const int start, const int end,
               int left_n_interventions, int right_n_interventions) {
	if (left_n_interventions > 1 && right_n_interventions > 1) {
    for (int i=start; i < end; ++i) {
		  out[i] = lhs_var[lhs_lineage[i]] * rhs_var[rhs_lineage[i]];
    }
  } else if (left_n_interventions > 1) {
    for (int i=start; i < end; ++i) {
		  out[i] = lhs_var[lhs_lineage[i]];
    }
  } else {
    for (int i=start; i < end; ++i) {
		  out[i] = rhs_var[rhs_lineage[i]];
    }
  }
	return 0;
}

int join_dense_simd(int thread_id, int* __restrict__ lhs_lineage, int* __restrict__  rhs_lineage,
                      __mmask16* __restrict__ lhs_var,  __mmask16* __restrict__ rhs_var,
                      __mmask16* __restrict__ out, const int start, const int end, int n_masks,
                      int left_n_interventions, int right_n_interventions) {
	if (left_n_interventions > 1 && right_n_interventions > 1) {
    for (int i=start; i < end; ++i) {
      int lhs_col = lhs_lineage[i] * n_masks;
      int rhs_col = rhs_lineage[i] * n_masks;
      int col = i * n_masks;
      for (int j=0; j < n_masks; j+=32) {
        __m512i a = _mm512_stream_load_si512((__m512i*)&lhs_var[lhs_col+j]);
        __m512i b = _mm512_stream_load_si512((__m512i*)&rhs_var[rhs_col+j]);
        _mm512_store_si512((__m512i*)&out[col+j], _mm512_or_si512(a, b));
      }
    }
  } else if (left_n_interventions > 1) {
    for (int i=start; i < end; ++i) {
      int lhs_col = lhs_lineage[i] * n_masks;
      int col = i * n_masks;
      for (int j=0; j < n_masks; j+=32) {
        __m512i a = _mm512_load_si512((__m512i*)&lhs_var[lhs_col+j]);
        _mm512_store_si512((__m512i*)&out[col+j], a);
      }
    }
  } else {
    for (int i=start; i < end; ++i) {
      int rhs_col = rhs_lineage[i] * n_masks;
      int col = i * n_masks;
      for (int j=0; j < n_masks; j+=32) {
        __m512i b = _mm512_load_si512((__m512i*)&rhs_var[rhs_col+j]);
        _mm512_store_si512((__m512i*)&out[col+j], b);
      }
    }
  }

	return 0;
}


int join_dense_scalar(int thread_id, int* __restrict__ lhs_lineage, int* __restrict__  rhs_lineage,
                      __mmask16* __restrict__ lhs_var,  __mmask16* __restrict__ rhs_var,
                      __mmask16* __restrict__ out, const int start, const int end, int n_masks,
                      int left_n_interventions, int right_n_interventions) {
	if (left_n_interventions > 1 && right_n_interventions > 1) {
    for (int i=start; i < end; ++i) {
      int lhs_col = lhs_lineage[i] * n_masks;
      int rhs_col = rhs_lineage[i] * n_masks;
      int col = i * n_masks;
      for (int j=0; j < n_masks; ++j) {
        out[col+j] = lhs_var[lhs_col+j] | rhs_var[rhs_col+j];
      }
    }
  } else if (left_n_interventions > 1) {
    for (int i=start; i < end; ++i) {
      int lhs_col = lhs_lineage[i] * n_masks;
      int col = i * n_masks;
      for (int j=0; j < n_masks; ++j) {
        out[col+j] = lhs_var[lhs_col+j];
      }
    }
  } else {
    for (int i=start; i < end; ++i) {
      int rhs_col = rhs_lineage[i] * n_masks;
      int col = i * n_masks;
      for (int j=0; j < n_masks; ++j) {
        out[col+j] = rhs_var[rhs_col+j];
      }
    }
  }

	return 0;
}

int filter_single(int thread_id, int* __restrict__ lineage,  int8_t* __restrict__ var,
    int8_t* __restrict__ out,
    const int start, const int end) {
	for (int i=start; i < end; ++i) {
		out[i] = var[lineage[i]];
	}
	return 0;
}

int filter_dense(int thread_id, int* __restrict__ lineage,  __mmask16* __restrict__ var, __mmask16* __restrict__ out,
                 const int start, const int end, int n_masks, int n_intervention, bool is_scalar) {

	if (is_scalar || n_intervention < 512) {
		for (int i=start; i < end; ++i) {
		int col_out = i*n_masks;
		int col_oid = lineage[i]*n_masks;
			for (int j=0; j < n_masks; ++j) {
				out[col_out+j] = var[col_oid+j];
			}
		}
	} else {
		for (int i=start; i < end; ++i) {
		int col_out = i*n_masks;
		int col_oid = lineage[i]*n_masks;
			for (int j=0; j < n_masks; j+=32) {
				__m512i b = _mm512_stream_load_si512((__m512i*)&var[col_oid+j]);
				_mm512_store_si512((__m512i*)&out[col_out+j], b);
			}
		}
	}

	return 0;
}
int groupby_agg_scalar_single(int* lineage, int8_t* __restrict__ var_0,
                void* __restrict__  out,
                std::unordered_map<int, void*>& input_data_map,
                const int start, const int end,
                int n_interventions, 
                int col_idx, string func, string typ) {
	if (func == "count") {
		int* __restrict__ out_int = (int*)out;
    for (int i=start; i < end; ++i) {
      int oid = lineage[i];
      int col = oid * n_interventions;
      out_int[col] += (!var_0[i]);
    }

	} else if (func == "sum" || func == "avg" || func == "stddev") { // sum
		if (typ == "int") {
			int* __restrict__  out_int = (int*)out;
			int* in_arr = reinterpret_cast<int *>(input_data_map[col_idx]);
      for (int i=start; i < end; ++i) {
        int oid = lineage[i];
        int col = oid * n_interventions;
        out_int[col] += in_arr[i] * (!var_0[i]);
      }
		} else {
			float* __restrict__  out_float = (float*)out;
			float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
      for (int i=start; i < end; ++i) {
        int oid = lineage[i];
        int col = oid * n_interventions;
        out_float[col] += in_arr[i] * (!var_0[i]);
      }
		}
	} 
	return 0;
}

int groupby_agg_scalar(int* lineage, __mmask16* __restrict__ var_0,
                void* __restrict__  out,
                std::unordered_map<int, void*>& input_data_map,
                const int start, const int end,
                int n_interventions, int n_masks, int mask_size,
                int col_idx, string func, string typ) {
	if (func == "count") {
		int* __restrict__ out_int = (int*)out;
    for (int i=start; i < end; ++i) {
      int oid = lineage[i];
      int col = oid * n_interventions;
      for (int j = 0; j < n_masks; ++j) {
        int row = j * mask_size;
        __mmask16 tmp_mask = ~var_0[i*n_masks+j];
        for (int k = 0; k < mask_size; ++k) {
          out_int[col + (row+k)] += (1 & tmp_mask >> k);
        }
      }
    }
	} else if (func == "sum" || func == "avg" || func == "stddev") { // sum
		if (typ == "int") {
			int* __restrict__  out_int = (int*)out;
			int* in_arr = reinterpret_cast<int *>(input_data_map[col_idx]);
      for (int i=start; i < end; ++i) {
        int oid = lineage[i];
        int col = oid * n_interventions;
        for (int j = 0; j < n_masks; ++j) {
          int row = j * mask_size;
          __mmask16 tmp_mask = ~var_0[i*n_masks+j];
          for (int k = 0; k < mask_size; ++k) {
            out_int[col + (row+k)] += in_arr[i] * ( 1 & tmp_mask >> k);
          }
        }
      }
		} else {
			float* __restrict__  out_float = (float*)out;
			float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
      for (int i=start; i < end; ++i) {
        int oid = lineage[i];
        int col = oid * n_interventions;
        for (int j = 0; j < n_masks; ++j) {
          int row = j * mask_size;
          __mmask16 tmp_mask = ~var_0[i*n_masks+j];
          for (int k = 0; k < mask_size; ++k) {
            out_float[col + (row+k)] += in_arr[i] * ( 1 & tmp_mask >> k);
          }
        }
      }
		}
	} 
	return 0;
}


void  HashAggregateIntervene2DEval(int thread_id, EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                  std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                                  PhysicalOperator* op, void* var_0) {
	// check if this has child aggregate, if so then get its id
  // TODO: change it to FadeNode
	FadeNode* cur_node = dynamic_cast<FadeNode*>(fade_data[op->id].get());
	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	pair<int, int> start_end_pair = Fade::get_start_end(row_count, thread_id, cur_node->num_worker);
	if (config.debug) std::cout << "Count summary: " << row_count << " "  << " " << start_end_pair.first << " " << start_end_pair.second << " " << config.num_worker << " " << thread_id << std::endl;

  if (cur_node->n_interventions == 1) {
    if (cur_node->has_agg_child) {
    } else {
      if (config.use_gb_backward_lineage) {
      } else {
        int* forward_lineage_ptr = op->lineage_op->forward_lineage[0].data();
        int8_t* annotations_ptr = dynamic_cast<FadeNodeSingle*>(fade_data[fade_data[op->children[0]->id]->opid].get())->single_del_interventions;
        for (auto& out_var : cur_node->alloc_vars_funcs) {
          string func = cur_node->alloc_vars_funcs[out_var.first];
          int col_idx = cur_node->alloc_vars_index[out_var.first];
          string typ = cur_node->alloc_vars_types[out_var.first];
          groupby_agg_scalar_single(forward_lineage_ptr,
                              annotations_ptr,
                              cur_node->alloc_vars[out_var.first][thread_id],
                              cur_node->input_data_map,
                              start_end_pair.first, start_end_pair.second,
                              cur_node->n_interventions,
                              col_idx, func, typ);
        }
      }
    }
  } else {
    if (cur_node->has_agg_child) {
    } else {
      if (config.use_gb_backward_lineage) {
      } else {
        int* forward_lineage_ptr = op->lineage_op->forward_lineage[0].data();
        __mmask16* annotations_ptr = dynamic_cast<FadeNodeDense*>(fade_data[fade_data[op->children[0]->id]->opid].get())->del_interventions;
        idx_t n_masks = dynamic_cast<FadeNodeDense*>(fade_data[op->id].get())->n_masks;
        for (auto& out_var : cur_node->alloc_vars_funcs) {
          string func = cur_node->alloc_vars_funcs[out_var.first];
          int col_idx = cur_node->alloc_vars_index[out_var.first];
          string typ = cur_node->alloc_vars_types[out_var.first];
          groupby_agg_scalar(forward_lineage_ptr,
                              annotations_ptr,
                              cur_node->alloc_vars[out_var.first][thread_id],
                              cur_node->input_data_map,
                              start_end_pair.first, start_end_pair.second,
                              cur_node->n_interventions, n_masks,
                              config.mask_size,
                              col_idx, func, typ);
        }
      }
    }
  }

}

void Intervention2DEval(int thread_id, EvalConfig& config, PhysicalOperator* op,
                        std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		Intervention2DEval(thread_id, config, op->children[i].get(), fade_data);
	}

	FadeNode* cur_node = dynamic_cast<FadeNode*>(fade_data[op->id].get());
	if (op->type == PhysicalOperatorType::TABLE_SCAN || 
      (op->type == PhysicalOperatorType::FILTER && !config.prune && cur_node->n_interventions > 0)) {
		pair<int, int> start_end_pair = Fade::get_start_end(cur_node->n_groups, thread_id, config.num_worker);
		if (cur_node->rows == cur_node->n_groups) return;
		  int opid = fade_data[op->id]->opid;
      if (config.n_intervention == 1) {
        FadeNodeSingle* single_cur_node = dynamic_cast<FadeNodeSingle*>(fade_data[op->id].get());
        int8_t* in_ptr = nullptr;
        if (op->type == PhysicalOperatorType::TABLE_SCAN) {
            in_ptr = single_cur_node->base_single_del_interventions;
        } else {
          in_ptr = dynamic_cast<FadeNodeSingle*>(fade_data[opid].get())->single_del_interventions;
        }
        filter_single(thread_id, op->lineage_op->backward_lineage[0].data(),
                                   in_ptr,
                                   single_cur_node->single_del_interventions,
                                   start_end_pair.first, start_end_pair.second);
      } else {
        FadeNodeDense* dense_cur_node = dynamic_cast<FadeNodeDense*>(fade_data[op->id].get());
        __mmask16* in_ptr = nullptr;
        if (op->type == PhysicalOperatorType::TABLE_SCAN) {
            in_ptr = dense_cur_node->base_target_matrix;
        } else {
          in_ptr = dynamic_cast<FadeNodeDense*>(fade_data[opid].get())->del_interventions;
        }
        filter_dense(thread_id, op->lineage_op->backward_lineage[0].data(),
                                  in_ptr,
                                  dense_cur_node->del_interventions,
                                  start_end_pair.first, start_end_pair.second,
                                  dense_cur_node->n_masks, config.n_intervention,
                                  config.is_scalar);
      }
      std::unique_lock<std::mutex> lock(cur_node->mtx);
      if (++cur_node->counter < cur_node->num_worker) {
        cur_node->cv.wait(lock, [cur_node] { return cur_node->counter  >= cur_node->num_worker; });
      } else {
        cur_node->cv.notify_all();
      }
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
      int lhs_opid = fade_data[op->children[0]->id]->opid;
      int rhs_opid = fade_data[op->children[1]->id]->opid;
	  pair<int, int> start_end_pair = Fade::get_start_end(cur_node->n_groups, thread_id, config.num_worker);
			int n_left_interventions = fade_data[fade_data[op->children[0]->id]->opid]->n_interventions;
			int n_right_interventions = fade_data[fade_data[op->children[1]->id]->opid]->n_interventions;
      if (cur_node->n_interventions == 1) {
        FadeNodeSingle* single_cur_node = dynamic_cast<FadeNodeSingle*>(fade_data[op->id].get());
        join_single(thread_id, op->lineage_op->backward_lineage[0].data(),
                          op->lineage_op->backward_lineage[1].data(),
                          dynamic_cast<FadeNodeSingle*>(fade_data[lhs_opid].get())->single_del_interventions,
                          dynamic_cast<FadeNodeSingle*>(fade_data[rhs_opid].get())->single_del_interventions,
                             single_cur_node->single_del_interventions,
                          start_end_pair.first, start_end_pair.second,
                          n_left_interventions, n_right_interventions);
      } else {
        FadeNodeDense* dense_cur_node = dynamic_cast<FadeNodeDense*>(fade_data[op->id].get());
	      if (config.is_scalar || config.n_intervention < 512) {
          join_dense_scalar(thread_id, op->lineage_op->backward_lineage[0].data(),
                        op->lineage_op->backward_lineage[1].data(),
                        dynamic_cast<FadeNodeDense*>(fade_data[lhs_opid].get())->del_interventions,
                        dynamic_cast<FadeNodeDense*>(fade_data[rhs_opid].get())->del_interventions,
                        dense_cur_node->del_interventions,
                        start_end_pair.first, start_end_pair.second, dense_cur_node->n_masks,
                        n_left_interventions, n_right_interventions);
        } else {
          join_dense_simd(thread_id, op->lineage_op->backward_lineage[0].data(),
                        op->lineage_op->backward_lineage[1].data(),
                        dynamic_cast<FadeNodeDense*>(fade_data[lhs_opid].get())->del_interventions,
                        dynamic_cast<FadeNodeDense*>(fade_data[rhs_opid].get())->del_interventions,
                        dense_cur_node->del_interventions,
                        start_end_pair.first, start_end_pair.second, dense_cur_node->n_masks,
                        n_left_interventions, n_right_interventions);
        }
        if (config.debug) dense_cur_node->debug_dense_matrix();
      }
      std::unique_lock<std::mutex> lock(cur_node->mtx);
      if (++cur_node->counter < cur_node->num_worker) {
        cur_node->cv.wait(lock, [cur_node] { return cur_node->counter  >= cur_node->num_worker; });
      } else {
        cur_node->cv.notify_all();
				}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		if (thread_id >= cur_node->num_worker ) return;
		int opid = fade_data[op->children[0]->id]->opid;
		if (config.n_intervention == 1) {
			HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op,
			                         dynamic_cast<FadeNodeSingle*>(fade_data[opid].get())->single_del_interventions);
		} else {
			if (config.debug) {
				dynamic_cast<FadeNodeDense*>(fade_data[opid].get())->debug_dense_matrix();
			}
			HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op,
			                             dynamic_cast<FadeNodeDense*>(fade_data[opid].get())->del_interventions);
		}
    std::unique_lock<std::mutex> lock(cur_node->mtx);
    if (++cur_node->counter < cur_node->num_worker) {
      cur_node->cv.wait(lock, [cur_node] { return cur_node->counter  >= cur_node->num_worker; });
    } else {
      cur_node->cv.notify_all();
    }
	}
}

/*
  1. traverse plan to construct template
  2. compile
  2. traverse plan to bind variables and execute code
*/
// TODO: run post processing, code gen, and compilation after query execution
string Fade::WhatifDense(PhysicalOperator *op, EvalConfig config) {
	std::cout << op->ToString() << std::endl;
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;
	std::unordered_map<idx_t, unique_ptr<FadeNode>> fade_data;
  std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec(config.columns_spec_str);

  Clear(op);

	start_time = std::chrono::steady_clock::now();
	if (config.n_intervention == 1) {
		AllocSingle(config, op, fade_data, columns_spec);
	} else {
		AllocDense(config, op, fade_data, columns_spec);
	}
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double prep_time = time_span.count();
	
  start_time = std::chrono::steady_clock::now();
	GetCachedData(config, op, fade_data, columns_spec);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double data_time = time_span.count();

	double intervention_gen_time = 0;
	if (!config.use_preprep_tm) {
		std::cout << "fill random: " << std::endl;
		// 3. Prepare base interventions; should be one time cost per DB
    	std::vector<std::thread> workers_random;
    	start_time = std::chrono::steady_clock::now();
    	// using leaf lineage can reduce this to only the tuples used by the final output
		for (int i = 0; i < config.num_worker; ++i) {
			workers_random.emplace_back(GenRandomWhatifIntervention, i, std::ref(config), op, std::ref(fade_data), std::ref(columns_spec), false);
		}
		// Wait for all tasks to complete
		for (std::thread& worker : workers_random) {
			worker.join();
		}
		end_time = std::chrono::steady_clock::now();
		time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
		intervention_gen_time = time_span.count();
  }

  std::cout << "intervention" << std::endl;
	std::vector<std::thread> workers;
	start_time = std::chrono::steady_clock::now();
  	for (int i = 0; i < config.num_worker; ++i) {
		workers.emplace_back(Intervention2DEval, i, std::ref(config),  op, std::ref(fade_data));
	}
	// Wait for all tasks to complete
	for (std::thread& worker : workers) {
		worker.join();
	}

	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double eval_time = time_span.count();

	config.topk=10;
	if (config.topk > 0) {
	  start_time = std::chrono::steady_clock::now();
		// rank final agg result
		std::vector<int> topk_vec = rank(op, config, fade_data);
		std::string result = std::accumulate(topk_vec.begin(), topk_vec.end(), std::string{},
		                                     [](const std::string& a, int b) {
			                                     return a.empty() ? std::to_string(b) : a + "," + std::to_string(b);
		                                     });
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double rank_time = time_span.count();
    std::cout <<  result  << std::endl;
    return "select " + to_string(intervention_gen_time) + " as intervention_gen_time, "
           + to_string(prep_time) + " as prep_time, "
           + to_string(0) + " as compile_time, "
           + to_string(0) + " as code_gen_time, "
           + to_string(data_time) + " as data_time, "
           + to_string(eval_time) + " as eval_time, "
           + to_string(rank_time) + " as rank_time";
  } else {
    return "select " + to_string(intervention_gen_time) + " as intervention_gen_time, "
           + to_string(prep_time) + " as prep_time, "
           + to_string(0) + " as compile_time, "
           + to_string(0) + " as code_gen_time, "
           + to_string(data_time) + " as data_time, "
           + to_string(eval_time) + " as eval_time";
  }
}

} // namespace duckdb
#endif

