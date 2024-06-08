#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include <fstream>
#include <thread>
#include <vector>

namespace duckdb {

// lineage: 1D backward lineage
// var: sparse input intervention matrix
// out: sparse output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
int whatif_sparse_filter(vector<int>& lineage,
                         int* __restrict__ var,
                         int* __restrict__ out,
                         int start, int end) {
	for (int i=start; i < end; ++i) {
		out[i] = var[lineage[i]];
	}
	return 0;
}

// lhs/rhs_lineage: 1D backward lineage
// lhs/rhs_var: sparse input intervention matrix
// out: sparse output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
// left/right_n_interventions: how many annotations the left/right side has
int whatif_sparse_join(int* lhs_lineage, int* rhs_lineage,
                       int* __restrict__ lhs_var,  int* __restrict__ rhs_var,
                       int* __restrict__ out, const int start, const int end,
                       int left_n_interventions, int right_n_interventions) {
	if (left_n_interventions > 1 && right_n_interventions > 1) {
		for (int i=start; i < end; i++) {
			out[i] = lhs_var[lhs_lineage[i]] * right_n_interventions + rhs_var[rhs_lineage[i]];
		}
	} else if (left_n_interventions > 1) {
		for (int i=start; i < end; i++) {
			out[i] = lhs_var[lhs_lineage[i]];
		}
	} else {
		for (int i=start; i < end; i++) {
			out[i] = rhs_var[rhs_lineage[i]];
		}
	}

	return 0;
}

int groupby_agg_incremental_arr_single_group_bw(int g, std::vector<int>& bw_lineage, int* __restrict__ var_0,
                                void* __restrict__  out,
                                std::unordered_map<int, void*>& input_data_map,
                                int n_interventions, int col_idx, string func, string typ) {
	if (func == "count") {
		int* __restrict__ out_int = (int*)out;
		for (int i=0; i < bw_lineage.size(); ++i) {
			int iid = bw_lineage[i];
			int row = var_0[iid];
			int col = g * n_interventions;
			out_int[col + row]++;
		}
	} else if (func == "sum" || func == "avg" || func == "stddev") { // sum
		if (typ == "int") {
			int* __restrict__  out_int = (int*)out;
			int *in_arr = reinterpret_cast<int *>(input_data_map[col_idx]);
		  for (int i=0; i < bw_lineage.size(); ++i) {
			  int iid = bw_lineage[i];
			  int col = g * n_interventions;
				int row = var_0[iid];
				out_int[col + row] += in_arr[iid];
			}
		} else {
			float* __restrict__  out_float = (float*)out;
			float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
		  for (int i=0; i < bw_lineage.size(); ++i) {
			  int iid = bw_lineage[i];
			  int col = g * n_interventions;
				int row = var_0[iid];
				out_float[col + row] += in_arr[iid];
			}
		}
	} else if (func == "sum_2") {
			float* __restrict__  out_float = (float*)out;
			float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
		  for (int i=0; i < bw_lineage.size(); ++i) {
			  int iid = bw_lineage[i];
			  int col = g * n_interventions;
				int row = var_0[iid];
				out_float[col + row] += (in_arr[iid] * in_arr[iid]);
			}
  }
	return 0;
}

int groupby_agg_incremental_arr(int* lineage, int* __restrict__ var_0,
                                void* __restrict__  out,
                                std::unordered_map<int, void*>& input_data_map,
                                const int start, const int end,
                                int n_interventions, int col_idx, string func, string typ) {
	if (func == "count") {
		int* __restrict__ out_int = (int*)out;
		for (int i=start; i < end; ++i) {
			int oid = lineage[i];
			int col = oid * n_interventions;
			int row = var_0[i];
			out_int[col + row] += 1;
		}
	} else if (func == "sum" || func == "avg" || func == "stddev") { // sum
		if (typ == "int") {
			int* __restrict__  out_int = (int*)out;
			int *in_arr = reinterpret_cast<int *>(input_data_map[col_idx]);
			for (int i=start; i < end; ++i) {
				int oid = lineage[i];
				int col = oid * n_interventions;
				int row = var_0[i];
				out_int[col + row] += in_arr[i];
			}
		} else {
			float* __restrict__  out_float = (float*)out;
			float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
			for (int i=start; i < end; ++i) {
				int oid = lineage[i];
				int col = oid * n_interventions;
				int row = var_0[i];
				out_float[col + row] += in_arr[i];
			}
		}
	} else if (func == "sum_2") {
			float* __restrict__  out_float = (float*)out;
			float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
			for (int i=start; i < end; ++i) {
				int oid = lineage[i];
				int col = oid * n_interventions;
				int row = var_0[i];
				out_float[col + row] += (in_arr[i] * in_arr[i]);
			}
  }
	return 0;
}

void Fade::InterventionSparseEvalPredicate(int thread_id, EvalConfig& config, PhysicalOperator* op,
                                     std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                                     bool use_compiled=false) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		InterventionSparseEvalPredicate(thread_id, config, op->children[i].get(), fade_data);
	}

	FadeSparseNode* cur_node = dynamic_cast<FadeSparseNode*>(fade_data[op->id].get());
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (cur_node->n_interventions <= 1) return;
		// what indicates there is a filter? cur_node->gen?
		if (cur_node->rows > cur_node->n_groups) {
			pair<int, int> start_end_pair = Fade::get_start_end(op->lineage_op->backward_lineage[0].size(), thread_id, config.num_worker);
			if (use_compiled) {
				dynamic_cast<FadeNodeSparseCompile*>(fade_data[op->id].get())->filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
				                    cur_node->base_annotations.get(), cur_node->annotations.get(),
				                    start_end_pair.first, start_end_pair.second);

			} else {
				whatif_sparse_filter(op->lineage_op->backward_lineage[0],
				                     cur_node->base_annotations.get(),
				                     cur_node->annotations.get(),
				                     start_end_pair.first, start_end_pair.second);
				std::unique_lock<std::mutex> lock(cur_node->mtx);
				if (++cur_node->counter < cur_node->num_worker) {
					cur_node->cv.wait(lock, [cur_node] { return cur_node->counter  >= cur_node->num_worker; });
				} else {
					cur_node->cv.notify_all();
				}
			}
		} else {
			if (thread_id == 0) {
				cur_node->annotations = std::move(cur_node->base_annotations);
			}
		}
	} else if (op->type == PhysicalOperatorType::FILTER) {
		if (config.prune) return;
		if (cur_node->n_interventions <= 1) return;
		pair<int, int> start_end_pair = Fade::get_start_end(cur_node->rows, thread_id, config.num_worker);
		if (use_compiled) {
			dynamic_cast<FadeNodeSparseCompile*>(fade_data[op->id].get())->filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
			                    dynamic_cast<FadeNodeSparseCompile*>(fade_data[fade_data[op->children[0]->id]->opid].get())->annotations.get(),
			                    cur_node->annotations.get(),
			                    start_end_pair.first, start_end_pair.second);
		} else {
			whatif_sparse_filter(op->lineage_op->backward_lineage[0],
			                     dynamic_cast<FadeSparseNode*>(fade_data[fade_data[op->children[0]->id]->opid].get())->annotations.get(),
			                     cur_node->annotations.get(), start_end_pair.first, start_end_pair.second);
			std::unique_lock<std::mutex> lock(cur_node->mtx);
			if (++cur_node->counter < cur_node->num_worker) {
				cur_node->cv.wait(lock, [cur_node] { return cur_node->counter  >= cur_node->num_worker; });
			} else {
				cur_node->cv.notify_all();
			}
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		if (cur_node->n_interventions <= 1) return;
		// if (!cur_node->gen) return;
		pair<int, int> start_end_pair = Fade::get_start_end(cur_node->rows, thread_id, config.num_worker);
		if (use_compiled) {
			dynamic_cast<FadeNodeSparseCompile*>(fade_data[op->id].get())->join_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
			                  op->lineage_op->backward_lineage[1].data(),
			                  dynamic_cast<FadeSparseNode*>(fade_data[fade_data[op->children[0]->id]->opid].get())->annotations.get(),
			                  dynamic_cast<FadeSparseNode*>(fade_data[fade_data[op->children[1]->id]->opid].get())->annotations.get(),
			                  cur_node->annotations.get(),
			                  start_end_pair.first, start_end_pair.second);
		} else {
			int n_left_interventions = fade_data[fade_data[op->children[0]->id]->opid]->n_interventions;
			int n_right_interventions = fade_data[fade_data[op->children[1]->id]->opid]->n_interventions;
			whatif_sparse_join(op->lineage_op->backward_lineage[0].data(),
			                   op->lineage_op->backward_lineage[1].data(),
			                   dynamic_cast<FadeSparseNode*>(fade_data[fade_data[op->children[0]->id]->opid].get())->annotations.get(),
			                   dynamic_cast<FadeSparseNode*>(fade_data[fade_data[op->children[1]->id]->opid].get())->annotations.get(),
			                   cur_node->annotations.get(),
			                   start_end_pair.first, start_end_pair.second,
			                   n_left_interventions, n_right_interventions);
			std::unique_lock<std::mutex> lock(cur_node->mtx);
			if (++cur_node->counter < cur_node->num_worker) {
				cur_node->cv.wait(lock, [cur_node] { return cur_node->counter  >= cur_node->num_worker; });
			} else {
				cur_node->cv.notify_all();
			}
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		if (fade_data[op->id]->n_interventions <= 1 ||  thread_id >= cur_node->num_worker ) return;
		idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		pair<int, int> start_end_pair = Fade::get_start_end(row_count, thread_id, cur_node->num_worker);
		if (config.debug)  std::cout << "Count summary: " << row_count   << " " << start_end_pair.first << " " << start_end_pair.second << std::endl;
		if (use_compiled) {
			dynamic_cast<FadeNodeSparseCompile*>(fade_data[op->id].get())->agg_fn(thread_id, op->lineage_op->forward_lineage[0].data(),
			                 dynamic_cast<FadeSparseNode*>(fade_data[fade_data[op->children[0]->id]->opid].get())->annotations.get(),
			                 cur_node->alloc_vars,
			                 cur_node->input_data_map,
			                 start_end_pair.first, start_end_pair.second);
		} else {
      int* forward_lineage_ptr = op->lineage_op->forward_lineage[0].data();
      int* annotations_ptr = dynamic_cast<FadeSparseNode*>(fade_data[fade_data[op->children[0]->id]->opid].get())->annotations.get();
			for (auto& out_var : cur_node->alloc_vars_funcs) {
				string func = cur_node->alloc_vars_funcs[out_var.first];
				int col_idx = cur_node->alloc_vars_index[out_var.first];
				string typ = cur_node->alloc_vars_types[out_var.first];
        if (config.groups.empty()) {
          if (func == "count") {
            for (int i=start_end_pair.first; i < start_end_pair.second; ++i) {
              int oid = forward_lineage_ptr[i];
               config.groups_count[oid]++;
            }
          } else if (func == "avg" || func == "stddev" || func == "sum") {
            float *in_arr = reinterpret_cast<float *>(cur_node->input_data_map[col_idx]);
            for (int i=start_end_pair.first; i < start_end_pair.second; ++i) {
              int oid = forward_lineage_ptr[i];
               config.groups_sum[oid] += in_arr[i];
            }
            if (func == "stddev") {
              float *in_arr = reinterpret_cast<float *>(cur_node->input_data_map[col_idx]);
              for (int i=start_end_pair.first; i < start_end_pair.second; ++i) {
                int oid = forward_lineage_ptr[i];
                config.groups_sum_2[oid] += (in_arr[i] * in_arr[i]);
              }
            }
          }
        } else {
          for (int g=0; g < config.groups.size(); ++g) {
            int gid = config.groups[g];
            std::vector<int>& bw_lineage = op->lineage_op->gb_backward_lineage[gid];
            if (func == "count") {
              config.groups_count[g] = bw_lineage.size();
            } else if (func == "avg" || func == "stddev" || func == "sum") {
              float *in_arr = reinterpret_cast<float *>(cur_node->input_data_map[col_idx]);
              for (int i=0; i < bw_lineage.size(); ++i) {
                int iid = bw_lineage[i];
                 config.groups_sum[g] += in_arr[iid];
              }
              if (func == "stddev") {
                float *in_arr = reinterpret_cast<float *>(cur_node->input_data_map[col_idx]);
                for (int i=0; i < bw_lineage.size(); ++i) {
                  int iid = bw_lineage[i];
                  config.groups_sum_2[g] += (in_arr[iid] * in_arr[iid]);
                }
              }
            }
          }
        }
      }
			for (auto& out_var : cur_node->alloc_vars_funcs) {
				string func = cur_node->alloc_vars_funcs[out_var.first];
				int col_idx = cur_node->alloc_vars_index[out_var.first];
				string typ = cur_node->alloc_vars_types[out_var.first];
        if (config.groups.empty()) {
          groupby_agg_incremental_arr(forward_lineage_ptr,
                                      annotations_ptr,
                                      cur_node->alloc_vars[out_var.first][thread_id],
                                      cur_node->input_data_map,
                                      start_end_pair.first, start_end_pair.second, cur_node->n_interventions,
                                      col_idx, func, typ);
        } else {
          for (int g=0; g < config.groups.size(); ++g) {
            int gid = config.groups[g];
            groupby_agg_incremental_arr_single_group_bw(g, op->lineage_op->gb_backward_lineage[gid], annotations_ptr,
                                        cur_node->alloc_vars[out_var.first][thread_id],
                                        cur_node->input_data_map,
                                        cur_node->n_interventions,
                                        col_idx, func, typ);
          }
        }
				if (cur_node->num_worker > 1) {
					// combine into partition 0
					std::unique_lock<std::mutex> lock(cur_node->mtx);
					if (++cur_node->counter < cur_node->num_worker) {
						cur_node->cv.wait(lock, [cur_node] { return cur_node->counter  >= cur_node->num_worker; });
					} else {
						cur_node->cv.notify_all();
					}
				}
			}
		}
	}
}

string Fade::WhatIfSparse(PhysicalOperator *op, EvalConfig config) {
  global_fade_node = nullptr;
  std::cout << "WhatIfSparse" << std::endl;
  	std::cout << op->ToString() << std::endl;
  std::chrono::steady_clock::time_point start_time, end_time;
  std::chrono::duration<double> time_span;

  // 1. (t.col1|t.col2|..)
  std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec(config.columns_spec_str);
  std::unordered_map<idx_t, unique_ptr<FadeNode>> fade_data;

  start_time = std::chrono::steady_clock::now();
  bool compile = false;
  Fade::GenSparseAndAlloc(config, op, fade_data, columns_spec, compile);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double gen_time = time_span.count();

  // Load data from cached execution
  start_time = std::chrono::steady_clock::now();
  Fade::GetCachedData(config, op, fade_data, columns_spec);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double data_time = time_span.count();

  std::cout << "Evaluate interventions: " << std::endl;
  // Evaluate interventions for each intervention. each call evaluate one aggregate function
  // no need to compile
  std::vector<std::thread> workers;
  start_time = std::chrono::steady_clock::now();
  for (int i = 0; i < config.num_worker; ++i) {
	workers.emplace_back(InterventionSparseEvalPredicate, i, std::ref(config),  op, std::ref(fade_data), false);
  }

  // Wait for all tasks to complete
  for (std::thread& worker : workers) {
	worker.join();
  }

  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double eval_time = time_span.count();
  global_fade_node = std::move(fade_data[ fade_data[op->id]->opid ]);
  global_config = config;
  return "SELECT 1"; // from duckdb_fade()";
}

} // namespace duckdb
#endif
