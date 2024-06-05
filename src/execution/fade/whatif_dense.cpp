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


int join_dense_single(int thread_id, int* __restrict__ lhs_lineage, int* __restrict__  rhs_lineage,
               void* __restrict__ lhs_var_ptr,  void* __restrict__ rhs_var_ptr,
               void* __restrict__ out_ptr, const int start, const int end, int n_masks) {
	int8_t* __restrict__ out = (int8_t* __restrict__)out_ptr;
	int8_t* __restrict__ rhs_var = (int8_t* __restrict__)rhs_var_ptr;
	int8_t* __restrict__ lhs_var = (int8_t* __restrict__)lhs_var_ptr;
	for (int i=start; i < end; ++i) {
		// oss << get_single_join_template(config, n_left_interventions, int_right_interventions);
	}
	// oss << "\n\t}\n\tsync_point.arrive_and_wait();\n\treturn 0; \n}\n";
	return 0;
}

int join_dense(int thread_id, int* __restrict__ lhs_lineage, int* __restrict__  rhs_lineage,
                      void* __restrict__ lhs_var_ptr,  void* __restrict__ rhs_var_ptr,
                      void* __restrict__ out_ptr, const int start, const int end, int n_masks) {
	__mmask16* __restrict__ out = (__mmask16* __restrict__)out_ptr;
	__mmask16* __restrict__ rhs_var = (__mmask16* __restrict__)rhs_var_ptr;
	__mmask16* __restrict__ lhs_var = (__mmask16* __restrict__)lhs_var_ptr;
	for (int i=start; i < end; ++i) {
		// oss << get_batch_join_template(config, n_left_n_masks, n_right_n_masks);
	}

	// oss << "\n\t}\n\tsync_point.arrive_and_wait();\n\treturn 0; \n}\n";
	return 0;
}

int filter_dense_single(int thread_id, int* __restrict__ lineage,  void* __restrict__ var_ptr, void* __restrict__ out_ptr,
                        const int start, const int end) {

	int8_t* __restrict__ out = (int8_t* __restrict__)out_ptr;
	int8_t* __restrict__ var = (int8_t* __restrict__)var_ptr;
	for (int i=start; i < end; ++i) {
		out[i] = var[lineage[i]];
	}
	// oss << "\tsync_point.arrive_and_wait();\n\t return 0; \n}\n\n";
	return 0;
}

int filter_dense(int thread_id, int* __restrict__ lineage,  void* __restrict__ var_ptr, void* __restrict__ out_ptr,
                 const int start, const int end, int n_masks, int n_intervention, bool is_scalar) {

	__mmask16* __restrict__ out = (__mmask16* __restrict__)out_ptr;
	__mmask16* __restrict__ var = (__mmask16* __restrict__)var_ptr;

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
			//	__m512i b = _mm512_stream_load_si512((__m512i*)&var[col_oid+j]);
			//	_mm512_store_si512((__m512i*)&out[col_out+j], b);
			}
		}
	}

	// oss << "\tsync_point.arrive_and_wait();\n\t return 0; \n}\n\n";
	return 0;
}

void  Fade::HashAggregateIntervene2DEval(int thread_id, EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                  std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                                  PhysicalOperator* op, void* var_0, bool use_compile) {
	// check if this has child aggregate, if so then get its id
	FadeNodeDenseCompile* cur_node = dynamic_cast<FadeNodeDenseCompile*>(fade_data[op->id].get());
	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	pair<int, int> start_end_pair = Fade::get_start_end(row_count, thread_id, config.num_worker);
	if (config.debug) std::cout << "Count summary: " << row_count << " "  << " " << start_end_pair.first << " " << start_end_pair.second << " " << config.num_worker << " " << thread_id << std::endl;

	if (use_compile) {
		if (cur_node->has_agg_child) {
			cur_node->agg_fn_nested(thread_id, op->lineage_op->forward_lineage[0].data(), var_0, cur_node->alloc_vars,
			                        fade_data[fade_data[op->id]->child_agg_id]->alloc_vars, start_end_pair.first, start_end_pair.second);
		} else {
			if (config.use_gb_backward_lineage) {
				cur_node->agg_fn_bw(thread_id, op->lineage_op->gb_backward_lineage, var_0, cur_node->alloc_vars,
				                    cur_node->input_data_map);
			} else {
				cur_node->agg_fn(thread_id, op->lineage_op->forward_lineage[0].data(), var_0, cur_node->alloc_vars,
				                 cur_node->input_data_map, start_end_pair.first, start_end_pair.second);
			}
		}
	} else {

	}
}

void Fade::Intervention2DEval(int thread_id, EvalConfig& config, PhysicalOperator* op,
                        std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                        bool use_compiled=false) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		Intervention2DEval(thread_id, config, op->children[i].get(), fade_data, use_compiled);
	}

	FadeNode* cur_node = dynamic_cast<FadeNode*>(fade_data[op->id].get());
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		pair<int, int> start_end_pair = Fade::get_start_end(cur_node->n_groups, thread_id, config.num_worker);
		if (cur_node->rows == cur_node->n_groups) return;
		if (config.n_intervention == 1) {
			FadeNodeSingleCompile* single_cur_node = dynamic_cast<FadeNodeSingleCompile*>(fade_data[op->id].get());
			single_cur_node->filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
			                           single_cur_node->base_single_del_interventions,
			                           single_cur_node->single_del_interventions,
			                           start_end_pair.first, start_end_pair.second);
		} else {
			FadeNodeDenseCompile* dense_cur_node = dynamic_cast<FadeNodeDenseCompile*>(fade_data[op->id].get());
			dense_cur_node->filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
			                          dense_cur_node->base_target_matrix,
			                          dense_cur_node->del_interventions,
			                          start_end_pair.first, start_end_pair.second);
		}
	} else if (op->type == PhysicalOperatorType::FILTER) {
		if (config.prune || cur_node->n_interventions == 0) return;
		pair<int, int> start_end_pair = Fade::get_start_end(cur_node->n_groups, thread_id, config.num_worker);
		int opid = fade_data[op->children[0]->id]->opid;
		if (cur_node->n_interventions == 1) {
			FadeNodeSingleCompile* single_cur_node = dynamic_cast<FadeNodeSingleCompile*>(fade_data[op->id].get());
			single_cur_node->filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
			                     dynamic_cast<FadeNodeSingleCompile*>(fade_data[opid].get())->single_del_interventions,
			                     single_cur_node->single_del_interventions,
			                     start_end_pair.first, start_end_pair.second);
		} else {
			FadeNodeDenseCompile* dense_cur_node = dynamic_cast<FadeNodeDenseCompile*>(fade_data[op->id].get());
			dense_cur_node->filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
			                    dynamic_cast<FadeNodeDenseCompile*>(fade_data[opid].get())->del_interventions,
			                    dense_cur_node->del_interventions,
			                    start_end_pair.first, start_end_pair.second);
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
      int lhs_opid = fade_data[op->children[0]->id]->opid;
      int rhs_opid = fade_data[op->children[1]->id]->opid;
	  pair<int, int> start_end_pair = Fade::get_start_end(cur_node->n_groups, thread_id, config.num_worker);
	  if (cur_node->n_interventions == 1) {
	  	FadeNodeSingleCompile* single_cur_node = dynamic_cast<FadeNodeSingleCompile*>(fade_data[op->id].get());
		single_cur_node->join_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
				                op->lineage_op->backward_lineage[1].data(),
				                dynamic_cast<FadeNodeSingleCompile*>(fade_data[lhs_opid].get())->single_del_interventions,
				                dynamic_cast<FadeNodeSingleCompile*>(fade_data[rhs_opid].get())->single_del_interventions,
			                     single_cur_node->single_del_interventions,
				                start_end_pair.first, start_end_pair.second);
	  } else {
		FadeNodeDenseCompile* dense_cur_node = dynamic_cast<FadeNodeDenseCompile*>(fade_data[op->id].get());
		dense_cur_node->join_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
			              op->lineage_op->backward_lineage[1].data(),
			              dynamic_cast<FadeNodeDenseCompile*>(fade_data[lhs_opid].get())->del_interventions,
			              dynamic_cast<FadeNodeDenseCompile*>(fade_data[rhs_opid].get())->del_interventions,
			                    dense_cur_node->del_interventions,
			              start_end_pair.first, start_end_pair.second);
		if (config.debug) dense_cur_node->debug_dense_matrix();
	  }
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		int opid = fade_data[op->children[0]->id]->opid;
		if (config.n_intervention == 1) {
			HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op,
			                         dynamic_cast<FadeNodeSingleCompile*>(fade_data[opid].get())->single_del_interventions,
			                         use_compiled);
		} else {
			if (config.debug) {
				dynamic_cast<FadeNodeDenseCompile*>(fade_data[opid].get())->debug_dense_matrix();
			}
			HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op,
			                             dynamic_cast<FadeNodeDenseCompile*>(fade_data[opid].get())->del_interventions,
			                             use_compiled);
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
		workers.emplace_back(Intervention2DEval, i, std::ref(config),  op, std::ref(fade_data), false);
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

