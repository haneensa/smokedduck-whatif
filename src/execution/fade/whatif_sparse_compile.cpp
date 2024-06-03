#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include <fstream>
#include <dlfcn.h>
#include <immintrin.h>
#include <thread>
#include <vector>

namespace duckdb {


string FilterCodeAndAllocPredicate(EvalConfig& config, PhysicalOperator *op) {
	string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
	    << fname
	    << R"((int thread_id, int* lineage,  void* __restrict__ var_ptr, void* __restrict__ out_ptr, int start, int end) {
)";

	oss << "\tint* __restrict__ out = (int* __restrict__)out_ptr;\n";
	oss << "\tint* __restrict__ var = (int* __restrict__)var_ptr;\n";

//  if (config.debug) {
    oss << "\tstd::cout << \"Filter: \" << start << \" \" << end << std::endl;\n";
  //}

	oss << R"(
	for (int i=start; i < end; ++i) {
		out[i] = var[lineage[i]];
	}
)";
	if (config.num_worker > 1) {
		oss << "\tsync_point.arrive_and_wait();\n\t return 0; \n}\n";
	} else {
		oss << "\treturn 0; \n}\n";
	}

	return oss.str();
}


string JoinCodeAndAllocPredicate(EvalConfig config, PhysicalOperator *op, int n_left_interventions, int n_right_interventions) {
	string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
	    << fname
	    << R"((int thread_id, int* lhs_lineage, int* rhs_lineage,  void* __restrict__ lhs_var_ptr,
void* __restrict__ rhs_var_ptr,  void* __restrict__ out_ptr, const int start, const int end) {
)";

	oss << "\t int* __restrict__ out = (int* __restrict__)out_ptr;\n";
	oss << "\t int* __restrict__ rhs_var = (int* __restrict__)rhs_var_ptr;\n";
	oss << "\t int* __restrict__ lhs_var = (int* __restrict__)lhs_var_ptr;\n";

	oss << "\tconst int right_n_interventions = " + to_string(n_right_interventions) + ";\n";
	oss << "\tfor (int i=start; i < end; i++) {\n";


	if (n_left_interventions > 1 && n_right_interventions > 1) {
		oss << R"(out[i] = lhs_var[lhs_lineage[i]] * right_n_interventions + rhs_var[rhs_lineage[i]];)";
	} else if (n_left_interventions > 1) {
		oss << R"(out[i] = lhs_var[lhs_lineage[i]];)";
	} else {
		oss << R"(out[i] = rhs_var[rhs_lineage[i]];)";
	}
	if (config.num_worker > 1) {
		oss << "\n\t\t}\n \tsync_point.arrive_and_wait();\n return 0; \n}\n";
	} else {
		oss << "\n\t\t}\n return 0; \n}\n";
	}

	return oss.str();
}


string get_agg_init_predicate(EvalConfig config, int row_count, int chunk_count, int opid, int n_interventions, string fn, string alloc_code,
                    string get_data_code, string get_vals_code) {
	string fname = "agg_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	if (config.use_duckdb) {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
              duckdb::ChunkCollection &chunk_collection, const int start, const int end) {
)";
	} else {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
              std::unordered_map<int, void*>& input_data_map, const int start, const int end) {
)";
  }
	
  oss << "\t__mmask16 tmp_mask;\n";
	oss << "\tconst int num_threads  = " << config.num_worker << ";\n";
  oss << "\tif (thread_id >= num_threads)  return 0;\n";
	oss << "\t int* __restrict__ var_0 = (int* __restrict__)var_0_ptr;\n";

	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";

  if (config.debug)
 	  oss << "std::cout << \"agg \" <<  n_interventions << \" \" << start << \" \" << end << std::endl;";

	oss << alloc_code;

	oss << get_data_code;

	return oss.str();
}


string get_loop_close2(EvalConfig& config, string post_vals) {
	std::ostringstream oss;
	if (config.incremental == false) {
    oss << "\n\t\t\t}\n"; // close for (int j=0; j < n_masks; ++j)
    oss << post_vals;
	}
  oss << "\n\t\t}\n"; // close for (int i=start; .. end) or for (oid ..

	return oss.str();
}

string get_loop_opening2(EvalConfig& config, string get_vals, string pre_vals) {
	std::ostringstream oss;
  oss <<  R"(
	for (int i=start; i < end; i++) {
		int oid = lineage[i];
		int col = oid*n_interventions;
		int row = var_0[i];
)";
  oss << get_vals;

  if (!config.incremental) {
    // temp_0 = val_0
    oss << pre_vals;
		if (config.is_scalar) {
			oss << R"(
			for (int j=0; j < n_interventions; ++j) {
)";
		} else {
			oss << R"(
      tmp_mask = 0xFFFF;
      tmp_mask &= ~(1 << row%16);
			for (int j=0; j < n_interventions; j+=16) {
)";
		}
    }


	return oss.str();
}

string get_agg_eval_predicate(EvalConfig config, int agg_count, string fn, string out_var="", string in_var="", string data_type="int") {
	std::ostringstream oss;
	if (config.incremental) { // if functions are incrementally removable, then agg the part
		if (fn == "sum") {
			oss << "\t\t";
			oss << out_var + "[col+row] +=" + in_var + ";\n";
		} else if (fn == "count") {
			oss << "\t\t";
			oss << "out_count[col+row] += 1;\n";
		}
		return oss.str();
	}

	if (config.is_scalar) {
		oss << "\t\t\t\t" << out_var+"[col + j ] +="+ in_var;
		//oss << " * (row!=j);\n";
		oss << ";\n";
	} else {
		oss << "{";
    // TODO: maintain the original value for row and restore it after this
		if (data_type == "float") {
			oss << "\t__m512 a  = _mm512_load_ps((__m512*) &"+out_var+"[col + j]);\n";
			//oss << "\t __m512 v = _mm512_set1_ps("+in_var+");\n";
			oss << "\t_mm512_store_ps((__m512*) &"+out_var+"[col + j], _mm512_add_ps(a, "+in_var+"));\n";
      //oss << "\t_mm512_store_ps((__m512*) &"+out_var+"[col + j], _mm512_mask_add_ps(a, tmp_mask, a, "+in_var+"));\n";
		} else if (data_type == "int") {
			oss << "\t__m512i a = _mm512_load_si512((__m512i*)&" + out_var + "[col+j]);\n";
			//oss << "\t __m512i v = _mm512_set1_epi32("+in_var+");\n";
			oss << "\t_mm512_store_si512((__m512i*) &"+out_var+"[col + j], _mm512_add_epi32(a, "+in_var+"));\n";
		}
		oss << "}";
	}


	return oss.str();
}




string HashAggregateCodeAndAllocPredicate(EvalConfig& config, PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                                          int keys, int n_groups, int n_interventions,
                                          std::unordered_map<string, vector<void*>>& alloc_vars,
                                          std::unordered_map<string, int>& alloc_vars_index,
                                          std::unordered_map<string, string>& alloc_vars_types) {
	string eval_code;
	string body_code;
	string code;
	string alloc_code;
	string get_data_code;
	string get_vals_code;
	string post_vals_code;
	string pre_vals_code;

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	// if n_groups * n_interventions > rows then use single thread
	bool include_count = false;
	int agg_count = 0;
	// Populate the aggregate child vectors
	for (idx_t i=0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i]->Cast<BoundAggregateExpression>();
		vector<idx_t> aggregate_input_idx;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			aggregate_input_idx.push_back(bound_ref_expr.index);
		}
		string name = aggr.function.name;

		if (include_count == false && (name == "count" || name == "count_star")) {
			include_count = true;
			continue;
		} else if (name == "avg") {
			include_count = true;
		}

		if (name == "sum" || name == "sum_no_overflow" || name == "avg") {
			if (agg_count > 0 && agg_count % config.batch == 0) {
				body_code += get_loop_opening2(config, get_vals_code, pre_vals_code) +
				             eval_code + get_loop_close2(config, post_vals_code);
				get_vals_code = "";
				pre_vals_code = "";
				post_vals_code = "";
				eval_code = "";
			}
			// int col_idx = i + keys;
			string out_var = "out_" + to_string(i); // new output
			string in_arr = "col_" + to_string(i);  // input arrays
			string in_val = "val_" + to_string(i);  // input values val = col_x[i]

			string input_type = alloc_vars_types[out_var];
			string output_type = alloc_vars_types[out_var];

			get_data_code += "\t\t"+input_type+"* " + in_arr + " = reinterpret_cast<"+input_type+" *>(input_data_map[" +
				                 to_string(i) + "]);\n";
			pre_vals_code += "\t\t"  + output_type + " " + out_var + "_temp = " + out_var  + "[col + row];\n";
			post_vals_code += "\t\t"  + out_var + "[col + row] = " + out_var + "_temp;\n";

			if (config.is_scalar) {
			  get_vals_code += "\t\t\t" + output_type +" " + in_val + "= " + in_arr + "[i];\n";
			} else {
			  if (output_type == "float") {
			    get_vals_code += "\t\t\ __m512 "+in_val+"= _mm512_set1_ps(" + in_arr + "[i]);\n";
			  } else {
			    get_vals_code += "\t\t\ __m512i "+in_val+"= _mm512_set1_epi32(" + in_arr + "[i]);\n";
			  }
			}
			// access output arrays
			alloc_code += Fade::get_agg_alloc(config, i, "sum", output_type);
			// core agg operation
			eval_code += get_agg_eval_predicate(config, agg_count++, "sum", out_var, in_val, output_type);
		}
	}

	if (include_count == true) {
		string out_var = "out_count";
		alloc_code += Fade::get_agg_alloc(config, 0, "count", "int");
		if (agg_count > 0 && agg_count % config.batch == 0) {
			body_code += get_loop_opening2(config, get_vals_code, pre_vals_code) + eval_code +
			             get_loop_close2(config, post_vals_code);
			get_vals_code = "";
			pre_vals_code = "";
			post_vals_code = "";
			eval_code = "";
		}
		if (config.is_scalar == false) {
		  get_vals_code += "\t\t__m512i one = _mm512_set1_epi32(1);\n";
		  eval_code += get_agg_eval_predicate(config, agg_count++, "count", out_var, "one", "int");
		} else {
		  eval_code += get_agg_eval_predicate(config, agg_count++, "count", out_var, "1", "int");
		}
		pre_vals_code += "\t\tint " + out_var + "_temp = " + out_var  + "[col + row];\n";
		post_vals_code += "\t\t"  + out_var + "[col + row] = " + out_var + "_temp;\n";
	}
	if (agg_count > 0 && !eval_code.empty()) {
      body_code += get_loop_opening2(config, get_vals_code, pre_vals_code) + eval_code +
		             get_loop_close2(config, post_vals_code);
      get_vals_code = "";
      pre_vals_code = "";
      post_vals_code = "";
      eval_code = "";
	}


	string init_code = get_agg_init_predicate(config, row_count,
	                                          op->children[0]->lineage_op->chunk_collection.ChunkCount(),
	                                          op->id,  n_interventions, "agg", alloc_code,
	                                          get_data_code, get_vals_code);
	string end_code;

	end_code += Fade::group_partitions(config, n_groups, alloc_vars, alloc_vars_index, alloc_vars_types);
	end_code +=  "\treturn 0;\n}\n";

	code = init_code + body_code + end_code;

	return code;
}

void GenCodeAndAllocPredicate(EvalConfig& config, string& code, PhysicalOperator* op,
                              std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                              std::unordered_map<std::string, std::vector<std::string>>& columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenCodeAndAllocPredicate(config, code, op->children[i].get(), fade_data, columns_spec);
	}

	FadeNodeSparseCompile* cur_node = dynamic_cast<FadeNodeSparseCompile*>(fade_data[op->id].get());
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (cur_node->n_interventions > 1 && cur_node->rows > cur_node->n_groups) {
		  cur_node->gen = true;
		  code += FilterCodeAndAllocPredicate(config, op);
		}
  	} else if (op->type == PhysicalOperatorType::FILTER) {
		if ( cur_node->n_interventions > 1 && config.prune == false) {
			cur_node->gen = true;
			code += FilterCodeAndAllocPredicate(config, op);
    	}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		if (cur_node->n_interventions <= 1) return;
		int n_left_interventions = fade_data[fade_data[op->children[0]->id]->opid]->n_interventions;
		int n_right_interventions = fade_data[fade_data[op->children[1]->id]->opid]->n_interventions;
		code += JoinCodeAndAllocPredicate(config, op, n_left_interventions, n_right_interventions);
		cur_node->gen = true;
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY ||
      op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY ||
	  op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		cur_node->gen = true;
		if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		  PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
		  auto &aggregates = gb->grouped_aggregate_data.aggregates;

		  code += HashAggregateCodeAndAllocPredicate(config, op, aggregates,
			                                         gb->grouped_aggregate_data.groups.size(), cur_node->n_groups, cur_node->n_interventions,
			                                         cur_node->alloc_vars,  cur_node->alloc_vars_index,
			                                         cur_node->alloc_vars_types);
		} else if (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
		  PhysicalPerfectHashAggregate * gb = dynamic_cast<PhysicalPerfectHashAggregate *>(op);
		  auto &aggregates = gb->aggregates;
		  code += HashAggregateCodeAndAllocPredicate(config, op, aggregates, gb->groups.size(),
			                                         cur_node->n_groups, cur_node->n_interventions,
			                                         cur_node->alloc_vars,  cur_node->alloc_vars_index,
			                                         cur_node->alloc_vars_types);
		} else {
		  PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(op);
		  auto &aggregates = gb->aggregates;
		  code += HashAggregateCodeAndAllocPredicate(config, op, aggregates, 0, cur_node->n_groups,
			                                         cur_node->n_interventions,
			                                         cur_node->alloc_vars,  cur_node->alloc_vars_index,
			                                         cur_node->alloc_vars_types);
		}
	}
}

// TODO: subtract incremental agg value from total agg
// TODO: add nested agg for sparse implementation
// TODO: add sparse encoding for ineq predicates
// TODO: add different ranking metrics
// TODO: stream results of aggregate if n * siezof(v) * groups * W > mem
string Fade::PredicateSearch(PhysicalOperator *op, EvalConfig config) {
	std::cout << "Predicate Search" << std::endl;
	// timing vars
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;

	// 1. Parse Spec = table_name.col:scale
	std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec( config);

	// holds any extra data needed during exec
	std::unordered_map<idx_t, unique_ptr<FadeNode>> fade_data;
	std::cout << "gen intervention" << std::endl;
	start_time = std::chrono::steady_clock::now();
	bool compiled = true;
	Fade::GenSparseAndAlloc(config, op, fade_data, columns_spec, compiled);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double gen_time = time_span.count();

	std::cout << "gen code" << std::endl;
	// 4. Alloc vars, generate eval code
	string code;
	start_time = std::chrono::steady_clock::now();
	GenCodeAndAllocPredicate(config, code, op, fade_data, columns_spec);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double prep_time = time_span.count();
  
  	start_time = std::chrono::steady_clock::now();
	GetCachedData(config, op, fade_data, columns_spec);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double data_time = time_span.count();

	std::ostringstream oss;
	oss << get_header(config) << "\n" << code;
	string final_code = oss.str();
	std::cout << final_code << std::endl;

	start_time = std::chrono::steady_clock::now();
	void* handle = compile(final_code, 0);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double compile_time = time_span.count();
	if (handle == nullptr) return "select 0";
	std::cout << "bind" << std::endl;
	BindFunctions(config, handle,  op, fade_data);

	std::vector<std::thread> workers;

	std::cout << "eval" << std::endl;
	start_time = std::chrono::steady_clock::now();
 	for (int i = 0; i < config.num_worker; ++i) {
		workers.emplace_back(InterventionSparseEvalPredicate, i, std::ref(config),  op, std::ref(fade_data), true);
	}

	// Wait for all tasks to complete
	for (std::thread& worker : workers) {
		worker.join();
	}

	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double eval_time = time_span.count();

	if (dlclose(handle) != 0) {
		std::cout<< "Error: %s\n" << dlerror() << std::endl;
	}

	system("rm loop.cpp loop.so");


	if (config.topk > 0) {
		// rank final agg result
		/* std::vector<int> topk_vec = rank(op, config, fade_data);
		std::string result = std::accumulate(topk_vec.begin(), topk_vec.end(), std::string{},
		                                     [](const std::string& a, int b) {
			                                     return a.empty() ? std::to_string(b) : a + "," + std::to_string(b);
		                                     });
    	std::cout << result << std::endl;*/
		// return "select '" + result + "'";
		return "select " + to_string(gen_time) + " as intervention_gen_time, "
				   + to_string(prep_time) + " as prep_time, "
				   + to_string(compile_time) + " as compile_time, "
           + to_string(0) + " as code_gen_time, "
           + to_string(data_time) + " as data_time, "
				   + to_string(eval_time) + " as eval_time";
	} else {
		return "select " + to_string(gen_time) + " as intervention_gen_time, "
				   + to_string(prep_time) + " as prep_time, "
				   + to_string(compile_time) + " as compile_time, "
           + to_string(0) + " as code_gen_time, "
           + to_string(data_time) + " as data_time, "
				   + to_string(eval_time) + " as eval_time";
	}
}

} // namespace duckdb
#endif
