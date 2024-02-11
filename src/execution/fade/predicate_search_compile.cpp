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


string FilterCodeAndAllocPredicate(EvalConfig config, PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
	    << fname
	    << R"((int thread_id, int* lineage,  void* __restrict__ var_ptr, void* __restrict__ out_ptr) {
)";

	oss << "\t int* __restrict__ out = (int* __restrict__)out_ptr;\n";
	oss << "\t int* __restrict__ var = (int* __restrict__)var_ptr;\n";

	int row_count = fade_data[op->id].lineage[0].size();

	oss << "\tconst int row_count = " + to_string(row_count) + ";\n";

	if (config.num_worker > 0) {
		int batch_size = row_count / config.num_worker;
		if (row_count % config.num_worker > 0)
			batch_size++;
		oss << "\tconst int batch_size  = " << batch_size << ";\n";
		oss << "\tint start = thread_id * batch_size;\n";
		oss << "\tint end   = start + batch_size;\n";
		oss << "\tif (end >= row_count) { end = row_count; }\n";
	} else {
		oss << "\tint start = 0;\n";
		oss << "\tint end   = row_count;\n";
	}
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


string JoinCodeAndAllocPredicate(EvalConfig config, PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
	    << fname
	    << R"((int thread_id, int* lhs_lineage, int* rhs_lineage,  void* __restrict__ lhs_var_ptr,  void* __restrict__ rhs_var_ptr,  void* __restrict__ out_ptr) {
)";

	oss << "\t int* __restrict__ out = (int* __restrict__)out_ptr;\n";
	oss << "\t int* __restrict__ rhs_var = (int* __restrict__)rhs_var_ptr;\n";
	oss << "\t int* __restrict__ lhs_var = (int* __restrict__)lhs_var_ptr;\n";

	int row_count = fade_data[op->id].lineage[0].size();
	oss << "\tconst int row_count = " + to_string(row_count) + ";\n";
	oss << "\tconst int right_n_interventions = " + to_string(fade_data[op->children[1]->id].n_interventions) + ";\n";

	if (config.num_worker > 0) {
		int batch_size = row_count / config.num_worker;
		if (row_count % config.num_worker > 0)
			batch_size++;
		oss << "\tconst int batch_size  = " << batch_size << ";\n";
		oss << "\tint start = thread_id * batch_size;\n";
		oss << "\tint end   = start + batch_size;\n";
		oss << "\tif (end >= row_count) { end = row_count; }\n";
	} else {
		oss << "\tint start = 0;\n";
		oss << "\tint end   = row_count;\n";
	}

	oss << "\tfor (int i=start; i < end; i++) {\n";


	if ( fade_data[op->children[0]->id].n_interventions > 1 && fade_data[op->children[1]->id].n_interventions > 1) {
		oss << R"(out[i] = lhs_var[lhs_lineage[i]] * right_n_interventions + rhs_var[rhs_lineage[i]];)";
	} else if (fade_data[op->children[0]->id].n_interventions > 1) {
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
	int n_masks = n_interventions / config.mask_size;
	string fname = "agg_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	if (config.use_duckdb) {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars, duckdb::ChunkCollection &chunk_collection) {
)";
	} else {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars, std::unordered_map<int, void*>& input_data_map) {
)";
	}

	oss << "\t int* __restrict__ var_0 = (int* __restrict__)var_0_ptr;\n";

	oss << "\tconst int chunk_count = " << chunk_count << ";\n";
	oss << "\tconst int row_count = " << row_count << ";\n";
	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";

	if (config.num_worker > 0) {
		oss << "\tconst int num_threads  = " << config.num_worker << ";\n";
		if (config.use_duckdb) {
			int batch_size = chunk_count / config.num_worker;
			if (chunk_count % config.num_worker > 0)
				batch_size++;
			oss << "\tconst int batch_size  = " << batch_size << ";\n";
			oss << "\tconst int start = thread_id * batch_size;\n";
			oss << "\tint end   = start + batch_size;\n";
			oss << "\tif (end >= chunk_count) { end = chunk_count; }\n";
		} else {
			int batch_size = row_count / config.num_worker;
			if (row_count % config.num_worker > 0)
				batch_size++;
			oss << "\tconst int batch_size  = " << batch_size << ";\n";
			oss << "\tconst int start = thread_id * batch_size;\n";
			oss << "\tint end   = start + batch_size;\n";
			oss << "\tif (end >= row_count) { end = row_count; }\n";
		}
	} else {
		oss << "\tconst int start = 0;\n";
		oss << "\tconst int end   = row_count;\n";
	}
	oss << "std::cout << \"agg \" << chunk_count << \" \" <<  row_count << \" \" << n_interventions << \" \" << start << \" \" << end << std::endl;";

	oss << alloc_code;

	if (config.use_duckdb) {
		oss << R"(
	int offset = 0;
	for (int chunk_idx=start; chunk_idx < end; ++chunk_idx) {
		duckdb::DataChunk &collection_chunk = chunk_collection.GetChunk(chunk_idx);
  )";
	}

	oss << get_data_code;
	if (config.use_duckdb) {
		oss << R"(
		for (int i=0; i < collection_chunk.size(); ++i) {
			int oid = lineage[i+offset];
			int col = oid*n_interventions;
			int row = var_0[i+offset];
)";
	} else {
		oss << R"(
	for (int i=start; i < end; i++) {
		int oid = lineage[i];
		int col = oid*n_interventions;
		int row = var_0[i];
)";
	}

	oss << get_vals_code;


	return oss.str();
}

string get_agg_eval_predicate(EvalConfig config, int agg_count, string fn, string out_var="", string in_var="", string data_type="int") {
	std::ostringstream oss;
	// if functions are incrementally removable, then agg the part
	if (fn == "sum") {
		oss << "\t\t";
		oss << out_var + "[col+row] +=" + in_var + ";\n";
	} else if (fn == "count") {
		oss << "\t\t";
		oss << "out_count[col+row] += 1;\n";
	}
	return oss.str();
}

string HashAggregateCodeAndAllocPredicate(EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                PhysicalOperator* op) {
	string eval_code;
	string code;
	string alloc_code;
	string get_data_code;
	string get_vals_code;

	PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
	auto &aggregates = gb->grouped_aggregate_data.aggregates;
	// get n_groups: max(oid)+1
	idx_t n_groups = 1;
	if (op->type != PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		n_groups = lop->log_index->ha_hash_index.size(); //lop->chunk_collection.Count();
	}

	fade_data[op->id].n_groups = n_groups;

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	bool include_count = false;
	int batches = 4;
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
			int col_idx = i + gb->grouped_aggregate_data.groups.size();

			string out_var = "out_" + to_string(i); // new output
			string in_arr = "col_" + to_string(i);  // input arrays
			string in_val = "val_" + to_string(i);  // input values val = col_x[i]

			string input_type = fade_data[op->id].alloc_vars_types[out_var];
			string output_type = fade_data[op->id].alloc_vars_types[out_var];

			if (config.use_duckdb) {
				// use CollectionChunk that stores input data
				get_data_code += "\t\t"+input_type+"* " + in_arr + " = reinterpret_cast<"+input_type+" *>(collection_chunk.data[" +
				                 to_string(col_idx) + "].GetData());\n";
			} else {
				// use unordered_map<int, void*> that stores pointers to input data
				get_data_code += "\t\t"+input_type+"* " + in_arr + " = reinterpret_cast<"+input_type+" *>(input_data_map[" +
				                 to_string(i) + "]);\n";
			}

			get_vals_code += "\t\t\t" + output_type +" " + in_val + "= " + in_arr + "[i];\n";
			// access output arrays
			alloc_code += Fade::get_agg_alloc(i, "sum", output_type);
			// core agg operation
			eval_code += get_agg_eval_predicate(config, agg_count++, "sum", out_var, in_val, output_type);
		}
	}

	if (include_count == true) {
		string out_var = "out_count";
		alloc_code += Fade::get_agg_alloc(0, "count", "int");
		eval_code += get_agg_eval_predicate(config, agg_count++, "count", out_var, "1", "int");
	}


	string init_code = get_agg_init_predicate(config, row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(), op->id,  fade_data[op->id].n_interventions, "agg", alloc_code, get_data_code, get_vals_code);
	string end_code;

	if (config.use_duckdb) {
		end_code += "\t} \n offset +=  collection_chunk.size();\n}\nreturn 0;\n}\n";
	} else {
		end_code += "\t}\n \treturn 0;\n}\n";
	}

	code = init_code + eval_code + end_code;

	return code;
}

void GenCodeAndAllocPredicate(EvalConfig& config, string& code, PhysicalOperator* op,
                              std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                              std::unordered_map<std::string, std::vector<std::string>> columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenCodeAndAllocPredicate(config, code, op->children[i].get(), fade_data, columns_spec);
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (columns_spec.find(op->lineage_op->table_name) == columns_spec.end()) {
			fade_data[op->id].n_interventions = 1;
			return;
		}

		// TODO: compile factorize or randomGen
		if (config.n_intervention == 0) {
			// factorize need access to base table
			std::pair<vector<int>, idx_t> res = Fade::factorize(op, op->lineage_op, columns_spec);
			fade_data[op->id].annotations = res.first;
			fade_data[op->id].n_interventions = res.second;
		} else {
			// random need access to  config.n_intervention
			fade_data[op->id].n_interventions = config.n_intervention;
			fade_data[op->id].annotations = Fade::random_unique(op->lineage_op, config.n_intervention);
		}
	} else if (op->type == PhysicalOperatorType::FILTER) {
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		if ( fade_data[op->id].n_interventions <= 1) {
			return;
		}
		fade_data[op->id].annotations.resize(fade_data[op->id].lineage[0].size());
		code += FilterCodeAndAllocPredicate(config, op, op->lineage_op, fade_data);
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		idx_t left_n_interventions =  fade_data[op->children[0]->id].n_interventions;
		idx_t right_n_interventions =  fade_data[op->children[1]->id].n_interventions;
		fade_data[op->id].n_interventions = left_n_interventions * right_n_interventions;
		if (fade_data[op->id].n_interventions <= 1) return;
		fade_data[op->id].annotations.resize(fade_data[op->id].lineage[0].size());
		code += JoinCodeAndAllocPredicate(config, op, op->lineage_op, fade_data);
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		fade_data[op->id].annotations.resize(row_count);
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		Fade::HashAggregateAllocate(config, op->lineage_op, fade_data, op);
		code += HashAggregateCodeAndAllocPredicate(config, op->lineage_op, fade_data, op);
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	  //  UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
	}  else if (op->type == PhysicalOperatorType::PROJECTION) {
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
	}
}

void Intervention2DEvalPredicate(int thread_id, EvalConfig config, void* handle, PhysicalOperator* op,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		Intervention2DEvalPredicate(thread_id, config, handle, op->children[i].get(), fade_data);
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		if (config.prune) return;
		if (fade_data[op->id].n_interventions <= 1) return;
		idx_t row_count = fade_data[op->id].lineage[0].size();
		int result = fade_data[op->id].filter_fn(thread_id, fade_data[op->id].lineage[0].data(),
			                                         fade_data[op->children[0]->id].annotations.data(),
			                                         fade_data[op->id].annotations.data());
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		if (fade_data[op->id].n_interventions <= 1) return;
		idx_t row_count = fade_data[op->id].lineage[0].size();
		int result = fade_data[op->id].join_fn(thread_id, fade_data[op->id].lineage[0].data(),
			                                       fade_data[op->id].lineage[1].data(), fade_data[op->children[0]->id].annotations.data(),
			                                       fade_data[op->children[1]->id].annotations.data(), fade_data[op->id].annotations.data());
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		if (fade_data[op->id].n_interventions <= 1) return;
		if (config.use_duckdb) {
			int result = fade_data[op->id].agg_duckdb_fn(thread_id, fade_data[op->id].lineage[0].data(),
			                                             fade_data[op->children[0]->id].annotations.data(), fade_data[op->id].alloc_vars,
			                                             op->children[0]->lineage_op->chunk_collection);
		} else {
			int result = fade_data[op->id].agg_fn(thread_id, fade_data[op->id].lineage[0].data(),
			                                      fade_data[op->children[0]->id].annotations.data(), fade_data[op->id].alloc_vars,
			                                      fade_data[op->id].input_data_map);
		}
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	} else if (op->type == PhysicalOperatorType::PROJECTION) {
		fade_data[op->id].annotations = std::move(fade_data[op->children[0]->id].annotations);
	}
}


void BindFunctionsPredicate(EvalConfig config, void* handle, PhysicalOperator* op,
                   std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {

	for (idx_t i = 0; i < op->children.size(); i++) {
		BindFunctionsPredicate(config, handle, op->children[i].get(), fade_data);
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		if (config.prune) return;
		if (fade_data[op->id].n_interventions <= 1) return;
		string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		fade_data[op->id].filter_fn = (int(*)(int, int*, void*, void*))dlsym(handle, fname.c_str());
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		if (fade_data[op->id].n_interventions <= 1) return;
		string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		fade_data[op->id].join_fn = (int(*)(int, int*, int*, void*, void*, void*))dlsym(handle, fname.c_str());
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		string fname = "agg_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		if (config.use_duckdb) {
			fade_data[op->id].agg_duckdb_fn = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&, ChunkCollection&))dlsym(handle, fname.c_str());
		} else {
			fade_data[op->id].agg_fn = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,  std::unordered_map<int, void*>&))dlsym(handle, fname.c_str());
		}
	}
}

string Fade::PredicateSearch(PhysicalOperator *op, EvalConfig config) {
	// timing vars
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;

	// 1. Parse Spec = table_name.col:scale
	std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec( config);

	// holds any extra data needed during exec
	std::unordered_map<idx_t, FadeDataPerNode> fade_data;

	// 2. Post Process
	start_time = std::chrono::steady_clock::now();
	LineageManager::PostProcess(op);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double post_processing_time = time_span.count();

	// 3. retrieve lineage
	start_time = std::chrono::steady_clock::now();
	Fade::GetLineage(config, op, fade_data);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double lineage_time = time_span.count();

	// 4.1 Prune
	double prune_time = 0;
	if (config.prune) {
		start_time = std::chrono::steady_clock::now();
		vector<int> out_order;
		Fade::PruneLineage(config, op, fade_data, out_order);
		end_time = std::chrono::steady_clock::now();
		time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
		prune_time = time_span.count();
	}

	// 4. Alloc vars, generate eval code
	string code;
	start_time = std::chrono::steady_clock::now();
	GenCodeAndAllocPredicate(config, code, op, fade_data, columns_spec);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double prep_time = time_span.count();

	std::ostringstream oss;
	oss << get_header(config) << "\n" << fill_random_code(config) << "\n"  << code;
	string final_code = oss.str();

	//if (config.debug)
		std::cout << final_code << std::endl;

	start_time = std::chrono::steady_clock::now();
	void* handle = compile(final_code, 0);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double compile_time = time_span.count();

	if (handle == nullptr) return "select 0";

	BindFunctionsPredicate(config, handle,  op, fade_data);

	std::vector<std::thread> workers;

	start_time = std::chrono::steady_clock::now();

	for (int i = 0; i < config.num_worker; ++i) {
		workers.emplace_back(Intervention2DEvalPredicate, i, config, handle,  op, std::ref(fade_data));
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

	ReleaseFade(config, handle, op, fade_data);

	return "select " + to_string(post_processing_time) + " as post_processing_time, "
	       + to_string(0) + " as intervention_gen_time, "
	       + to_string(prep_time) + " as prep_time, "
	       + to_string(lineage_time) + " as lineage_time, "
	       + to_string(prune_time) + " as prune_time, "
	       + to_string(compile_time) + " as compile_time, "
	       + to_string(eval_time) + " as eval_time";
}

} // namespace duckdb
#endif
