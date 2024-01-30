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

/*
  1. traverse plan to construct template
  2. compile
  2. traverse plan to bind variables and execute code?
*/

namespace duckdb {

idx_t mask_size = 16;

string get_header() {
	return R"(
#include <iostream>
#include <unordered_map>
#include <immintrin.h>
)";
}

string get_agg_init(int opid, int mask_size, int n_interventions, string fn, string alloc_code) {
	int n_masks = n_interventions / mask_size;
	string fname = "agg_" + to_string(opid);
	std::ostringstream oss;
	oss << R"(
extern "C" int )"
	    << fname
	    << R"((int row_count, int* lineage, __mmask16* var_0, float* input, std::unordered_map<std::string, void*>& alloc_vars) {
	std::cout << row_count << std::endl;
	const int mask_size = )"
	    << mask_size << ";\n";
	oss << "	const int n_interventions  = " << n_interventions << ";\n";
	oss << "	const int n_masks  = " << n_masks << ";\n";
	oss << alloc_code;
	oss << R"(
	for (int i=0; i < row_count; ++i) {
		int oid = lineage[i];
		float val = input[i];
		int col = oid*n_interventions;

		for (int j=0; j < n_masks; j++) {
			int16_t randomValue = var_0[i*n_masks+j];
			int row = j * mask_size;
)";
	return oss.str();
}

string pre_inner_loop() {
	return R"(
			for (int k=0; k < mask_size; k++) {
				int del = (1 &  randomValue >> k);
)";
}

string post_inner_loop() {
	return R"(
		}
)";
}
string get_agg_finalize() {
	return R"(
		}
	}

	return 0;
})";
}


string get_agg_scalar_alloc(int fid, string fn) {
	std::ostringstream oss;
	if (fn == "sum") {
		oss << R"(
				float* out_)" + to_string(fid) + R"( = (float*)alloc_vars["out_)" + to_string(fid) << R"("];
)";
	} else if (fn == "count") {
		oss << R"(
				int* out_count = (int*)alloc_vars["out_count"];
)";
	}
	return oss.str();
}


string get_agg_scalar_eval(int fid, string fn) {
	std::ostringstream oss;
	if (fn == "sum") {
		oss << R"(
			out_)" + to_string(fid) + R"([col + (row + k) ] += val * del;
)";
	} else if (fn == "count") {
		oss << R"(
			out_count[col + (row + k) ] += 1 * del;
)";
	}
	return oss.str();
}

string get_agg_simd_eval(int mask_size, int n_interventions) {
	int n_masks = n_interventions / mask_size;
	std::ostringstream oss;
	oss << R"(
extern "C" int loop_fn_simd(int row_count, int* lineage, __mmask16* var_0, float* input, float* out) {
	std::cout << row_count << " " << lineage[0] << std::endl;
	int mask_size = )" << mask_size << ";";
	oss << "	int n_interventions  = " << n_interventions << ";";
	oss << "	int n_masks = " << n_masks << ";";
	oss << R"(__m512 a_6  = _mm512_load_ps((__m512*) &out[2*n_interventions + 0*mask_size]);
	for (int i=0; i < row_count; ++i) {
		int oid = lineage[i];
		__m512 val = _mm512_set1_ps(input[i]);
		for (int j=0; j < n_masks; j++) {
			__mmask16 tmp_mask = var_0[i*n_masks+j];
			a_6  = _mm512_load_ps((__m512*) &out[oid*n_interventions + j*mask_size]);
			_mm512_store_ps((__m512*) &out[oid*n_interventions + j*mask_size], _mm512_mask_add_ps(a_6, tmp_mask, a_6, val));
		}
	}
	std::cout << "done" << std::endl;
	return 0;
}
)";

	return oss.str();
}

void* compile(std::string code, int id) {
	// Write the loop code to a temporary file
	std::ofstream file("loop.cpp");
	file << code;
	file.close();
	std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
	system("g++ -O3  -mavx512f  -march=native -c -o  loop.o -fPIC loop.cpp");
	system("g++ -shared -o loop.so loop.o");
	void *handle = dlopen("./loop.so", RTLD_LAZY);
	if (!handle) {
		std::cerr << "Cannot Open Library: " << dlerror() << std::endl;
	}
	return handle;
}


std::unordered_map<std::string, float> parseWhatifString(string intervention_type, const std::string& input) {
	std::unordered_map<std::string, float> result;
	std::istringstream iss(input);
	std::string token;

	while (std::getline(iss, token, '|')) {
		std::istringstream tokenStream(token);
		std::string table, prob;

		if (std::getline(tokenStream, table, ':')) {
			if (std::getline(tokenStream, prob)) {
				result[table] = std::stof(prob);
			}
		}
	}

	return result;
}


void GenRandomWhatifIntervention(PhysicalOperator* op,
                           std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                           std::unordered_map<std::string, float> columns_spec,
                           idx_t n_interventions) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenRandomWhatifIntervention(op->children[i].get(), fade_data, columns_spec, n_interventions);
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (columns_spec.find(op->lineage_op->table_name) == columns_spec.end()) {
			return;
		}
		idx_t row_count = op->lineage_op->chunk_collection.Count();
		// allocate deletion intervention: n_intervention X row_count

		float probability = columns_spec[op->lineage_op->table_name];

		// Initialize a random number generator
		const unsigned int seed = 42;
		std::random_device rd;
		std::mt19937 gen(seed);
		std::uniform_real_distribution<double> dis(0.0, 1.0);
		std::uniform_int_distribution<int> dist_255(0, 255);

		idx_t n_masks = std::ceil(n_interventions / mask_size);
    	std::cout << n_masks << " " << mask_size << " "<< n_interventions << std::endl;
		__mmask16* del_interventions = new __mmask16[row_count * n_masks];
		for (idx_t i = 0; i < row_count; ++i) {
			for (idx_t j = 0; j < n_masks; ++j) {
				__mmask16 randomValue = static_cast<int16_t>(dist_255(gen));
				del_interventions[i*n_masks+j] = randomValue;
			}
		}

		fade_data[op->id].del_interventions = del_interventions;
		fade_data[op->id].n_interventions = n_interventions;
		fade_data[op->id].n_masks = n_masks;
	}
}


void  FilterIntervene2D(shared_ptr<OperatorLineage> lop,
                     std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                     PhysicalOperator* op) {
	fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
	fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;
	fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;

	if ( fade_data[op->id].n_masks == 0) {
		return;
	}
	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	__mmask16* child_del_interventions = fade_data[op->children[0]->id].del_interventions;
	idx_t row_count = lop->log_index->table_size;
	idx_t n_masks = fade_data[op->id].n_masks;
	__mmask16* del_interventions = new __mmask16[row_count * n_masks];
	idx_t offset = 0;
	idx_t child_n_masks = fade_data[op->children[0]->id].n_masks;
	do {
		cache_on = false;
		result.Reset();
		result.Destroy();
		op->lineage_op->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
		result.Flatten();
		if (result.size() == 0) continue;
		unsigned int * in_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
		for (idx_t i=0; i < result.size(); ++i) {
			idx_t iid = in_index[i];
			for (idx_t j=0; j < n_masks; j++) {
				del_interventions[(i+offset)*n_masks+j] = child_del_interventions[iid*child_n_masks+j];
			}
		}
		offset = result.size();
	} while (cache_on || result.size() > 0);
	fade_data[op->id].del_interventions = del_interventions;
}


void  JoinIntervene2D(shared_ptr<OperatorLineage> lop,
                   std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                   PhysicalOperator* op) {
	if ( fade_data[op->children[0]->id].n_masks == 0 && fade_data[op->children[1]->id].n_masks == 0) {
		return;
	}

	fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
	idx_t child_n_masks = fade_data[op->children[0]->id].n_masks;
	if ( child_n_masks == 0) {
		child_n_masks = fade_data[op->children[1]->id].n_masks;
		fade_data[op->id].n_interventions = fade_data[op->children[1]->id].n_interventions;
	}

	fade_data[op->id].n_masks = child_n_masks;
	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;

	__mmask16* lhs_del_interventions = fade_data[op->children[0]->id].del_interventions;
	__mmask16* rhs_del_interventions = fade_data[op->children[1]->id].del_interventions;
	idx_t row_count = lop->log_index->table_size;
	idx_t n_masks = fade_data[op->id].n_masks;
	fade_data[op->id].del_interventions = new __mmask16[row_count * n_masks];
	idx_t offset = 0;
	do {
			cache_on = false;
			result.Reset();
			result.Destroy();
			op->lineage_op->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
			result.Flatten();
			if (result.size() == 0) continue;
			unsigned int * lhs_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
			unsigned int * rhs_index = reinterpret_cast<unsigned int *>(result.data[1].GetData());
		    if ( fade_data[op->children[0]->id].n_masks > 0 && fade_data[op->children[1]->id].n_masks > 0) {
				for (idx_t i=0; i < result.size(); ++i) {
					idx_t lhs = lhs_index[i];
					idx_t rhs = rhs_index[i];
					for (idx_t j=0; j < n_masks; j++) {
						fade_data[op->id].del_interventions[(i+offset)*n_masks+j] =
					        lhs_del_interventions[lhs*child_n_masks+j] * rhs_del_interventions[rhs*child_n_masks+j];
					}
				}
		    } else if (fade_data[op->children[0]->id].n_masks > 0) {
			    for (idx_t i=0; i < result.size(); ++i) {
				    idx_t lhs = lhs_index[i];
				    for (idx_t j=0; j < n_masks; j++) {
					    fade_data[op->id].del_interventions[(i+offset)*n_masks+j] = lhs_del_interventions[lhs*child_n_masks+j];
				    }
			    }
		    } else {
			    for (idx_t i=0; i < result.size(); ++i) {
				    idx_t rhs = rhs_index[i];
				    for (idx_t j=0; j < n_masks; j++) {
					    fade_data[op->id].del_interventions[(i+offset)*n_masks+j] = rhs_del_interventions[rhs*child_n_masks+j];
				    }
			    }
		    }
		    offset += result.size();
	} while (cache_on || result.size() > 0);
}

template<class T>
vector<vector<T>> SumRecompute2D(PhysicalOperator* op,
                               std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                               shared_ptr<OperatorLineage> lop,
                               BoundAggregateExpression& aggr, idx_t n_interventions,
                               idx_t n_groups, vector<idx_t> aggregate_input_idx) {
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	bool cache_on = false;
  std::vector<T> input_values;

	idx_t chunk_count = op->children[0]->lineage_op->chunk_collection.ChunkCount();

	// to parallelize, first need to materialize input chunks, then divide them between threads
	// this is necessary only if we are computing aggregates without the subtraction property
	// since we need to iterate over all interventions and recompute the aggregates

	idx_t child_n_masks = fade_data[op->children[0]->id].n_masks;
	__mmask16* child_del_interventions = fade_data[op->children[0]->id].del_interventions;
  std::vector<std::vector<T>> new_vals(n_groups, std::vector<T> (child_n_masks*mask_size, 0));
vector<vector<T>> new_vals2(n_groups, vector<T> (child_n_masks*mask_size, 0));

  std::cout << "sum " << child_n_masks << std::endl;
  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
	idx_t offset = 0;
	for (idx_t chunk_idx=0; chunk_idx < chunk_count; ++chunk_idx) {
		    DataChunk &collection_chunk = op->children[0]->lineage_op->chunk_collection.GetChunk(chunk_idx);
		    idx_t col_idx = aggregate_input_idx[0];
		    T* col = reinterpret_cast<T*>(collection_chunk.data[col_idx].GetData());
		    if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
			    for (idx_t i=0; i < collection_chunk.size(); ++i) {
				    for (idx_t j=0; j < child_n_masks; j++) {
					    __mmask16 randomValue = child_del_interventions[(i+offset)*child_n_masks+j];
					    for (idx_t k=0; k < mask_size; k++) {
						    int del = (1 &  randomValue >> k);
						    new_vals[0][j*mask_size + k] += col[i] * del;
					    }
				    }
			    }

		    } else {
			    for (idx_t i=0; i < collection_chunk.size(); ++i) {
				    input_values.push_back(col[i]);
			    }
		    }
		    offset +=  collection_chunk.size();
	}
  std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
  std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double execution_time = time_span.count();

  std::cout << "phase 1: " << execution_time << std::endl;
	// then specialize recomputation based on the aggregate type with possible SIMD implementations?
	if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		    return new_vals2;
	}

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
  std::vector<idx_t> lineage(row_count, 0);
  std::cout << "row count: " << row_count << std::endl;
  start_time = std::chrono::steady_clock::now();
	do {
		    cache_on = false;
		    result.Reset();
		    result.Destroy();
		    lop->GetLineageAsChunk(result, global_count, local_count,
		                           current_thread, log_id, cache_on);
		    result.Flatten();
		    if (result.size() == 0) continue;
		    int64_t * in_index = reinterpret_cast<int64_t *>(result.data[0].GetData());
		    int * out_index = reinterpret_cast<int *>(result.data[1].GetData());
		    for (idx_t i=0; i < result.size(); ++i) {
			    idx_t iid = in_index[i];
			    idx_t oid = out_index[i];
          lineage[iid] = oid;
		    }

	} while (cache_on || result.size() > 0);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  execution_time = time_span.count();

  std::cout << "phase 2: " << execution_time << " " << child_n_masks << " " << mask_size << " " << n_groups << std::endl;

  return new_vals2;
}

std::vector<int> GetGBLineage(shared_ptr<OperatorLineage> lop, int row_count) {
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	bool cache_on = false;
	std::vector<int> lineage(row_count);
	do {
		    cache_on = false;
		    result.Reset();
		    result.Destroy();
		    lop->GetLineageAsChunk(result, global_count, local_count,
		                           current_thread, log_id, cache_on);
		    result.Flatten();
		    int64_t * in_index = reinterpret_cast<int64_t *>(result.data[0].GetData());
		    int * out_index = reinterpret_cast<int *>(result.data[1].GetData());
		    for (idx_t i=0; i < result.size(); ++i) {
			    idx_t iid = in_index[i];
			    idx_t oid = out_index[i];
			    lineage[iid] = oid;
		    }
	} while (cache_on || result.size() > 0);
	return lineage;
}


string HashAggregateIntervene2D(shared_ptr<OperatorLineage> lop,
                            std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                            PhysicalOperator* op) {

	fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
	const int n_interventions = fade_data[op->id].n_interventions;
	fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;
	fade_data[op->id].del_interventions = fade_data[op->children[0]->id].del_interventions;

	string eval_code;
	string code;
	string alloc_code;

	PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
	auto &aggregates = gb->grouped_aggregate_data.aggregates;
	// get n_groups: max(oid)+1
	idx_t n_groups = 1;
	if (op->type != PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		    n_groups = lop->chunk_collection.Count();
	}

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	fade_data[op->id].lineage = std::move(GetGBLineage(lop, row_count));
	bool include_count = false;

	int batches = 4;
	int agg_count = 0;
	// Populate the aggregate child vectors
	for (idx_t i=0; i < aggregates.size(); i++) {
		    if (agg_count % batches == 0) {
			    if (agg_count > 0) {
				    eval_code += post_inner_loop();
			    }
			    eval_code += pre_inner_loop();
		    }
		    auto &aggr = aggregates[i]->Cast<BoundAggregateExpression>();
		    vector<idx_t> aggregate_input_idx;
		    for (auto &child_expr : aggr.children) {
			    D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			    auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			    aggregate_input_idx.push_back(bound_ref_expr.index);
		    }
		    string name = aggr.function.name;
		    if (name == "sum" || name == "sum_no_overflow") {
			    agg_count++;
			    alloc_code += get_agg_scalar_alloc(i, "sum");
			    eval_code += get_agg_scalar_eval(i, "sum");
			    fade_data[op->id].alloc_vars["out_"+to_string(i)] = aligned_alloc(64, sizeof(float)*n_groups*n_interventions);;
		    } else if (include_count == false && (name == "count" || name == "count_star")) {
			    include_count = true;
		    } else if (name == "avg") {
			    agg_count++;
			    if (include_count == false) {
				    include_count = true;
			    }
			    alloc_code += get_agg_scalar_alloc(i, "sum");
			    eval_code += get_agg_scalar_eval(i, "sum");
			    fade_data[op->id].alloc_vars["out_"+to_string(i)] = aligned_alloc(64, sizeof(float)*n_groups*n_interventions);;
		    }
	}

	if (include_count == true) {
		    if (agg_count % batches == 0) {
			    if (agg_count > 0) {
				    eval_code += post_inner_loop();
			    }
			    eval_code += pre_inner_loop();
		    }
		    alloc_code += get_agg_scalar_alloc(0, "count");
		    eval_code += get_agg_scalar_eval(0, "count");
		    fade_data[op->id].alloc_vars["out_count"] = aligned_alloc(64, sizeof(int)*n_groups*n_interventions);
	}

	eval_code += post_inner_loop();

	string init_code = get_agg_init(op->id, 16, fade_data[op->id].n_interventions, "agg", alloc_code);
	string end_code = get_agg_finalize();

	code = init_code + eval_code + end_code;

	return code;
}

void  HashAggregateIntervene2DEval(shared_ptr<OperatorLineage> lop,
                                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                  PhysicalOperator* op, void* handle, __mmask16* var_0) {
	PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
	vector<pair<idx_t, idx_t>> aggregate_input_idx;

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	std::vector<int> lineage = fade_data[op->id].lineage;
	string fname = "agg_"+ to_string(op->id);
	std::vector<float> input_vals(row_count, 0); //get this here
	int (*fn)(int, int*, __mmask16*, float*, std::unordered_map<std::string, void*>&) = (int(*)(int, int*, __mmask16*, float*,  std::unordered_map<std::string, void*>&))dlsym(handle, fname.c_str());
	int result = fn(row_count, lineage.data(), var_0, input_vals.data(), fade_data[op->id].alloc_vars);
}

void Intervention2D(string& code, PhysicalOperator* op,
                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                  std::unordered_map<std::string, float> columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		Intervention2D(code, op->children[i].get(), fade_data, columns_spec);
	}

	// if (op->type == PhysicalOperatorType::TABLE_SCAN) {
	//  TableIntervene(op->lineage_op, op, columns_spec);
	// } else
	if (op->type == PhysicalOperatorType::FILTER) {
		FilterIntervene2D(op->lineage_op, fade_data, op);
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		JoinIntervene2D(op->lineage_op, fade_data, op);
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		code += HashAggregateIntervene2D(op->lineage_op, fade_data, op);
	} /*else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
	} */ else if (op->type == PhysicalOperatorType::PROJECTION) {
		fade_data[op->id].n_masks  = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].del_interventions  = fade_data[op->children[0]->id].del_interventions;
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
	}
  std::cout << "Intervene: " << op->id << " " << fade_data[op->id].n_masks << std::endl;
}


void Intervention2DEval(void* handle, PhysicalOperator* op,
                    std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                    std::unordered_map<std::string, float> columns_spec) {
  for (idx_t i = 0; i < op->children.size(); i++) {
		Intervention2DEval(handle, op->children[i].get(), fade_data, columns_spec);
  }

  if (op->type == PhysicalOperatorType::FILTER) {
		FilterIntervene2D(op->lineage_op, fade_data, op);
  } else if (op->type == PhysicalOperatorType::HASH_JOIN
	         || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	         || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	         || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	         || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		JoinIntervene2D(op->lineage_op, fade_data, op);
  } else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		__mmask16* var_0 = fade_data[op->id].del_interventions;
		HashAggregateIntervene2DEval(op->lineage_op, fade_data, op, handle, var_0);
  } /*else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	  UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
  } */ else if (op->type == PhysicalOperatorType::PROJECTION) {
		fade_data[op->id].n_masks  = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].del_interventions  = fade_data[op->children[0]->id].del_interventions;
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
  }
  std::cout << "Intervene: " << op->id << " " << fade_data[op->id].n_masks << std::endl;
}

void Fade::Whatif(PhysicalOperator *op, string intervention_type, string columns_spec_str, int n_intervention) {
  std::unordered_map<std::string, float> columns_spec;


  columns_spec = parseWhatifString(intervention_type, columns_spec_str);

  // holds any extra data needed during exec
  std::unordered_map<idx_t, FadeDataPerNode> fade_data;

  // 2. Post Process
  LineageManager::PostProcess(op);

  // 4. Prepare base interventions; should be one time cost per DB
  GenRandomWhatifIntervention(op, fade_data, columns_spec, n_intervention);

  string code;
  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
  Intervention2D(code, op, fade_data, columns_spec);
  std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
  std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double execution_time = time_span.count();
  std::cout << "1. NTERVENTION_TIME : " << execution_time << std::endl;

  std::ostringstream oss;
  oss << get_header() << "\n" << code;
  string final_code = oss.str();
  std::cout << final_code << std::endl;
  void* handle = compile(final_code, 0);

  start_time = std::chrono::steady_clock::now();
  Intervention2DEval(handle, op, fade_data, columns_spec);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  execution_time = time_span.count();
  std::cout << "3. NTERVENTION_TIME : " << execution_time << std::endl;
  system("rm loop.cpp loop.o loop.so");
}


} // namespace duckdb
#endif
