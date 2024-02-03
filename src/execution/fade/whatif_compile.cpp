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
  2. traverse plan to bind variables and execute code
*/

namespace duckdb {


string get_header(bool is_scalar, bool duckdb) {
	std::ostringstream oss;
	oss << R"(
#include <iostream>
#include <unordered_map>
#include <immintrin.h>
)";

	if (duckdb) {
		oss << R"(
#include "duckdb.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
)";
	}


	return oss.str();
}


string get_agg_init(EvalConfig config, int opid, int n_interventions, string fn, string alloc_code,
                    string get_data_code, string get_vals_code) {
	int n_masks = n_interventions / config.mask_size;
	string fname = "agg_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	if (config.use_duckdb) {
			oss << R"(extern "C" int )"
				<< fname
				<< R"((int row_count, int* lineage, __mmask16* var_0, std::unordered_map<std::string, void*>& alloc_vars, duckdb::ChunkCollection &chunk_collection) {
	duckdb::idx_t chunk_count = chunk_collection.ChunkCount();
	std::cout << row_count << " " << chunk_count << std::endl;
)";
	} else {
		    oss << R"(extern "C" int )"
		        << fname
		        << R"((int row_count, int* lineage, __mmask16* var_0, std::unordered_map<std::string, void*>& alloc_vars, std::unordered_map<int, void*>& input_data_map) {
	std::cout << row_count << " " << input_data_map.size() << " " << alloc_vars.size() << std::endl;
)";
	}

	oss << "\tconst int mask_size = " << config.mask_size << ";\n";
	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";
	oss << "\tconst int n_masks  = " << n_masks << ";\n";
	oss << alloc_code;

	if (config.use_duckdb) {
		oss << R"(
	int offset = 0;
	for (int chunk_idx=0; chunk_idx < chunk_count; ++chunk_idx) {
		duckdb::DataChunk &collection_chunk = chunk_collection.GetChunk(chunk_idx);
  )";
	}

	oss << get_data_code;
	if (config.use_duckdb) {
		oss << R"(
		for (int i=0; i < collection_chunk.size(); ++i) {
			int oid = lineage[i+offset];
			int col = oid*n_interventions;
)";
	} else {
		 oss << R"(
	for (int i=0; i < row_count; i++) {
		int oid = lineage[i];
		int col = oid*n_interventions;
)";
	}

	oss << get_vals_code; //for simd get: __m512 val = _mm512_set1_ps(input[i]);


	return oss.str();
}


string get_agg_finalize(EvalConfig config) {
	std::ostringstream oss;

	if (config.is_scalar) oss <<  "\n\t\t\t\t}\n"; // close inner loop
	if (config.use_duckdb) {
		oss << R"(
			}
		}
		offset +=  collection_chunk.size();
	}

	return 0;
})";
	} else {
		oss << R"(
		}
	}

	return 0;
})";
	}
	return oss.str();
}


string get_agg_alloc(int fid, string fn) {
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


string get_agg_simd_eval(string fn, string out_var, string in_var) {
	std::ostringstream oss;
	oss << "{\n";
	oss << "\t__m512 a  = _mm512_load_ps((__m512*) &"+out_var+"[col + row]);\n";
	oss << "\t_mm512_store_ps((__m512*) &"+out_var+"[col + row], _mm512_mask_add_ps(a, tmp_mask, a, "+in_var+"));\n";
	oss << "}\n";
	return oss.str();
}


string get_agg_eval_scalar(string fn, string out_var="", string in_var="") {
	std::ostringstream oss;
	if (fn == "sum") {
		oss << "\t\t\t\t\t";
		oss << out_var+"[col + (row + k) ] +="+ in_var + " * del;\n";
	} else if (fn == "count") {
		oss << "\t\t\t\t\t";
		oss << "out_count[col + (row + k) ] += 1 * del;\n";
	}
	return oss.str();
}

string get_agg_eval(EvalConfig config, int agg_count, string fn, string out_var="", string in_var="") {
	std::ostringstream oss;

	if (agg_count % config.batch == 0) {
		if (agg_count > 0) {
			if (config.is_scalar) {
				oss << "\n\t\t}\n"; // close for (int k=0; k < mask_size; k++)
			}
			oss << "\n\t\t\t}\n"; // close for (int j=0; j < n_masks; j++)
		}

		oss << R"(
			for (int j=0; j < n_masks; j++) {
				int row = j * mask_size;
)";

		if (config.is_scalar) {
			oss << "\t\t\t\tint16_t randomValue = var_0[i*n_masks+j];\n";
		} else {
			oss << "\t\t\t\t__mmask16 tmp_mask = var_0[i*n_masks+j];\n";
		}

		if (config.is_scalar) {
			oss << R"(
				for (int k=0; k < mask_size; k++) {
					int del = (1 &  randomValue >> k);
)";
		}

	}

	if (config.is_scalar) {
		oss << get_agg_eval_scalar(fn, out_var, in_var);
	} else {
		oss << get_agg_simd_eval(fn, out_var, in_var);
	}
	return oss.str();
}

void* compile(std::string code, int id) {
	// Write the loop code to a temporary file
	std::ofstream file("loop.cpp");
	file << code;
	file.close();
	const char* duckdb_lib_path = std::getenv("DUCKDB_LIB_PATH");
	if (duckdb_lib_path == nullptr) {
		// Handle error: environment variable not set
		std::cout << "DUCKDB_LIB_PATH undefined"<< std::endl;
		return nullptr;
	} else {
		std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
		std::string build_command = "g++ -O3 -std=c++2a -mavx512f -march=native -shared -fPIC loop.cpp -o loop.so -L" + std::string(duckdb_lib_path) + " -lduckdb";
		std::cout << duckdb_lib_path << " " << build_command.c_str() << std::endl;
		system(build_command.c_str());
		void *handle = dlopen("./loop.so", RTLD_LAZY);
		if (!handle) {
			std::cerr << "Cannot Open Library: " << dlerror() << std::endl;
		}
		return handle;
	}
}


std::unordered_map<std::string, float> parseWhatifString(EvalConfig config) {
	std::unordered_map<std::string, float> result;
	std::istringstream iss(config.columns_spec_str);
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


void GenRandomWhatifIntervention(EvalConfig config, PhysicalOperator* op,
                           std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                           std::unordered_map<std::string, float> columns_spec,
                           idx_t n_interventions) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenRandomWhatifIntervention(config, op->children[i].get(), fade_data, columns_spec, n_interventions);
	}

	// TODO: add option to push filter / pruned lineage

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

		idx_t n_masks = std::ceil(n_interventions / config.mask_size);
    	std::cout << "GenRandomWhatifIntervention spec = n_masks: " << n_masks << " mask_size: " << config.mask_size << " n_interventions: "<< n_interventions << std::endl;
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
				fade_data[op->id].del_interventions[(i+offset)*n_masks+j] = child_del_interventions[iid*child_n_masks+j];
			}
		}
		offset = result.size();
	} while (cache_on || result.size() > 0);
}


void  JoinIntervene2D(shared_ptr<OperatorLineage> lop,
                   std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                   PhysicalOperator* op) {
	if ( fade_data[op->children[0]->id].n_masks == 0 && fade_data[op->children[1]->id].n_masks == 0) {
		return;
	}

	idx_t child_n_masks = fade_data[op->id].n_masks;
	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;

	__mmask16* lhs_del_interventions = fade_data[op->children[0]->id].del_interventions;
	__mmask16* rhs_del_interventions = fade_data[op->children[1]->id].del_interventions;
	idx_t n_masks = fade_data[op->id].n_masks;
	idx_t row_count = lop->log_index->table_size;
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


string HashAggregateIntervene2D(EvalConfig config, shared_ptr<OperatorLineage> lop,
                            std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                            PhysicalOperator* op) {
	const int n_interventions = fade_data[op->id].n_interventions;

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
		    n_groups = lop->chunk_collection.Count();
	}

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	fade_data[op->id].lineage = std::move(GetGBLineage(lop, row_count));
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
			    string out_var = "out_" + to_string(i); // new output
			    string in_arr = "col_" + to_string(i);  // input arrays
			    string in_val = "val_" + to_string(i);  // input values val = col_x[i]

			    // TODO: check data type

			    if (config.use_duckdb) {
				    // use CollectionChunk that stores input data
				    get_data_code += "\t\tfloat* " + in_arr + " = reinterpret_cast<float *>(collection_chunk.data[" +
				                     to_string(i) + "].GetData());\n";
			    } else {
				    // use unordered_map<int, void*> that stores pointers to input data
				    get_data_code += "\t\tfloat* " + in_arr + " = reinterpret_cast<float *>(input_data_map[" +
				                     to_string(i) + "]);\n";
			    }

			    if (config.is_scalar) {
				    get_vals_code += "\t\t\tfloat " + in_val + "= " + in_arr + "[i];\n";
			    } else {
				    get_vals_code += "\t\t\t__m512 " + in_val + "= _mm512_set1_ps(" + in_arr + "[i]);\n";
			    }
			    // access output arrays
			    alloc_code += get_agg_alloc(i, "sum");
			    // core agg operation
			    eval_code += get_agg_eval(config, agg_count++, "sum", out_var, in_val);
			    fade_data[op->id].alloc_vars[out_var] = aligned_alloc(64, sizeof(float) * n_groups * n_interventions);
		    }
	}

	if (include_count == true) {
		 //   alloc_code += get_agg_alloc(0, "count");
		  //  eval_code += get_agg_eval(agg_count++, batches, is_scalar, "count", "out_count");
		   // fade_data[op->id].alloc_vars["out_count"] = aligned_alloc(64, sizeof(int)*n_groups*n_interventions);
	}


	string init_code = get_agg_init(config, op->id,  fade_data[op->id].n_interventions, "agg", alloc_code, get_data_code, get_vals_code);
	string end_code = get_agg_finalize(config);

	code = init_code + eval_code + end_code;

	return code;
}

void  HashAggregateIntervene2DEval(EvalConfig config, shared_ptr<OperatorLineage> lop,
                                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                  PhysicalOperator* op, void* handle, __mmask16* var_0) {
	PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
	vector<pair<idx_t, idx_t>> aggregate_input_idx;

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	std::vector<int> lineage = fade_data[op->id].lineage;
	string fname = "agg_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	if (config.use_duckdb) {
		int (*fn)(int, int*, __mmask16*, std::unordered_map<std::string, void*>&, ChunkCollection&) = (int(*)(int, int*, __mmask16*, std::unordered_map<std::string, void*>&, ChunkCollection&))dlsym(handle, fname.c_str());
		int result = fn(row_count, lineage.data(), var_0, fade_data[op->id].alloc_vars, op->children[0]->lineage_op->chunk_collection);
	} else {
		std::vector<float> input_vals(row_count, 0);
		std::unordered_map<int, void*> input_data_map;
		for (int i=0; i < fade_data[op->id].alloc_vars.size(); i++) {
			    input_data_map[i] = input_vals.data();
		}

		int (*fn)(int, int*, __mmask16*, std::unordered_map<std::string, void*>&,  std::unordered_map<int, void*>&) = (int(*)(int, int*, __mmask16*, std::unordered_map<std::string, void*>&,  std::unordered_map<int, void*>&))dlsym(handle, fname.c_str());
		int result = fn(row_count, lineage.data(), var_0, fade_data[op->id].alloc_vars, input_data_map);
	}
}

void Intervention2D(EvalConfig config, string& code, PhysicalOperator* op,
                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                  std::unordered_map<std::string, float> columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		Intervention2D(config, code, op->children[i].get(), fade_data, columns_spec);
	}

	// if (op->type == PhysicalOperatorType::TABLE_SCAN) {
	//  TableIntervene(op->lineage_op, op, columns_spec);
	// } else
	if (op->type == PhysicalOperatorType::FILTER) {
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		idx_t row_count = op->lineage_op->log_index->table_size;
		idx_t n_masks = fade_data[op->id].n_masks;
		fade_data[op->id].del_interventions = new __mmask16[row_count * n_masks];
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		idx_t child_n_masks = fade_data[op->children[0]->id].n_masks;
		if ( child_n_masks == 0) {
			    child_n_masks = fade_data[op->children[1]->id].n_masks;
			    fade_data[op->id].n_interventions = fade_data[op->children[1]->id].n_interventions;
		}

		fade_data[op->id].n_masks = child_n_masks;

		idx_t n_masks = fade_data[op->id].n_masks;
		idx_t row_count = op->lineage_op->log_index->table_size;
		fade_data[op->id].del_interventions = new __mmask16[row_count * n_masks];
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].del_interventions = fade_data[op->children[0]->id].del_interventions;
		code += HashAggregateIntervene2D(config, op->lineage_op, fade_data, op);
	} /*else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
	} */ else if (op->type == PhysicalOperatorType::PROJECTION) {
		fade_data[op->id].n_masks  = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].del_interventions  = fade_data[op->children[0]->id].del_interventions;
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
	}
  std::cout << "Intervene: " << op->id << " " << fade_data[op->id].n_masks << std::endl;
}


void Intervention2DEval(EvalConfig config, void* handle, PhysicalOperator* op,
                    std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                    std::unordered_map<std::string, float> columns_spec) {
  for (idx_t i = 0; i < op->children.size(); i++) {
		Intervention2DEval(config, handle, op->children[i].get(), fade_data, columns_spec);
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
		HashAggregateIntervene2DEval(config, op->lineage_op, fade_data, op, handle, var_0);
  } /*else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	  UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
  } */ else if (op->type == PhysicalOperatorType::PROJECTION) {
  }
  std::cout << "Intervene: " << op->id << " " << fade_data[op->id].n_masks << std::endl;
}


void ReleaseFade(EvalConfig config, void* handle, PhysicalOperator* op,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                        std::unordered_map<std::string, float> columns_spec) {

  if (op->type != PhysicalOperatorType::PROJECTION) {
		if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
			    for (auto &pair : fade_data[op->id].alloc_vars) {
				    if (pair.second != nullptr) {
					    free(pair.second);
					    pair.second = nullptr;
				    }
			    }
		} else {
			    if (fade_data[op->id].del_interventions != nullptr) {
				    std::cout << "releasing " << op->id << std::endl;
				    free(fade_data[op->id].del_interventions);
				    fade_data[op->id].del_interventions = nullptr;
			    }
		}
  }

  for (idx_t i = 0; i < op->children.size(); i++) {
		ReleaseFade(config, handle, op->children[i].get(), fade_data, columns_spec);
  }

}

void Fade::Whatif(PhysicalOperator *op, EvalConfig config) {
  // timing vars
  std::chrono::steady_clock::time_point start_time, end_time;
  std::chrono::duration<double> time_span;

  std::unordered_map<std::string, float> columns_spec;
  // 1. Parse Spec
  // e.g. random delete table_name:prob, scale by factor: table_name.col:scale
  columns_spec = parseWhatifString(config);

  // holds any extra data needed during exec
  std::unordered_map<idx_t, FadeDataPerNode> fade_data;

  // 2. Post Process
  start_time = std::chrono::steady_clock::now();
  LineageManager::PostProcess(op);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double post_processing_time = time_span.count();
  std::cout << "1. PostProcess : " << post_processing_time << std::endl;

  // 3. Prepare base interventions; should be one time cost per DB
  start_time = std::chrono::steady_clock::now();
  GenRandomWhatifIntervention(config, op, fade_data, columns_spec, config.n_intervention);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double intervention_gen_time = time_span.count();
  std::cout << "2. GenRandomWhatifIntervention : " << intervention_gen_time << std::endl;

  // 4. Alloc vars, generate eval code
  string code;
  start_time = std::chrono::steady_clock::now();
  Intervention2D(config, code, op, fade_data, columns_spec);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double prep_time = time_span.count();
  std::cout << "3. Intervention2D : " << prep_time << std::endl;

  std::ostringstream oss;
  oss << get_header(config.is_scalar, config.use_duckdb) << "\n" << code;
  string final_code = oss.str();
  std::cout << final_code << std::endl;

  start_time = std::chrono::steady_clock::now();
  void* handle = compile(final_code, 0);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double compile_time = time_span.count();
  std::cout << "4. compile : " << compile_time << std::endl;

  if (handle == nullptr) return;

  start_time = std::chrono::steady_clock::now();
  Intervention2DEval(config, handle, op, fade_data, columns_spec);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double eval_time = time_span.count();
  std::cout << "5. Intervention2DEval : " << eval_time << std::endl;

  if (dlclose(handle) != 0) {
		std::cout<< "Error: %s\n" << dlerror() << std::endl;
  }

  system("rm loop.cpp loop.so");

  double total = prep_time + compile_time + eval_time;
  std::cout << "6. total exec : " << total << std::endl;

  ReleaseFade(config, handle, op, fade_data, columns_spec);


  // Debug
}


} // namespace duckdb
#endif
