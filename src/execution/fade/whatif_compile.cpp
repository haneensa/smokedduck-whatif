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
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include <thread>
#include <vector>

/*
  1. traverse plan to construct template
  2. compile
  2. traverse plan to bind variables and execute code
*/

namespace duckdb {


string get_header(EvalConfig config) {
	std::ostringstream oss;
	oss << R"(
#include <iostream>
#include <vector>
#include <unordered_map>
#include <immintrin.h>
#include <barrier>
#include <random>
)";

	if (config.use_duckdb) {
		oss << R"(
#include "duckdb.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
)";
	}

	if (config.num_worker > 1) {
		oss << "std::barrier sync_point(" + to_string(config.num_worker) + ");\n";
	}

	return oss.str();
}

string fill_random_code(EvalConfig config) {
	std::ostringstream oss;
	oss << R"(
extern "C" int fill_random(int row_count, float prob, int n_masks, __mmask16* del_interventions) {
	std::cout << "fill_random: " << row_count  << " " << prob << " " <<  n_masks << std::endl;

	// Initialize a random number generator
	const unsigned int seed = 42;
	std::random_device rd;
	std::mt19937 gen(seed);
	std::uniform_real_distribution<double> dis(0.0, 1.0);
	std::uniform_int_distribution<int> dist_255(0, 255);

	for (int i = 0; i < row_count; ++i) {
		for (int j = 0; j < n_masks; ++j) {
			__mmask16 randomValue = static_cast<int16_t>(dist_255(gen));
			del_interventions[i*n_masks+j] = 255;
		}
	}

	return 0;
}
)";
	return oss.str();
}

string get_agg_init(EvalConfig config, int row_count, int chunk_count, int opid, int n_interventions, string fn, string alloc_code,
                    string get_data_code, string get_vals_code) {
	int n_masks = n_interventions / config.mask_size;
	string fname = "agg_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	if (config.use_duckdb) {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, __mmask16* __restrict__ var_0, std::unordered_map<std::string, std::vector<void*>>& alloc_vars, duckdb::ChunkCollection &chunk_collection) {
)";
	} else {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, __mmask16* var_0, std::unordered_map<std::string, std::vector<void*>>& alloc_vars, std::unordered_map<int, void*>& input_data_map) {
)";
	}


	oss << "\tconst int chunk_count = " << chunk_count << ";\n";
	oss << "\tconst int row_count = " << row_count << ";\n";
	oss << "\tconst int mask_size = " << config.mask_size << ";\n";
	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";
	oss << "\tconst int n_masks  = " << n_masks << ";\n";

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

	// oss << "\tstd::cout << \"Specs: \" << row_count << \" \" << mask_size << \" \" << n_interventions << \" \" << n_masks << \" \" << start << \" \" << end << std::endl;";

	oss << alloc_code;

	if (!config.is_scalar) {
		oss << "\t__m512i zeros_i = _mm512_setzero_si512();\n";
		oss << "\t__m512 zeros = _mm512_setzero_ps();\n";
	}

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
)";
	} else {
		oss << R"(
	for (int i=start; i < end; i++) {
		int oid = lineage[i];
		int col = oid*n_interventions;
)";
	}

	oss << get_vals_code;


	return oss.str();
}


string get_agg_finalize(EvalConfig config, FadeDataPerNode& node_data) {
	std::ostringstream oss;

	if (config.is_scalar) oss <<  "\n\t\t\t\t}\n"; // close inner loop
	if (config.use_duckdb) {
		oss << R"(
			}
		}
		offset +=  collection_chunk.size();
})";
	} else {
		oss << R"(
		}
})";
	}

	if (config.num_worker > 1) {
		oss << "\tsync_point.arrive_and_wait();\n";
    		oss << "\tconst int group_count = " << node_data.n_groups << ";\n";
		oss << R"(
	for (int jc = 0; jc < group_count; jc += 16) {
		if ((jc / 16) % num_threads == thread_id) {
)";
		for (auto &pair : node_data.alloc_vars) {
      oss << "{\n";
			int fid = node_data.alloc_vars_index[pair.first] ;
			string type_str = node_data.alloc_vars_types[pair.first];
			if (fid == -1) { // count
				oss <<  type_str +"* __restrict__ final_out = (" + type_str + "* __restrict__)alloc_vars[\"out_count\"][0];\n";
			} else {
				oss <<  type_str +"* __restrict__ final_out = (" + type_str + "* __restrict__)alloc_vars[\"out_"+to_string(fid)+"\"][0];\n";
			}
			oss << R"(
		    for (int j = jc; j < jc + 16 && j < group_count; ++j) {
		        for (int i = 1; i < num_threads; ++i) {
)";
			if (fid == -1) { // count
				oss <<  type_str +"* __restrict__ final_in = (" + type_str + "* __restrict__)alloc_vars[\"out_count\"][i];\n";
			} else {
				oss <<  type_str +"* __restrict__ final_in = (" + type_str + "* __restrict__)alloc_vars[\"out_"+to_string(fid)+"\"][i];\n";
			}
		  oss << R"(for (int k = 0; k < n_interventions; ++k) {
						int index = j * n_interventions + k;
)";
			oss << "final_out[index] += final_in[index];\n";
			oss << R"(
					}
				}
			}
    }
)";
		}
		oss << R"(
		}
	}
)";
	}
	oss << "\treturn 0;\n}\n";

	return oss.str();
}


string get_agg_alloc(int fid, string fn, string out_type) {
	std::ostringstream oss;
	if (fn == "sum") {
		oss << "\n";
		oss << out_type << "*__restrict__ out_" + to_string(fid) + " = (" + out_type +"*)alloc_vars[\"out_" + to_string(fid) << "\"][thread_id];\n";
	} else if (fn == "count") {
		oss << R"(
	int* __restrict__  out_count = (int*)alloc_vars["out_count"][thread_id];
)";
	}
	return oss.str();
}


string get_agg_simd_eval(string fn, string out_var, string in_var, string data_type) {
	std::ostringstream oss;
	oss << "{\n";
	if (data_type == "float") {
		oss << "\t__m512 a  = _mm512_load_ps((__m512*) &"+out_var+"[col + row]);\n";
		oss << "\t __m512 v = _mm512_set1_ps("+in_var+");\n";
		oss << "\t_mm512_store_ps((__m512*) &"+out_var+"[col + row], _mm512_mask_add_ps(a, tmp_mask, a, v));\n";
	} else if (data_type == "int") {
		oss << "\t__m512i a = _mm512_loadu_si512((__m512i*)&" + out_var + "[col+row]);\n";
		oss << "\t __m512i v = _mm512_mask_set1_epi32(zeros_i, tmp_mask, "+in_var+");\n";
		oss << "\t_mm512_storeu_si512((__m512i*) &"+out_var+"[col + row], _mm512_add_epi32(a, v));\n";
	}
	oss << "}\n";
	return oss.str();
}


string get_agg_eval_scalar(string fn, string out_var="", string in_var="") {
	std::ostringstream oss;
	if (fn == "sum") {
		oss << "\t\t\t\t\t";
		oss << out_var+"[col + (row + k) ] +="+ in_var + " * (1 &  tmp_mask >> k);\n";
	} else if (fn == "count") {
		oss << "\t\t\t\t\t";
		oss << "out_count[col + (row + k) ] += 1 * (1 &  tmp_mask >> k);\n";
	}
	return oss.str();
}

string get_agg_eval(EvalConfig config, int agg_count, string fn, string out_var="", string in_var="", string data_type="int") {
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

    if (config.use_duckdb) {
		  oss << "\t\t\t\t__mmask16 tmp_mask = var_0[(i+offset)*n_masks+j];\n";
    } else {
		  oss << "\t\t\t\t__mmask16 tmp_mask = var_0[i*n_masks+j];\n";
    }

		if (config.is_scalar) {
			oss << R"(
				for (int k=0; k < mask_size; k++) {
)";
		}

	}

	if (config.is_scalar) {
		oss << get_agg_eval_scalar(fn, out_var, in_var);
	} else {
		oss << get_agg_simd_eval(fn, out_var, in_var, data_type);
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


// TODO: push filter lineage
// TODO: perfect hash agg, agg
void GenRandomWhatifIntervention(EvalConfig config, PhysicalOperator* op,
                                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                 std::unordered_map<std::string, float> columns_spec,
                                 void* handle) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenRandomWhatifIntervention(config, op->children[i].get(), fade_data, columns_spec, handle);
	}

	// TODO: add option to push filter / pruned lineage
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (columns_spec.find(op->lineage_op->table_name) == columns_spec.end()) {
		//	return;
		}

		idx_t row_count = op->lineage_op->chunk_collection.Count();
		float probability = columns_spec[op->lineage_op->table_name];
		string fname = "fill_random";
		int (*random_fn)(int, float, int, __mmask16*) = (int(*)(int, float, int, __mmask16*))dlsym(handle, fname.c_str());
		random_fn(row_count, probability, fade_data[op->id].n_masks, fade_data[op->id].del_interventions);
	}
}


void FillFilterLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	vector<int>& lineage =  fade_data[op->id].lineage[0];
	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	idx_t offset = 0;

	idx_t row_count = lop->log_index->table_size;
	lineage.resize(row_count);

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
			lineage[i+offset] = iid;
		}
		offset = result.size();
	} while (cache_on || result.size() > 0);
}

void FillJoinLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	vector<int>& lhs_lineage =  fade_data[op->id].lineage[0];
	vector<int>& rhs_lineage =  fade_data[op->id].lineage[1];
	int lhs_masks = fade_data[op->children[0]->id].n_masks;
	int rhs_masks = fade_data[op->children[1]->id].n_masks;
	idx_t row_count = lop->log_index->table_size;
	if (lhs_masks > 0 && rhs_masks > 0) {
		lhs_lineage.resize(row_count);
		rhs_lineage.resize(row_count);
	} else if (lhs_masks > 0) {
		lhs_lineage.resize(row_count);
	} else {
		rhs_lineage.resize(row_count);
	}
	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	idx_t offset = 0;
	do {
		cache_on = false;
		result.Reset();
		result.Destroy();
		lop->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
		result.Flatten();
		if (result.size() == 0) continue;
		unsigned int * lhs_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
		unsigned int * rhs_index = reinterpret_cast<unsigned int *>(result.data[1].GetData());
		if (lhs_masks > 0 && rhs_masks > 0) {
			for (idx_t i=0; i < result.size(); ++i) {
				lhs_lineage[i+offset] = lhs_index[i];
				rhs_lineage[i+offset] = rhs_index[i];
			}
		} else if (lhs_masks > 0) {
			for (idx_t i=0; i < result.size(); ++i) {
				lhs_lineage[i+offset] = lhs_index[i];
			}
		} else {
			for (idx_t i=0; i < result.size(); ++i) {
				rhs_lineage[i+offset] = rhs_index[i];
			}
		}
		offset += result.size();
	} while (cache_on || result.size() > 0);
}

string JoinCodeAndAlloc(EvalConfig config, PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	int n_masks = config.n_intervention / config.mask_size;
	string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lhs_lineage, int* rhs_lineage,  __mmask16* __restrict__ lhs_var,  __mmask16* __restrict__ rhs_var,  __mmask16* __restrict__ out) {
)";
	int row_count = lop->log_index->table_size;
	oss << "\tconst int row_count = " + to_string(row_count) + ";\n";
	oss << "\tconst int n_masks = " + to_string(fade_data[op->id].n_masks) + ";\n";

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

	// oss << "std::cout << \"JOIN: \" << row_count << \" \" <<  n_masks << std::endl;";

	oss << "\tfor (int i=start; i < end; i++) {\n";

	if (config.is_scalar) {
		oss << R"(
		for (int j=0; j < n_masks; j++) {
)";

		if ( fade_data[op->children[0]->id].n_masks > 0 && fade_data[op->children[1]->id].n_masks > 0) {
			oss << R"(out[i*n_masks+j] = lhs_var[lhs_lineage[i]*n_masks+j] * rhs_var[rhs_lineage[i]*n_masks+j];)";
		} else if (fade_data[op->children[0]->id].n_masks > 0) {
			oss << R"(out[i*n_masks+j] = lhs_var[lhs_lineage[i]*n_masks+j];)";
		} else {
			oss << R"(out[i*n_masks+j] = rhs_var[rhs_lineage[i]*n_masks+j];)";
		}
	} else {
		oss << R"(
		for (int j=0; j < n_masks; j+=32) {
)";
		if ( fade_data[op->children[0]->id].n_masks > 0 && fade_data[op->children[1]->id].n_masks > 0) {
			oss << R"(
		__m512i a = _mm512_loadu_si512((__m512i*)&lhs_var[lhs_lineage[i]*n_masks+j]);
		__m512i b = _mm512_loadu_si512((__m512i*)&rhs_var[rhs_lineage[i]*n_masks+j]);
		_mm512_storeu_si512((__m512i*)&out[i*n_masks+j], _mm512_and_si512(a, b));
)";
		} else if (fade_data[op->children[0]->id].n_masks > 0) {
			oss << R"(
		__m512i a = _mm512_loadu_si512((__m512i*)&lhs_var[lhs_lineage[i]*n_masks+j]);
		_mm512_storeu_si512((__m512i*)&out[i*n_masks+j], a);
)";
		} else {
			oss << R"(
		__m512i b = _mm512_loadu_si512((__m512i*)&rhs_var[rhs_lineage[i]*n_masks+j]);
		_mm512_storeu_si512((__m512i*)&out[i*n_masks+j], b);
)";		}
	}


	if (config.num_worker > 1) {
		oss << "\n\t}\n\t\t}\n \tsync_point.arrive_and_wait();\n return 0; \n}\n";
	} else {
		oss << "\n\t}\n\t\t}\n return 0; \n}\n";
	}

	return oss.str();
}


string FilterCodeAndAlloc(EvalConfig config, PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	int n_masks = config.n_intervention / config.mask_size;
	string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
	    << fname
	    << R"((int thread_id, int* lineage,  __mmask16* __restrict__ var, __mmask16* __restrict__ out) {
)";
	int row_count = lop->log_index->table_size;
	oss << "\tconst int row_count = " + to_string(row_count) + ";\n";
	oss << "\tconst int n_masks = " + to_string(fade_data[op->id].n_masks) + ";\n";

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

	if (config.debug)
		oss << "std::cout << \"Filter: \" << row_count << \" \" <<  n_masks << std::endl;\n";

	if (config.is_scalar) {
		oss << R"(
	for (int i=start; i < end; ++i) {
		for (int j=0; j < n_masks; ++j) {
			out[i*n_masks+j] = var[lineage[i]*n_masks+j];
		}
	}
)";
	} else {
		oss << R"(
	for (int i=start; i < end; ++i) {
		for (int j=0; j < n_masks; j+=32) {
			__m512i b = _mm512_loadu_si512((__m512i*)&var[lineage[i]*n_masks+j]);
			_mm512_storeu_si512((__m512i*)&out[i*n_masks+j], b);
		}
	}
)";
	}


	if (config.num_worker > 1) {
		oss << "\tsync_point.arrive_and_wait();\n\t return 0; \n}\n";
	} else {
		oss << "\treturn 0; \n}\n";
	}

	return oss.str();
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


template<class T1, class T2>
T2* GetInputVals(PhysicalOperator* op, shared_ptr<OperatorLineage> lop, idx_t col_idx) {
	idx_t chunk_count = op->children[0]->lineage_op->chunk_collection.ChunkCount();
	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	T2* input_values = new T2[row_count];

	idx_t offset = 0;
	for (idx_t chunk_idx=0; chunk_idx < chunk_count; ++chunk_idx) {
		DataChunk &collection_chunk = op->children[0]->lineage_op->chunk_collection.GetChunk(chunk_idx);
		T1* col = reinterpret_cast<T1*>(collection_chunk.data[col_idx].GetData());
		int count = collection_chunk.size();
		Vector new_vec(LogicalType::FLOAT, count);
		if (collection_chunk.data[col_idx].GetType().id() == LogicalTypeId::DECIMAL) {
			CastParameters parameters;
			uint8_t width = DecimalType::GetWidth(collection_chunk.data[col_idx].GetType());
			uint8_t scale = DecimalType::GetScale(collection_chunk.data[col_idx].GetType());
			switch (collection_chunk.data[col_idx].GetType().InternalType()) {
			case PhysicalType::INT16: {
				VectorCastHelpers::TemplatedDecimalCast<int16_t, float, TryCastFromDecimal>(
				    collection_chunk.data[col_idx], new_vec, count, parameters.error_message, width, scale);
				break;
			} case PhysicalType::INT32: {
				VectorCastHelpers::TemplatedDecimalCast<int32_t, float, TryCastFromDecimal>(
				    collection_chunk.data[col_idx], new_vec, count, parameters.error_message, width, scale);
				break;
			} case PhysicalType::INT64: {
				VectorCastHelpers::TemplatedDecimalCast<int64_t, float, TryCastFromDecimal>(
				    collection_chunk.data[col_idx], new_vec, count, parameters.error_message, width, scale);
				break;
			} case PhysicalType::INT128: {
				VectorCastHelpers::TemplatedDecimalCast<hugeint_t, float, TryCastFromDecimal>(
				    collection_chunk.data[col_idx], new_vec, count, parameters.error_message, width, scale);
				break;
			} default: {
				throw InternalException("Unimplemented internal type for decimal");
			}
			}
			col = reinterpret_cast<T1*>(new_vec.GetData());
		}
		for (idx_t i=0; i < collection_chunk.size(); ++i) {
			input_values[i+offset] = col[i]; // collection_chunk.data[col_idx].GetValue(i).GetValue<T2>();
		}
		offset +=  collection_chunk.size();
	}

	return input_values;
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

	fade_data[op->id].n_groups = n_groups;

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	fade_data[op->id].lineage[0] = std::move(GetGBLineage(lop, row_count));
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

			// TODO: check data type
			string input_type = "float";
			string output_type = "float";

			if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::INTEGER) {
				input_type = "int";
				output_type = "int";
			} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::FLOAT) {
				input_type = "float";
				output_type = "float";
			} else {
				input_type = "double";
				output_type = "float";
			}

			string out_var = "out_" + to_string(i); // new output
			string in_arr = "col_" + to_string(i);  // input arrays
			string in_val = "val_" + to_string(i);  // input values val = col_x[i]

			if (config.use_duckdb) {
				// use CollectionChunk that stores input data
				get_data_code += "\t\t"+input_type+"* " + in_arr + " = reinterpret_cast<"+input_type+" *>(collection_chunk.data[" +
				                 to_string(col_idx) + "].GetData());\n";
			} else {
				// use unordered_map<int, void*> that stores pointers to input data
				get_data_code += "\t\t"+input_type+"* " + in_arr + " = reinterpret_cast<"+input_type+" *>(input_data_map[" +
				                 to_string(i) + "]);\n";
				if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::INTEGER) {
					fade_data[op->id].input_data_map[i] = GetInputVals<int, int>(op, op->lineage_op, col_idx);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::FLOAT) {
					fade_data[op->id].input_data_map[i] = GetInputVals<float, float>(op, op->lineage_op, col_idx);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::DOUBLE) {
					fade_data[op->id].input_data_map[i] = GetInputVals<double, float>(op, op->lineage_op, col_idx);
				} else {
					fade_data[op->id].input_data_map[i] = GetInputVals<float, float>(op, op->lineage_op, col_idx);
				}

			}

			fade_data[op->id].alloc_vars[out_var].resize(config.num_worker);
			for (int t=0; t < config.num_worker; ++t) {
				if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::INTEGER) {
					fade_data[op->id].alloc_vars[out_var][t] = aligned_alloc(64, sizeof(int) * n_groups * n_interventions);
					fade_data[op->id].alloc_vars_types[out_var] = "int";
					memset(fade_data[op->id].alloc_vars[out_var][t], 0, sizeof(int) * n_groups * n_interventions);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::FLOAT) {
					fade_data[op->id].alloc_vars_types[out_var] = "float";
					fade_data[op->id].alloc_vars[out_var][t] = aligned_alloc(64, sizeof(float) * n_groups * n_interventions);
					memset(fade_data[op->id].alloc_vars[out_var][t], 0, sizeof(float) * n_groups * n_interventions);
				} else {
					fade_data[op->id].alloc_vars_types[out_var] = "float";
					fade_data[op->id].alloc_vars[out_var][t] = aligned_alloc(64, sizeof(float) * n_groups * n_interventions);
					memset(fade_data[op->id].alloc_vars[out_var][t], 0, sizeof(float) * n_groups * n_interventions);
				}
			}


			get_vals_code += "\t\t\t" + output_type +" " + in_val + "= " + in_arr + "[i];\n";

			// access output arrays
			alloc_code += get_agg_alloc(i, "sum", output_type);
			// core agg operation
			eval_code += get_agg_eval(config, agg_count++, "sum", out_var, in_val, output_type);
			fade_data[op->id].alloc_vars_index[out_var] = i;
		}
	}

	if (include_count == true) {
		string out_var = "out_count";
		alloc_code += get_agg_alloc(0, "count", "int");
		eval_code += get_agg_eval(config, agg_count++, "count", out_var, "1", "int");
		fade_data[op->id].alloc_vars[out_var].resize(config.num_worker);
		fade_data[op->id].alloc_vars_types[out_var] = "int";
		for (int t=0; t < config.num_worker; ++t) {
			fade_data[op->id].alloc_vars[out_var][t] = aligned_alloc(64, sizeof(int) * n_groups * n_interventions);
			memset(fade_data[op->id].alloc_vars[out_var][t], 0, sizeof(int) * n_groups * n_interventions);
		}
		fade_data[op->id].alloc_vars_index[out_var] = -1;
	}


	string init_code = get_agg_init(config, row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(), op->id,  fade_data[op->id].n_interventions, "agg", alloc_code, get_data_code, get_vals_code);
	string end_code = get_agg_finalize(config, fade_data[op->id]);

	code = init_code + eval_code + end_code;

	return code;
}

void  HashAggregateIntervene2DEval(int thread_id, EvalConfig config, shared_ptr<OperatorLineage> lop,
                                  std::unordered_map<idx_t, FadeDataPerNode> fade_data,
                                  PhysicalOperator* op, void* handle, __mmask16* var_0) {
	PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
	vector<pair<idx_t, idx_t>> aggregate_input_idx;
	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	std::vector<int> lineage = fade_data[op->id].lineage[0];
	if (config.use_duckdb) {
		int result = fade_data[op->id].agg_duckdb_fn(thread_id, lineage.data(), var_0, fade_data[op->id].alloc_vars, op->children[0]->lineage_op->chunk_collection);
	} else {
		int result = fade_data[op->id].agg_fn(thread_id, lineage.data(), var_0, fade_data[op->id].alloc_vars, fade_data[op->id].input_data_map);
	}
}

void GenCodeAndAlloc(EvalConfig config, string& code, PhysicalOperator* op,
                    std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                    std::unordered_map<std::string, float> columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenCodeAndAlloc(config, code, op->children[i].get(), fade_data, columns_spec);
	}


	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (columns_spec.find(op->lineage_op->table_name) == columns_spec.end()) {
			//return;
		}

		idx_t row_count = op->lineage_op->chunk_collection.Count();
		idx_t n_masks = std::ceil(config.n_intervention / config.mask_size);
		// allocate deletion intervention: n_intervention X row_count
		fade_data[op->id].del_interventions =  new __mmask16[row_count * n_masks];
		fade_data[op->id].n_interventions = config.n_intervention;
		fade_data[op->id].n_masks = n_masks;
	} else if (op->type == PhysicalOperatorType::FILTER) {
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		idx_t row_count = op->lineage_op->log_index->table_size;
		idx_t n_masks = fade_data[op->id].n_masks;
		if (n_masks > 0) {
			fade_data[op->id].del_interventions = new __mmask16[row_count * n_masks];
			FillFilterLineage(op, op->lineage_op, fade_data);
			code += FilterCodeAndAlloc(config, op, op->lineage_op, fade_data);
		}
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
		if (n_masks > 0) {
			fade_data[op->id].del_interventions = new __mmask16[row_count * n_masks];
			FillJoinLineage(op, op->lineage_op, fade_data);
			code += JoinCodeAndAlloc(config, op, op->lineage_op, fade_data);
		}
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

	if (config.debug) {
		std::cout << "GenCodeAndAlloc(" << op->id << ") -> n_masks:"
		          << fade_data[op->id].n_masks << ", n_interventions: "
		          << fade_data[op->id].n_interventions << std::endl;
	}
}


void BindFunctions(EvalConfig config, void* handle, PhysicalOperator* op,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                        std::unordered_map<std::string, float> columns_spec) {

	for (idx_t i = 0; i < op->children.size(); i++) {
		BindFunctions(config, handle, op->children[i].get(), fade_data, columns_spec);
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		if (fade_data[op->id].n_masks > 0) {
			string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
			fade_data[op->id].filter_fn = (int(*)(int, int*, __mmask16*, __mmask16*))dlsym(handle, fname.c_str());
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		if (fade_data[op->id].n_masks > 0) {
			string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
			fade_data[op->id].join_fn = (int(*)(int, int*, int*, __mmask16*, __mmask16*, __mmask16*))dlsym(handle, fname.c_str());
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		string fname = "agg_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		if (config.use_duckdb) {
			fade_data[op->id].agg_duckdb_fn = (int(*)(int, int*, __mmask16*, std::unordered_map<std::string, vector<void*>>&, ChunkCollection&))dlsym(handle, fname.c_str());
		} else {
			fade_data[op->id].agg_fn = (int(*)(int, int*, __mmask16*, std::unordered_map<std::string, vector<void*>>&,  std::unordered_map<int, void*>&))dlsym(handle, fname.c_str());
		}
	}
}


void Intervention2DEval(int thread_id, EvalConfig config, void* handle, PhysicalOperator* op,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                        std::unordered_map<std::string, float> columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		Intervention2DEval(thread_id, config, handle, op->children[i].get(), fade_data, columns_spec);
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		if (fade_data[op->id].n_masks > 0) {
			idx_t row_count = op->lineage_op->log_index->table_size;
			int result = fade_data[op->id].filter_fn(thread_id, fade_data[op->id].lineage[0].data(),
			                                       fade_data[op->children[0]->id].del_interventions,
			                                       fade_data[op->id].del_interventions);
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		if (fade_data[op->id].n_masks > 0) {
			idx_t row_count = op->lineage_op->log_index->table_size;
			int result = fade_data[op->id].join_fn(thread_id, fade_data[op->id].lineage[0].data(),
			                fade_data[op->id].lineage[1].data(), fade_data[op->children[0]->id].del_interventions,
			                fade_data[op->children[1]->id].del_interventions, fade_data[op->id].del_interventions);
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		__mmask16* var_0 = fade_data[op->id].del_interventions;
		HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op, handle, var_0);
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	    UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
	} else if (op->type == PhysicalOperatorType::PROJECTION) {
	}
}

template <class T>
void PrintOutput(FadeDataPerNode& info, T* data_ptr) {
	for (int i=0; i < info.n_groups; i++) {
		for (int j=0; j < info.n_interventions; j++) {
			int index = i * info.n_interventions + j;
			std::cout << index << " G: " << i << " I: " << j << " -> " <<  data_ptr[index] << std::endl;
			break;
		}
	}
}

void ReleaseFade(EvalConfig config, void* handle, PhysicalOperator* op,
                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                 std::unordered_map<std::string, float> columns_spec) {

	if (op->type != PhysicalOperatorType::PROJECTION) {
		if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
			for (auto &pair : fade_data[op->id].alloc_vars) {
				if (!pair.second.empty()) {
					if (config.debug) {
						if (fade_data[op->id].alloc_vars_types[pair.first] == "int") {
							PrintOutput<int>(fade_data[op->id], (int*)pair.second[0]);
						} else if (fade_data[op->id].alloc_vars_types[pair.first] == "float") {
							PrintOutput<float>(fade_data[op->id], (float*)pair.second[0]);
						}
					}
					for (int t = 0; t < pair.second.size(); t++) {
						free(pair.second[t]);
						pair.second[t] = nullptr;
					}

				}
			}
			for (auto &pair : fade_data[op->id].input_data_map) {
				if (pair.second != nullptr) {
					free(pair.second);
					pair.second = nullptr;
				}
			}
		} else {
			if (fade_data[op->id].del_interventions != nullptr) {
				free(fade_data[op->id].del_interventions);
				fade_data[op->id].del_interventions = nullptr;
			}
		}
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		ReleaseFade(config, handle, op->children[i].get(), fade_data, columns_spec);
	}

}

string Fade::Whatif(PhysicalOperator *op, EvalConfig config) {
	// timing vars
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;

	// 1. Parse Spec
	// e.g. random delete table_name:prob, scale by factor: table_name.col:scale
	std::unordered_map<std::string, float>  columns_spec = parseWhatifString(config);

	// holds any extra data needed during exec
	std::unordered_map<idx_t, FadeDataPerNode> fade_data;

	// 2. Post Process
	start_time = std::chrono::steady_clock::now();
	LineageManager::PostProcess(op);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double post_processing_time = time_span.count();

	// 4. Alloc vars, generate eval code
	string code;
	start_time = std::chrono::steady_clock::now();
	GenCodeAndAlloc(config, code, op, fade_data, columns_spec);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double prep_time = time_span.count();

	std::ostringstream oss;
	oss << get_header(config) << "\n" << fill_random_code(config) << "\n"  << code;
	string final_code = oss.str();

	if (config.debug)
		std::cout << final_code << std::endl;

	start_time = std::chrono::steady_clock::now();
	void* handle = compile(final_code, 0);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double compile_time = time_span.count();

	if (handle == nullptr) return "select 0";


	// 3. Prepare base interventions; should be one time cost per DB
	start_time = std::chrono::steady_clock::now();
	GenRandomWhatifIntervention(config, op, fade_data, columns_spec, handle);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double intervention_gen_time = time_span.count();

	BindFunctions(config, handle,  op, fade_data, columns_spec);

	std::vector<std::thread> workers;

	start_time = std::chrono::steady_clock::now();

	for (int i = 0; i < config.num_worker; ++i) {
		workers.emplace_back(Intervention2DEval, i, config, handle,  op, std::ref(fade_data), columns_spec);
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

	ReleaseFade(config, handle, op, fade_data, columns_spec);

	return "select " + to_string(post_processing_time) + " as post_processing_time, " + to_string(intervention_gen_time)
	       + " as intervention_gen_time, " + to_string(prep_time)
	       + " as prep_time, " + to_string(compile_time) + " as compile_time, " + to_string(eval_time) + " as eval_time";
}


} // namespace duckdb
#endif

