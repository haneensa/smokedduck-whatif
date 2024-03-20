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

string fill_random_code(EvalConfig& config) {
	std::ostringstream oss;
	oss << R"(
extern "C" int fill_random(int row_count, float prob, int n_masks, void* del_interventions_ptr, std::set<int>& del_set, std::set<int>& out_del_set) {
	std::cout << "fill_random: " << row_count  << " " << prob << " " <<  n_masks << std::endl;
	// Initialize a random number generator
  int64_t count = 0;
)";

	if (config.n_intervention == 1) {
		oss << "\t int8_t* __restrict__ del_interventions = (int8_t* __restrict__)del_interventions_ptr;\n";
	} else if (config.n_intervention == 1 && config.incremental == false) {
	} else {
		oss << "\t __mmask16* __restrict__ del_interventions = (__mmask16* __restrict__)del_interventions_ptr;\n";
	}

  // HACK: generate one intervention randomly, then reuse it
	if (config.n_intervention > 1) {
		oss << R"(
    std::random_device rd;
    std::mt19937 gen(rd());
    // Create a uniform distribution over the range [0, row_count]
    std::uniform_int_distribution<int> dist(0, row_count);

    std::vector<__mmask16> base(row_count);
    for (int i = 0; i < row_count; ++i) {
      base[i] = 0xFFFF;
      for (int k = 0; k < 16; ++k) {
		    if ((((double)rand() / RAND_MAX) < prob)) {
			      base[i] &= ~(1 << k);
            count++;
        }
      }
    }
)";
  }
	oss << "\nfor (int i = 0; i < row_count; ++i) {\n";
	if (config.n_intervention == 1 && config.incremental == true) {
		oss << "\n\tif ((((double)rand() / RAND_MAX) < prob))";
		oss << "\n\t\tdel_set.insert(i);";
	} else if (config.n_intervention == 1 && config.incremental == false) {
		oss << "\n\tdel_interventions[i] = !(((double)rand() / RAND_MAX) < prob);"; // TODO: use random
	} else {
		oss << R"(
		for (int j = 0; j < n_masks; ++j) {
      int r = dist(gen);
			del_interventions[i*n_masks+j] = base[r];
		}
)";
	}


	oss << R"(
	}

  std::cout << "done"<< std::endl;
  	std::cout << " random done " << del_set.size() << " " << count << " " << count /( 16) <<std::endl;
	return 0;
}
)";
	return oss.str();
}

// ADD get_agg_init for SCALE_UNIFORM which doesn't use any interventions below aggregates

string get_agg_init_no_intervention(EvalConfig& config, int total_agg_count, int row_count, int chunk_count, int opid, int n_interventions, string fn, string alloc_code,
                    string get_data_code, string get_vals_code) {
	int n_masks = n_interventions / config.mask_size;
	string fname = "agg_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	if (config.use_duckdb) {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
duckdb::ChunkCollection &chunk_collection, std::set<int>& del_set) {
)";
	} else {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
              std::unordered_map<int, void*>& input_data_map, std::set<int>& del_set) {
)";
	}


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

	// oss << "\tstd::cout << \"Specs: \" << row_count << \" \" << mask_size << \" \" << n_interventions << \" \" << n_masks << \" \" << start << \" \" << end << std::endl;";

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
)";
		} else {
			oss << R"(
	for (int i=start; i < end; ++i) {
		int oid = lineage[i];
		int col = oid*n_interventions;
)";
		}

		if (config.n_intervention > 1)
			oss << get_vals_code;


	return oss.str();
}

string get_agg_init(EvalConfig& config, int total_agg_count, int row_count, int chunk_count, int opid, int n_interventions, string fn, string alloc_code,
                    string get_data_code, string get_vals_code) {
	int n_masks = n_interventions / config.mask_size;
	string fname = "agg_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	if (config.use_duckdb) {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
duckdb::ChunkCollection &chunk_collection, std::set<int>& del_set) {
)";
	} else {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
              std::unordered_map<int, void*>& input_data_map, std::set<int>& del_set) {
)";
	}


	if (config.n_intervention == 1) {
		oss << "\t int8_t* __restrict__ var_0 = (int8_t* __restrict__)var_0_ptr;\n";
	} else {
		oss << "\t __mmask16* __restrict__ var_0 = (__mmask16* __restrict__)var_0_ptr;\n";
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

	if (config.incremental && config.n_intervention == 1 && config.use_duckdb == false) {
		// convert intervention array to annotation array where var[i]==1
		oss << get_data_code;
		oss << R"(
		//		for (int i : del_set) {
		    std::vector<int> zeros(del_set.size());
        int c = 0;
        for (auto it = del_set.begin(); it != del_set.end(); ++it) {
            zeros[c++] = *it;
        }
        for (int i=0; i < zeros.size(); ++i) {
          int oid = lineage[i];
          int iid = i;
)";
	} else {

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
)";
			if (config.n_intervention == 1 && ( config.intervention_type != InterventionType::SCALE_UNIFORM
			                                   &&  config.intervention_type != InterventionType::SCALE_RANDOM)) {
				if (total_agg_count > 1) {
					oss << "\t\t\t\t\tif (var_0[i+offset] == 0) continue;\n";
				}
			} else {
				oss << "\t\t\tint col = oid*n_interventions;\n";
			}
		} else {
			oss << R"(
	for (int i=start; i < end; ++i) {
		int oid = lineage[i];
)";
			if (config.n_intervention == 1 && ( config.intervention_type != InterventionType::SCALE_UNIFORM
			                                   &&  config.intervention_type != InterventionType::SCALE_RANDOM)) {
				if (total_agg_count > 1) {
					oss << "\t\t\t\t\tif (var_0[i] == 0) continue;\n";
				}
			} else {
				oss << "\t\t\tint col = oid*n_interventions;\n";
			}
		}

		if (config.n_intervention > 1)
			oss << get_vals_code;
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
		oss << "\t__m512i a = _mm512_load_si512((__m512i*)&" + out_var + "[col+row]);\n";
		// TODO: fix zeros_i is overwritten here
		oss << "\t __m512i v = _mm512_mask_set1_epi32(zeros_i, tmp_mask, "+in_var+");\n";
		oss << "\t_mm512_store_si512((__m512i*) &"+out_var+"[col + row], _mm512_add_epi32(a, v));\n";
	}
	oss << "}\n";
	return oss.str();
}


string get_agg_eval_scalar(string fn, string out_var="", string in_var="") {
	std::ostringstream oss;
	if (fn == "sum") {
		oss << "\t\t\t\t\t";
		oss << out_var+"[col + (row + k) ] +="+ in_var;
		oss << " * (1 &  tmp_mask >> k);\n";
	} else if (fn == "count") {
		oss << "\t\t\t\t\t";
		oss << "out_count[col + (row + k) ] += (1 &  tmp_mask >> k)";
	}

	return oss.str();
}

string get_single_agg_template(EvalConfig& config, int total_agg_count, int agg_count,
                               string fn, string out_var="", string in_var="",
                               string in_arr="", string data_type="int") {
	std::ostringstream oss;

	// if incremental, then instead of iterating over the whole input data
	// use_duckdb == False because I need to access elements directly. if I use duckdb, then first i need to locate the chunk, then access the element
	// we iterate over the interventions, and each intervention stores the id of the tuple to be deleted

	if (agg_count > 0 && agg_count % config.batch == 0) {
		if (config.incremental && config.use_duckdb == false) {
			oss << R"(
				}
		//		for (int i : del_set) {
        //for (auto it = del_set.begin(); it != del_set.end(); ++it) {
        for (int i=0; i < zeros.size(); ++i) {
					int oid = lineage[i];
					int iid = i;
)";
		} else if (config.use_duckdb) {
			oss << R"(
				}
				for (int i=0; i < collection_chunk.size(); ++i) {
				int oid = lineage[i+offset];
)";
			if (total_agg_count > 1 && ( config.intervention_type != InterventionType::SCALE_UNIFORM
			                            &&  config.intervention_type != InterventionType::SCALE_RANDOM)) {
				oss << "\t\t\t\t\tif (var_0[i+offset] == 0) continue;\n";
			}
		} else {
			oss << R"(
				}
				for (int i=start; i < end; ++i) {
					int oid = lineage[i];
)";
			if (total_agg_count > 1 && ( config.intervention_type != InterventionType::SCALE_UNIFORM
			                            &&  config.intervention_type != InterventionType::SCALE_RANDOM)) {
				oss << "\t\t\t\t\tif (var_0[i] == 0) continue;\n";
			}
		}

	}

	if (config.incremental && config.use_duckdb == false) {
		// out_var[oid] += in_var[i];
		if (fn == "sum") {
			oss << "\t\t\t\t\t";
			oss << out_var+"[oid] +="+ in_arr + "[iid];\n";
		} else if (fn == "count") {
			oss << "\t\t\t\t\t";
			oss << "out_count[oid] += 1;\n";
		}
	} else if (fn == "count" && (config.intervention_type == InterventionType::SCALE_RANDOM ||
	                             config.intervention_type == InterventionType::SCALE_UNIFORM)) {
	} else {
		if (fn == "sum") {
			oss << "\t\t\t\t\t";
			oss << out_var+"[oid] +="+ in_arr + "[i]";
		} else if (fn == "count") {
			oss << "\t\t\t\t\t";
			oss << "out_count[oid] += 1";
		}

		// if there are more than one multiples, then iterate over multiples[] array
		if (config.intervention_type == InterventionType::SCALE_RANDOM) {
			if (config.use_duckdb) {
				oss << " * (0.8 * var_0[i+offset] + 1);\n";
			} else {
				oss << " * (0.8 * var_0[i] + 1);\n";
			}
		} else if (config.intervention_type == InterventionType::SCALE_UNIFORM) {
			oss << "* 1.8;\n";
		} else if (total_agg_count > 1 || config.intervention_type == InterventionType::SCALE_UNIFORM) {
				oss << ";\n";
		} else {
			if (config.use_duckdb) {
				oss << " * var_0[i+offset];\n";
			} else {
				oss << " * var_0[i];\n";
			}
		}
	}


	return oss.str();
}

string get_batch_agg_template(EvalConfig& config, int agg_count, string fn, string out_var="", string in_var="", string data_type="int") {
	std::ostringstream oss;

	if (agg_count % config.batch == 0) {
		if (agg_count > 0) {
			if (config.is_scalar) {
				oss << "\n\t\t}\n"; // close for (int k=0; k < mask_size; k++)
			}
			oss << "\n\t\t\t}\n"; // close for (int j=0; j < n_masks; ++j)
		}

		oss << R"(
			for (int j=0; j < n_masks; ++j) {
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
		if (config.intervention_type == InterventionType::SCALE_RANDOM) {
			string var = "(0.8 * (1 &  tmp_mask >> k) + 1)";
			if (fn == "sum") {
				oss << "\t\t\t\t\t";
				oss << out_var+"[col + (row + k) ] +="+ in_var;
				oss << " * " << var << ";\n";
			}
		} else {
			oss << get_agg_eval_scalar(fn, out_var, in_var);
		}
	} else {
		if (config.intervention_type == InterventionType::SCALE_RANDOM) {
			oss << "{\n";
			if (data_type == "float") {
				oss << "\t__m512 a  = _mm512_load_ps((__m512*) &"+out_var+"[col + row]);\n";
				oss << "\t__m512 X = _mm512_set1_ps("+in_var+" * (0.8+1));\n";
				oss << "\t__m512 Y = _mm512_set1_ps("+in_var+");\n";
				oss << "\t__m512 v = _mm512_mask_blend_ps(tmp_mask, X, Y);\n";
				oss << "\t_mm512_store_ps((__m512*) &"+out_var+"[col + row], _mm512_add_ps(a, v));\n";
			} else if (data_type == "int") {
				oss << "\t__m512i a = _mm512_load_si512((__m512i*)&" + out_var + "[col+row]);\n";
				oss << "\t__m512i X = _mm512_set1_epi32("+in_var+" * (0.8+1));\n";
				oss << "\t__m512i Y = _mm512_set1_epi32("+in_var+");\n";
				oss << "\t__m512i v = _mm512_mask_blend_epi32(tmp_mask, X, Y);\n";
				oss << "\t_mm512_store_si512((__m512i*) &"+out_var+"[col + row], _mm512_add_epi32(a, v));\n";
			}
			oss << "}\n";
		} else {
			oss << get_agg_simd_eval(fn, out_var, in_var, data_type);
		}
	}

	return oss.str();
}


string get_agg_eval(EvalConfig& config, int total_agg_count, int agg_count, string fn, string out_var="", string in_var="", string in_arr="", string data_type="int") {
	std::ostringstream oss;

	if (config.n_intervention == 1) {
		oss << get_single_agg_template(config, total_agg_count, agg_count, fn, out_var, in_var, in_arr, data_type);
	} else if (config.intervention_type == InterventionType::SCALE_UNIFORM) {
		// TODO: how to include multiple
		// * muliples[row];
		// if not uniform: * (multiples[row] * scale_var_0[row] + 1);
		// delete intervention + scale intervention: // add another inner loop to iterate over scaling intervention
		// * muliples[row] * var_0[row];
		// 	if not uniform: * (multiples[row] * scale_var_0[row] + 1) * var_0[row];
		std::cout << "NOT SUPPORTED YET" << std::endl;
	} else {
		oss << get_batch_agg_template(config, agg_count, fn, out_var, in_var, data_type);
	}

	return oss.str();
}

// TODO: perfect hash agg, agg
void GenRandomWhatifIntervention(EvalConfig& config, PhysicalOperator* op,
                                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                 void* handle,
                                 std::unordered_map<std::string, std::vector<std::string>>& spec) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		GenRandomWhatifIntervention(config, op->children[i].get(), fade_data, handle, spec);
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (spec.find(op->lineage_op->table_name) == spec.end() && config.intervention_type == InterventionType::DENSE_DELETE_SPEC
		    || config.intervention_type == InterventionType::SCALE_UNIFORM) {
			return;
		}

		idx_t row_count = op->lineage_op->log_index->table_size;
		if (config.prune)
			  row_count = fade_data[op->id].lineage[0].size();

		string fname = "fill_random";
    std::cout << op->lineage_op->table_name << " " << row_count << std::endl;
		int (*random_fn)(int, float, int, void*, std::set<int>&) = (int(*)(int, float, int, void*, std::set<int>&))dlsym(handle, fname.c_str());
		if (config.n_intervention == 1) {
			random_fn(row_count, config.probability, fade_data[op->id].n_masks, fade_data[op->id].single_del_interventions, fade_data[op->id].del_set);
		} else {
			random_fn(row_count, config.probability, fade_data[op->id].n_masks, fade_data[op->id].del_interventions, fade_data[op->id].del_set);
		}
	}
}

string get_batch_join_template(EvalConfig &config, PhysicalOperator *op,
                          std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	std::ostringstream oss;
	if (config.is_scalar) {
		oss << R"(
      int lhs_col = lhs_lineage[i] * n_masks;
      int rhs_col = rhs_lineage[i] * n_masks;
      int col = i*n_masks;
      for (int j=0; j < n_masks; ++j) {
)";
		if ( fade_data[op->children[0]->id].n_masks > 0 && fade_data[op->children[1]->id].n_masks > 0) {
			oss << R"(out[col+j] = lhs_var[lhs_col+j] & rhs_var[rhs_col+j];)";
		} else if (fade_data[op->children[0]->id].n_masks > 0) {
			oss << R"(out[col+j] = lhs_var[lhs_col+j];)";
		} else {
			oss << R"(out[col+j] = rhs_var[rhs_col+j];)";
		}

		oss << "\n\t\t\t}";
	} else {
		oss << R"(
    int lhs_col = lhs_lineage[i] * n_masks;
    int rhs_col = rhs_lineage[i] * n_masks;
    int col = i*n_masks;
		for (int j=0; j < n_masks; j+=32) {
)";
		if ( fade_data[op->children[0]->id].n_masks > 0 && fade_data[op->children[1]->id].n_masks > 0) {
			oss << R"(
		__m512i a = _mm512_stream_load_si512((__m512i*)&lhs_var[lhs_col+j]);
		__m512i b = _mm512_stream_load_si512((__m512i*)&rhs_var[rhs_col+j]);
		_mm512_store_si512((__m512i*)&out[col+j], _mm512_and_si512(a, b));
)";
		} else if (fade_data[op->children[0]->id].n_masks > 0) {
			oss << R"(
		__m512i a = _mm512_load_si512((__m512i*)&lhs_var[lhs_col+j]);
		_mm512_store_si512((__m512i*)&out[col+j], a);
)";
		} else {
			oss << R"(
		__m512i b = _mm512_load_si512((__m512i*)&rhs_var[rhs_col+j]);
		_mm512_store_si512((__m512i*)&out[col+j], b);
)";
		}

		oss << "\n\t\t\t}";
	}
	return oss.str();
}

string get_single_join_template(EvalConfig &config, PhysicalOperator *op,
                          std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	std::ostringstream oss;

	// if incremental, iterate over each side, and add it to the del_out_set
	if ( fade_data[op->children[0]->id].n_interventions > 0 && fade_data[op->children[1]->id].n_interventions > 0) {
		oss << R"(out[i] = lhs_var[lhs_lineage[i]] * rhs_var[rhs_lineage[i]];)";
	} else if (fade_data[op->children[0]->id].n_interventions > 0) {
		oss << R"(out[i] = lhs_var[lhs_lineage[i]];)";
	} else {
		oss << R"(out[i] = rhs_var[rhs_lineage[i]];)";
	}

	return oss.str();
}
string JoinCodeAndAlloc(EvalConfig& config, PhysicalOperator *op, shared_ptr<OperatorLineage> lop,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	int n_masks = config.n_intervention / config.mask_size;
	string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* __restrict__ lhs_lineage, int* __restrict__  rhs_lineage,
				void* __restrict__ lhs_var_ptr,  void* __restrict__ rhs_var_ptr,  void* __restrict__ out_ptr,
				std::set<int>& lhs_del_set,
				std::set<int>& rhs_del_set,
				std::set<int>& out_del_set) {
)";

	// 	output_annotation.insert(side_forward_lineage[input id])

	int row_count = fade_data[op->id].lineage[0].size();
	oss << "\tconst int row_count = " + to_string(row_count) + ";\n";
	if (config.n_intervention == 1 && config.incremental == true) {
		oss << R"(
		for (int iid : rhs_del_set) {
      if (rhs_lineage[iid] >= 0) {
       //  std::cout << row_count << " rhs: " << rhs_lineage[iid] << " " << iid << std::endl;
			  out_del_set.insert(rhs_lineage[iid]);
      }
		}
		for (int iid : lhs_del_set) {
      //  if (row_count < iid)
        //  std::cout << row_count << " lhs: "  <<  iid << std::endl;
      if (lhs_lineage[iid] >= 0) {
     //  std::cout << row_count << " lhs: "  << lhs_lineage[iid] << " " << iid << std::endl;
			  out_del_set.insert(lhs_lineage[iid]);
			}
		}
    return 0;
}
)";
		return oss.str();
	}

	if (config.n_intervention == 1) {
		oss << "\t int8_t* __restrict__ out = (int8_t* __restrict__)out_ptr;\n";
		oss << "\t int8_t* __restrict__ rhs_var = (int8_t* __restrict__)rhs_var_ptr;\n";
		oss << "\t int8_t* __restrict__ lhs_var = (int8_t* __restrict__)lhs_var_ptr;\n";
	} else {
		oss << "\t __mmask16* __restrict__ out = (__mmask16* __restrict__)out_ptr;\n";
		oss << "\t __mmask16* __restrict__ rhs_var = (__mmask16* __restrict__)rhs_var_ptr;\n";
		oss << "\t __mmask16* __restrict__ lhs_var = (__mmask16* __restrict__)lhs_var_ptr;\n";
	}

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

	oss << "\tfor (int i=start; i < end; ++i) {\n";

	if (config.n_intervention == 1) {
		oss << get_single_join_template(config, op, fade_data);
	} else {
		oss << get_batch_join_template(config, op, fade_data);
	}

	if (config.num_worker > 1) {
		oss << "\n\t\t}\n \tsync_point.arrive_and_wait();\n return 0; \n}\n";
	} else {
		oss << "\n\t\t}\n return 0; \n}\n";
	}

	return oss.str();
}


string FilterCodeAndAlloc(EvalConfig& config, PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	int n_masks = config.n_intervention / config.mask_size;
	string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
	    << fname
	    << R"((int thread_id, int* __restrict__ lineage,  void* __restrict__ var_ptr, void* __restrict__ out_ptr,
        std::set<int>& del_set, std::set<int>& out_del_set) {
)";

	if (config.n_intervention == 1) {
		oss << "\t int8_t* __restrict__ out = (int8_t* __restrict__)out_ptr;\n";
		oss << "\t int8_t* __restrict__ var = (int8_t* __restrict__)var_ptr;\n";
	} else {
		oss << "\t __mmask16* __restrict__ out = (__mmask16* __restrict__)out_ptr;\n";
		oss << "\t __mmask16* __restrict__ var = (__mmask16* __restrict__)var_ptr;\n";
	}


	int row_count = fade_data[op->id].lineage[0].size();

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

//	if (config.debug)
//		oss << "std::cout << \"Filter: \" << row_count << \" \" <<  n_masks << std::endl;\n";

	if (config.n_intervention == 1 && config.incremental == true) {
		oss << R"(
		for (int iid : del_set) {
      int oid = lineage[iid];
      if (lineage[iid] >= 0) {
			  out_del_set.insert(oid); // forward_lineage[iid] -> oid
			}
		}
)";
	} else if (config.n_intervention == 1 && config.incremental == false) {
		oss << R"(
	for (int i=start; i < end; ++i) {
		out[i] = var[lineage[i]];
	}
)";
	} else if (config.is_scalar) {
		oss << R"(
	for (int i=start; i < end; ++i) {
    int col_out = i*n_masks;
    int col_oid = lineage[i]*n_masks;
		for (int j=0; j < n_masks; ++j) {
			out[col_out+j] = var[col_oid+j];
		}
	}
)";
	} else {
		oss << R"(
	for (int i=start; i < end; ++i) {
    int col_out = i*n_masks;
    int col_oid = lineage[i]*n_masks;
		for (int j=0; j < n_masks; j+=32) {
			__m512i b = _mm512_stream_load_si512((__m512i*)&var[col_oid+j]);
			_mm512_store_si512((__m512i*)&out[col_out+j], b);
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

string HashAggregateIntervene2D(EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                PhysicalOperator* op) {
  std::cout << "Hash Aggregate Intervene 2D" << std::endl;
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
		n_groups = lop->log_index->ha_hash_index.size(); //lop->chunk_collection.Count();
	}

	fade_data[op->id].n_groups = n_groups;

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	bool include_count = false;
	int batches = 4;
	int agg_count = 0;
	// Populate the aggregate child vectors
	for (idx_t i=0; i < aggregates.size(); ++i) {
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
			eval_code += get_agg_eval(config, aggregates.size(), agg_count++, "sum", out_var, in_val,  in_arr, output_type);
		}
	}

	if (include_count == true) {
		string out_var = "out_count";
		alloc_code += Fade::get_agg_alloc(0, "count", "int");
		eval_code += get_agg_eval(config, aggregates.size(), agg_count++, "count", out_var, "1", "", "int");
	}

	string init_code;
	if (config.intervention_type == InterventionType::SCALE_UNIFORM) {
		init_code = get_agg_init_no_intervention(config, aggregates.size(), row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(), op->id,  fade_data[op->id].n_interventions, "agg", alloc_code, get_data_code, get_vals_code);
	} else {
		init_code = get_agg_init(config, aggregates.size(), row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(), op->id,  fade_data[op->id].n_interventions, "agg", alloc_code, get_data_code, get_vals_code);
	}
	string end_code = Fade::get_agg_finalize(config, fade_data[op->id]);

	code = init_code + eval_code + end_code;

	return code;
}

void  HashAggregateIntervene2DEval(int thread_id, EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                  PhysicalOperator* op, void* handle, void* var_0) {
  int opid = fade_data[op->children[0]->id].opid;
	if (config.use_duckdb) {
		fade_data[op->id].agg_duckdb_fn(thread_id, fade_data[op->id].lineage[0].data(), var_0, fade_data[op->id].alloc_vars,
		                                op->children[0]->lineage_op->chunk_collection, fade_data[opid].del_set);
	} else {
		fade_data[op->id].agg_fn(thread_id, fade_data[op->id].lineage[0].data(), var_0, fade_data[op->id].alloc_vars,
		                     fade_data[op->id].input_data_map,  fade_data[opid].del_set);
	}
}

void GenCodeAndAlloc(EvalConfig& config, string& code, PhysicalOperator* op,
                    std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                     std::unordered_map<std::string, std::vector<std::string>>& spec) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		GenCodeAndAlloc(config, code, op->children[i].get(), fade_data, spec);
	}


	// two cases for scaling intervention: 1) where intervention, 2) uniform intervention (apply scaling equally to all tuples)
	// SCALE_UNIFORM, SCALE_RANDOM
	// uniform: only allocate memory for the aggregate results
	// random: allocate single intervention/selection vector per table with 0s/1s with prob
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (spec.find(op->lineage_op->table_name) == spec.end() && config.intervention_type == InterventionType::DENSE_DELETE_SPEC ||
		    config.intervention_type == InterventionType::SCALE_UNIFORM) {
      		std::cout << "skip this scan " << op->lineage_op->table_name <<std::endl;
			fade_data[op->id].n_interventions = 0;
			fade_data[op->id].n_masks = 0;
			return;
		}
    
    
		std::cout << "check this scan " << op->lineage_op->table_name << std::endl;
		idx_t row_count = op->lineage_op->log_index->table_size;

		if (config.prune) {
			row_count = fade_data[op->id].lineage[0].size();
		}

		idx_t n_masks = std::ceil(config.n_intervention / config.mask_size);
		fade_data[op->id].n_interventions = config.n_intervention;

		if (config.n_intervention == 1) {
		  // check if function is incremental, then alloc: fade_data[op->id].annotations
		  // to store zero elements row ids.
		  if (config.incremental == false) {
				fade_data[op->id].single_del_interventions = new int8_t[row_count];
		  }
		} else {
			//fade_data[op->id].del_interventions = new __mmask16[row_count * n_masks];
			fade_data[op->id].del_interventions = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * row_count * n_masks);
			std::cout << "alloc del_interventions " << row_count << " " << n_masks
				<< " " << fade_data[op->id].del_interventions << std::endl;
		}
		fade_data[op->id].n_masks = n_masks;
	} else if (op->type == PhysicalOperatorType::FILTER) {
		fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		idx_t n_masks = fade_data[op->id].n_masks;
		if (n_masks > 0 || fade_data[op->id].n_interventions == 1) {
			idx_t row_count = fade_data[op->id].lineage[0].size();
			if (config.prune) {
				fade_data[op->id].del_interventions  = fade_data[op->children[0]->id].del_interventions;
				fade_data[op->id].single_del_interventions  = fade_data[op->children[0]->id].single_del_interventions;
			} else if (config.n_intervention == 1) {
				if (config.incremental == false) {
					fade_data[op->id].single_del_interventions = new int8_t[row_count];
				}
			} else {
				//fade_data[op->id].del_interventions = new __mmask16[row_count * n_masks];
			  fade_data[op->id].del_interventions = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * row_count * n_masks);
			}
			if (config.prune == false)
				code += FilterCodeAndAlloc(config, op, op->lineage_op, fade_data);
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		int lhs_n = fade_data[op->children[0]->id].n_interventions;
		int rhs_n = fade_data[op->children[1]->id].n_interventions;
		fade_data[op->id].n_interventions = (lhs_n > 0) ? lhs_n : rhs_n;
		fade_data[op->id].n_masks = (lhs_n > 0) ? fade_data[op->children[0]->id].n_masks : 
      fade_data[op->children[1]->id].n_masks;
		idx_t n_masks = fade_data[op->id].n_masks;
		if (n_masks > 0 || fade_data[op->id].n_interventions == 1) {
			idx_t row_count = fade_data[op->id].lineage[0].size();
			if (config.n_intervention == 1) {
				if (config.incremental == false) {
					fade_data[op->id].single_del_interventions = new int8_t[row_count];
				}
			} else {
				//fade_data[op->id].del_interventions = new __mmask16[row_count * n_masks];
			  fade_data[op->id].del_interventions = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * row_count * n_masks);
			}
			code += JoinCodeAndAlloc(config, op, op->lineage_op, fade_data);
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		fade_data[op->id].del_interventions  = fade_data[op->children[0]->id].del_interventions;
		fade_data[op->id].single_del_interventions  = fade_data[op->children[0]->id].single_del_interventions;
		if (config.intervention_type == InterventionType::SCALE_UNIFORM) {
			fade_data[op->id].n_interventions = config.n_intervention;
		} else {
			fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		}
		fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;
		Fade::HashAggregateAllocate(config, op->lineage_op, fade_data, op);
		code += HashAggregateIntervene2D(config, op->lineage_op, fade_data, op);
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	  //  UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
	}  else if (op->type == PhysicalOperatorType::PROJECTION) {
		fade_data[op->id].n_masks  = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].del_interventions  = fade_data[op->children[0]->id].del_interventions;
		fade_data[op->id].single_del_interventions  = fade_data[op->children[0]->id].single_del_interventions;
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
	}

	if (config.debug) {
		std::cout << "GenCodeAndAlloc(" << op->id << ") -> n_masks:"
		          << fade_data[op->id].n_masks << ", n_interventions: "
		          << fade_data[op->id].n_interventions << std::endl;
	}
}

void BindFunctions(EvalConfig& config, void* handle, PhysicalOperator* op,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {

	for (idx_t i = 0; i < op->children.size(); ++i) {
		BindFunctions(config, handle, op->children[i].get(), fade_data);
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		if (config.prune) return;
		if (fade_data[op->id].n_masks > 0 || fade_data[op->id].n_interventions == 1) {
			string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
			fade_data[op->id].filter_fn = (int(*)(int, int*, void*, void*, std::set<int>&, std::set<int>&))dlsym(handle, fname.c_str());
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		if (fade_data[op->id].n_masks > 0 || fade_data[op->id].n_interventions == 1) {
			string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
			fade_data[op->id].join_fn = (int(*)(int, int*, int*, void*, void*, void*, std::set<int>&, std::set<int>&, std::set<int>&))dlsym(handle, fname.c_str());
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		string fname = "agg_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		if (config.use_duckdb) {
			fade_data[op->id].agg_duckdb_fn = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
			                                           ChunkCollection&, std::set<int>&))dlsym(handle, fname.c_str());
		} else {
			fade_data[op->id].agg_fn = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
			    std::unordered_map<int, void*>&, std::set<int>&))dlsym(handle, fname.c_str());
		}
	}
}


void Intervention2DEval(int thread_id, EvalConfig& config, void* handle, PhysicalOperator* op,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		Intervention2DEval(thread_id, config, handle, op->children[i].get(), fade_data);
	}
    
	if (op->type == PhysicalOperatorType::FILTER) {
		if (config.prune) return;
		if (fade_data[op->id].n_masks > 0 || fade_data[op->id].n_interventions == 1) {
			idx_t row_count = fade_data[op->id].lineage[0].size();
      int opid = fade_data[op->children[0]->id].opid;
			if (config.n_intervention == 1 && config.incremental == true) {
     //   std::cout << op->id <<  " Filter start " << opid << " " << row_count << std::endl;
				int result = fade_data[op->id].filter_fn(thread_id, fade_data[op->id].forward_lineage[0].data(),
				                                         fade_data[op->children[0]->id].single_del_interventions,
				                                         fade_data[op->id].single_del_interventions,
                                                 fade_data[opid].del_set,
				                                         fade_data[op->id].del_set);
      //  std::cout << "Filter end" << std::endl;
			} else if (config.n_intervention == 1 && config.incremental == false) {
				int result = fade_data[op->id].filter_fn(thread_id, fade_data[op->id].lineage[0].data(),
				                                         fade_data[op->children[0]->id].single_del_interventions,
				                                         fade_data[op->id].single_del_interventions,
                                                 fade_data[opid].del_set,
				                                         fade_data[op->id].del_set);
			} else {
				int result = fade_data[op->id].filter_fn(thread_id, fade_data[op->id].lineage[0].data(),
				                                         fade_data[op->children[0]->id].del_interventions,
				                                         fade_data[op->id].del_interventions,
				                                         fade_data[opid].del_set,
				                                         fade_data[op->id].del_set);
			}
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
      int lhs_opid = fade_data[op->children[0]->id].opid;
      int rhs_opid = fade_data[op->children[1]->id].opid;
		if (fade_data[op->id].n_masks > 0 || fade_data[op->id].n_interventions == 1) {
		    idx_t row_count = fade_data[op->id].lineage[0].size();
			if (config.n_intervention == 1 && config.incremental == true) {
      //  std::cout << op->id << " Join start " << lhs_opid << " " << rhs_opid << " " << row_count << std::endl;
     //   std::cout << fade_data[op->id].forward_lineage[0].size() << " "
       //   << fade_data[op->id].forward_lineage[1].size() << std::endl;
				int result = fade_data[op->id].join_fn(thread_id, fade_data[op->id].forward_lineage[0].data(),
				                                       fade_data[op->id].forward_lineage[1].data(),
				                                       fade_data[op->children[0]->id].single_del_interventions,
				                                       fade_data[op->children[1]->id].single_del_interventions,
				                                       fade_data[op->id].single_del_interventions,
				                                       fade_data[lhs_opid].del_set,
				                                       fade_data[rhs_opid].del_set,
				                                       fade_data[op->id].del_set);
     //   std::cout << "Join end" << std::endl;
			} else if (config.n_intervention == 1 && config.incremental == false) {
				int result = fade_data[op->id].join_fn(thread_id, fade_data[op->id].lineage[0].data(),
				                                       fade_data[op->id].lineage[1].data(),
				                                       fade_data[op->children[0]->id].single_del_interventions,
				                                       fade_data[op->children[1]->id].single_del_interventions,
				                                       fade_data[op->id].single_del_interventions,
					                                   fade_data[op->children[0]->id].del_set,
					                                   fade_data[op->children[1]->id].del_set,
					                                   fade_data[op->id].del_set);
			} else {
				int result = fade_data[op->id].join_fn(thread_id, fade_data[op->id].lineage[0].data(),
				                                       fade_data[op->id].lineage[1].data(), fade_data[op->children[0]->id].del_interventions,
				                                       fade_data[op->children[1]->id].del_interventions, fade_data[op->id].del_interventions,
				                                       fade_data[op->children[0]->id].del_set,
				                                       fade_data[op->children[1]->id].del_set,
				                                       fade_data[op->id].del_set);
			}
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		if (config.n_intervention == 1) {
			HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op, handle,
			                             fade_data[op->id].single_del_interventions);
		} else {
			HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op, handle,  fade_data[op->id].del_interventions);
		}
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	   // UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
	} else if (op->type == PhysicalOperatorType::PROJECTION) {
//		fade_data[op->id].del_set = fade_data[op->children[0]->id].del_set;
  }
}


void Clear(PhysicalOperator *op) {
	// massage the data to make it easier to query
	// for hash join, build hash table on the build side that map the address to id
	// for group by, build hash table on the unique groups
	if (op->lineage_op) {
		op->lineage_op->Clear();
	}

	for (idx_t i = 0; i < op->children.size(); ++i) {
		Clear(op->children[i].get());
	}
}

void GetForwardLineage(EvalConfig& config, PhysicalOperator* op,
                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		GetForwardLineage(config, op->children[i].get(), fade_data);
	}

	unordered_map<int, int> forward_lineage_map[2];
	if (op->type == PhysicalOperatorType::TABLE_SCAN || op->type == PhysicalOperatorType::FILTER) {
    fade_data[op->id].opid = op->id;
		int row_count = fade_data[op->id].lineage[0].size();
		for (int i=0; i < row_count; ++i) {
			forward_lineage_map[0][i] = fade_data[op->id].lineage[0][i];
		}
    int forward_row_count = fade_data[op->id].lineage[0].size();
	  if (op->type == PhysicalOperatorType::FILTER) {
      int opid = fade_data[op->children[0]->id].opid;
      forward_row_count = fade_data[opid].lineage[0].size();
    }
    fade_data[op->id].forward_lineage[0].assign(forward_row_count, -1);
		for (const auto& pair : forward_lineage_map[0]) {
			fade_data[op->id].forward_lineage[0][pair.first] = pair.second;
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
    fade_data[op->id].opid = op->id;
    int lhs_opid = fade_data[op->children[0]->id].opid;
    int rhs_opid = fade_data[op->children[1]->id].opid;
		int lhs_row_count = fade_data[lhs_opid].lineage[0].size();
		int rhs_row_count = fade_data[rhs_opid].lineage[0].size();
    int row_count = fade_data[op->id].lineage[0].size();
		for (int side=0; side < 2; side++) {
      for (int i=0; i < row_count; ++i) {
        forward_lineage_map[side][i] = fade_data[op->id].lineage[side][i];
      }
      int opid = fade_data[op->children[side]->id].opid;
      int forward_row_count = fade_data[opid].lineage[0].size();
			fade_data[op->id].forward_lineage[side].assign(forward_row_count, -1);
      std::cout << op->id << " " << lhs_opid << " " << rhs_opid << " " << lhs_row_count << " "
        << rhs_row_count << " " << row_count << " " << forward_row_count << std::endl;
			for (const auto& pair : forward_lineage_map[side]) {
				fade_data[op->id].forward_lineage[side][pair.second] = pair.first;
			}
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
    fade_data[op->id].opid = fade_data[op->children[0]->id].opid;
		idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		fade_data[op->id].lineage[0] = std::move(Fade::GetGBLineage(op->lineage_op, row_count));
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
    fade_data[op->id].opid = fade_data[op->children[0]->id].opid;
	} else if (op->type == PhysicalOperatorType::PROJECTION) {
    fade_data[op->id].opid = fade_data[op->children[0]->id].opid;
	}
}
/*
  1. traverse plan to construct template
  2. compile
  2. traverse plan to bind variables and execute code
*/
string Fade::Whatif(PhysicalOperator *op, EvalConfig config) {
	// timing vars
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;

	// holds any extra data needed during exec
	std::unordered_map<idx_t, FadeDataPerNode> fade_data;

	// 1. Parse Spec = table_name.col:scale
	std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec( config);

	// TODO: run post processing, code gen, and compilation after query execution
    // TODO: combine post process and gen. materialize all lineage as 1D array and free lineage temp data
	// 2. Post Process
	start_time = std::chrono::steady_clock::now();
	LineageManager::PostProcess(op);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double post_processing_time = time_span.count();

	// 3. retrieve lineage
	start_time = std::chrono::steady_clock::now();
	GetLineage(config, op, fade_data);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double lineage_time = time_span.count();

	// 4.1 Prune
	double prune_time = 0;
	if (config.prune) {
		start_time = std::chrono::steady_clock::now();
		vector<int> out_order;
		PruneLineage(config, op, fade_data, out_order);
		end_time = std::chrono::steady_clock::now();
		time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
		prune_time = time_span.count();
	}
	
  int forward_lineage_time = 0;
	if (config.n_intervention == 1 && config.incremental == true) {
		start_time = std::chrono::steady_clock::now();
		GetForwardLineage(config, op, fade_data);
		end_time = std::chrono::steady_clock::now();
		time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
		forward_lineage_time = time_span.count();
	}

  	Clear(op);

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

	//if (config.debug)
		std::cout << final_code << std::endl;

	start_time = std::chrono::steady_clock::now();
	void* handle = compile(final_code, 0);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double compile_time = time_span.count();

	if (handle == nullptr) return "select 0";

	// 3. Prepare base interventions; should be one time cost per DB
	start_time = std::chrono::steady_clock::now();
	GenRandomWhatifIntervention(config, op, fade_data, handle, columns_spec);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double intervention_gen_time = time_span.count();

	BindFunctions(config, handle,  op, fade_data);

	std::vector<std::thread> workers;

	start_time = std::chrono::steady_clock::now();
  	if (config.num_worker > 1) {
		for (int i = 0; i < config.num_worker; ++i) {
		  workers.emplace_back(Intervention2DEval, i, std::ref(config), handle,  op, std::ref(fade_data));
		}
		// Wait for all tasks to complete
		for (std::thread& worker : workers) {
		  worker.join();
		}
	} else {
		Intervention2DEval(0, config, handle,  op, fade_data);
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
	       + to_string(intervention_gen_time) + " as intervention_gen_time, "
	       + to_string(prep_time) + " as prep_time, "
	       + to_string(lineage_time) + " as lineage_time, "
	       + to_string(prune_time) + " as prune_time, "
	       + to_string(compile_time) + " as compile_time, "
	       + to_string(eval_time) + " as eval_time";
}

} // namespace duckdb
#endif

