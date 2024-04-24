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


int (*random_fn)(int, int, float, int, void*, std::set<int>&, int, std::vector<__mmask16>&);
int rand_count = 65535;
bool use_preprep_tm = false;

std::vector<__mmask16> base(rand_count);

string fill_random_code(EvalConfig& config) {
	std::ostringstream oss;
	oss << R"(
extern "C" int fill_random(int thread_id, int row_count, float prob, int n_masks, void* del_interventions_ptr, std::set<int>& del_set, int rand_count, std::vector<__mmask16>& base) {
//	std::cout << "fill_random: " << row_count  << " " << prob << " " <<  n_masks << std::endl;
	// Initialize a random number generator
  int64_t count = 0;
)";

	if (config.n_intervention == 1) {
		oss << "\t int8_t* __restrict__ del_interventions = (int8_t* __restrict__)del_interventions_ptr;\n";
	} else if (config.n_intervention == 1 && config.incremental == false) {
	} else {
		oss << "\t __mmask16* __restrict__ del_interventions = (__mmask16* __restrict__)del_interventions_ptr;\n";
	}

	oss << "\tconst int num_threads  = " << config.num_worker << ";\n";
	oss << "\tint batch_size  = row_count / num_threads;\n";
	oss << "\tif (row_count % num_threads > 0) batch_size++;\n";
	oss << "\tconst int start = thread_id * batch_size;\n";
	oss << "\tint end   = start + batch_size;\n";
	oss << "\tif (end >= row_count) { end = row_count; }\n";

  // HACK: generate one intervention randomly, then reuse it
	if (config.n_intervention > 1) {
		oss << R"(
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, rand_count);
)";
    int c = 0;
    for (int i = 0; i < rand_count; ++i) {
      base[c++] = 0x0;
      for (int k = 0; k < 16; ++k) {
		    if ((((double)rand() / RAND_MAX) < config.probability)) {
			      base[c] &= (1 << k);
        }
      }
    }
  }
	oss << "\nfor (int i = start; i < end; ++i) {\n";
	if (config.n_intervention == 1 && config.incremental == true) {
		oss << "\n\tif ((((double)rand() / RAND_MAX) < prob))";
		oss << "\n\t\tdel_set.insert(i);";
	} else if (config.n_intervention == 1 && config.incremental == false) {
		oss << "\n\tdel_interventions[i] = !(((double)rand() / RAND_MAX) < prob);";
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

//  std::cout << thread_id << " random done " << del_set.size() << " " << count << " " << count /( 16) << " " << start << " " << end << std::endl;
	return 0;
}
)";
	return oss.str();
}


void GenRandomWhatifIntervention(int thread_id, EvalConfig& config, PhysicalOperator* op,
                                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                 void* handle,
                                 std::unordered_map<std::string, std::vector<std::string>>& spec) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		GenRandomWhatifIntervention(thread_id, config, op->children[i].get(), fade_data, handle, spec);
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (spec.find(op->lineage_op->table_name) == spec.end() && !spec.empty()) {
			return;
		} else if (!spec.empty()) {
     // std::cout << "generate interventions " << op->lineage_op->table_name <<  " "
      //  << spec[op->lineage_op->table_name][0] << std::endl;
    }

    idx_t out_count = op->lineage_op->backward_lineage[0].size();
    idx_t max_row_count = op->lineage_op->backward_lineage[0][out_count-1];
    for (int i=0; i < out_count; ++i)
      if (op->lineage_op->backward_lineage[0][i] > max_row_count)
        max_row_count = op->lineage_op->backward_lineage[0][i];
    int input_target_matrix_count = max_row_count;
    if (config.prune) input_target_matrix_count = out_count;

    //std::cout << "random fn: " << out_count << " " << max_row_count << std::endl;
		if (config.n_intervention == 1) {
      random_fn(thread_id, input_target_matrix_count, config.probability, fade_data[op->id].n_masks, fade_data[op->id].base_single_del_interventions, fade_data[op->id].del_set, rand_count, base);
		} else {
      random_fn(thread_id, input_target_matrix_count, config.probability, fade_data[op->id].n_masks, fade_data[op->id].base_target_matrix, fade_data[op->id].del_set, rand_count, base);
		}
	}
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
duckdb::ChunkCollection &chunk_collection, std::set<int>& del_set, const int start, const int end) {
)";
	} else {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
              std::unordered_map<int, void*>& input_data_map, std::set<int>& del_set, const int start, const int end) {
)";
	}


	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";

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
duckdb::ChunkCollection &chunk_collection, std::set<int>& del_set, const int start, const int end) {
)";
	} else {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
              std::unordered_map<int, void*>& input_data_map, std::set<int>& del_set, const int start, const int end) {
)";
	}


	if (config.n_intervention == 1) {
		oss << "\t int8_t* __restrict__ var_0 = (int8_t* __restrict__)var_0_ptr;\n";
	} else {
		oss << "\t __mmask16* __restrict__ var_0 = (__mmask16* __restrict__)var_0_ptr;\n";
	}

	oss << "\tconst int mask_size = " << config.mask_size << ";\n";
	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";
	oss << "\tconst int n_masks  = " << n_masks << ";\n";
	oss << "\tconst int num_threads  = " << config.num_worker << ";\n";

  if (config.debug)
    oss << "\tstd::cout << \"Specs: \"  << \" \" << mask_size << \" \" << n_interventions << \" \" << n_masks << \" \" << start << \" \" << end << std::endl;";

	oss << alloc_code;

	if (config.incremental && config.n_intervention == 1 && config.use_duckdb == false) {
		// convert intervention array to annotation array where var[i]==1
		oss << get_data_code;
		oss << R"(
       // std::vector<int> zeros(del_set.size());
        //int c = 0;
        //for (auto it = del_set.begin(); it != del_set.end(); ++it) {
          //  zeros[c++] = *it;
        //}
       // for (int i=0; i < zeros.size(); ++i) {
				for (int i : del_set) {
          int iid = i; // zeros[i];
          int oid = lineage[iid];
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
					oss << "\t\t\t\t\tif (var_0[i+offset] == 1) continue;\n";
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
		oss << "out_count[col + (row + k) ] += (1 &  tmp_mask >> k);";
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
				for (int i : del_set) {
     //   for (int i=0; i < zeros.size(); ++i) {
					int iid = i; // zeros[i];
					int oid = lineage[iid];
)";
		} else if (config.use_duckdb) {
			oss << R"(
				}
				for (int i=0; i < collection_chunk.size(); ++i) {
				int oid = lineage[i+offset];
)";
			if (total_agg_count > 1 && ( config.intervention_type != InterventionType::SCALE_UNIFORM
			                            &&  config.intervention_type != InterventionType::SCALE_RANDOM)) {
				oss << "\t\t\t\t\tif (var_0[i+offset] == 1) continue;\n";
			}
		} else {
			oss << R"(
				}
				for (int i=start; i < end; ++i) {
					int oid = lineage[i];
)";
			if (total_agg_count > 1 && ( config.intervention_type != InterventionType::SCALE_UNIFORM
			                            &&  config.intervention_type != InterventionType::SCALE_RANDOM)) {
				oss << "\t\t\t\t\tif (var_0[i] == 1) continue;\n";
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
				oss << " * ~var_0[i+offset];\n";
			} else {
				oss << " * ~var_0[i];\n";
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
			oss << "\t\t\t\t__mmask16 tmp_mask = ~var_0[(i+offset)*n_masks+j];\n";
		} else {
			oss << "\t\t\t\t__mmask16 tmp_mask = ~var_0[i*n_masks+j];\n";
		}
    if (!config.is_scalar) {
      oss << "\t__m512i zeros_i = _mm512_setzero_si512();\n";
      oss << "\t__m512 zeros = _mm512_setzero_ps();\n";
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
		if (fn != "count" && config.intervention_type == InterventionType::SCALE_RANDOM) {
			oss << "{\n";
			if (data_type == "float") {
				oss << "\t__m512 a  = _mm512_load_ps((__m512*) &"+out_var+"[col + row]);\n";
				oss << "\t__m512 v = _mm512_mask_blend_ps(tmp_mask, x_vec_"+in_var+",vec_"+in_var+ ");\n";
				oss << "\t_mm512_store_ps((__m512*) &"+out_var+"[col + row], _mm512_add_ps(a, v));\n";
			} else if (data_type == "int") {
				oss << "\t__m512i a = _mm512_load_si512((__m512i*)&" + out_var + "[col+row]);\n";
				oss << "\t__m512i v = _mm512_mask_blend_epi32(tmp_mask, x_vec_"+in_var+", vec_"+ in_var + ");\n";
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

string get_batch_join_template(EvalConfig &config, PhysicalOperator *op,
                          std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	std::ostringstream oss;
	if (config.is_scalar || config.n_intervention < 512) {
		oss << R"(
      int lhs_col = lhs_lineage[i] * n_masks;
      int rhs_col = rhs_lineage[i] * n_masks;
      int col = i*n_masks;
      for (int j=0; j < n_masks; ++j) {
)";
		if ( fade_data[op->children[0]->id].n_masks > 0 && fade_data[op->children[1]->id].n_masks > 0) {
			oss << R"(out[col+j] = lhs_var[lhs_col+j] | rhs_var[rhs_col+j];)";
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
		_mm512_store_si512((__m512i*)&out[col+j], _mm512_or_si512(a, b));
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
	if (config.n_intervention == 1 && config.incremental == true) {
    oss << R"(extern "C" int )"
          << fname
          << R"((int thread_id, std::unordered_map<int, std::vector<int>> &lhs_lineage, 
          std::unordered_map<int, std::vector<int>>   &rhs_lineage,
          void* __restrict__ lhs_var_ptr,  void* __restrict__ rhs_var_ptr,  void* __restrict__ out_ptr,
          std::set<int>& lhs_del_set,
          std::set<int>& rhs_del_set,
          std::set<int>& out_del_set, const int start, const int end) {
  )";

  } else {
    oss << R"(extern "C" int )"
          << fname
          << R"((int thread_id, int* __restrict__ lhs_lineage, int* __restrict__  rhs_lineage,
          void* __restrict__ lhs_var_ptr,  void* __restrict__ rhs_var_ptr,  void* __restrict__ out_ptr,
          std::set<int>& lhs_del_set,
          std::set<int>& rhs_del_set,
          std::set<int>& out_del_set, const int start, const int end) {
  )";

  }
	// 	output_annotation.insert(side_forward_lineage[input id])

	if (config.n_intervention == 1 && config.incremental == true) {
		oss << R"(
  for (int iid : rhs_del_set) {
    if (!rhs_lineage[iid].empty()) {
      for (int oid : rhs_lineage[iid])
        out_del_set.insert(oid);
    }
  }
    
  for (int iid : lhs_del_set) {
    if (!lhs_lineage[iid].empty()) {
      for (int oid : lhs_lineage[iid])
        out_del_set.insert(oid);
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

  if (config.debug)
	  oss << "std::cout << \"JOIN: \"  << \" \" <<  n_masks << " " << start << " " << end << std::endl;";

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
        std::set<int>& del_set, std::set<int>& out_del_set, const int start, const int end) {
)";

	if (config.n_intervention == 1) {
		oss << "\t int8_t* __restrict__ out = (int8_t* __restrict__)out_ptr;\n";
		oss << "\t int8_t* __restrict__ var = (int8_t* __restrict__)var_ptr;\n";
	} else {
		oss << "\t __mmask16* __restrict__ out = (__mmask16* __restrict__)out_ptr;\n";
		oss << "\t __mmask16* __restrict__ var = (__mmask16* __restrict__)var_ptr;\n";
	}



	oss << "\tconst int n_masks = " + to_string(fade_data[op->id].n_masks) + ";\n";


	if (config.debug)
		oss << "std::cout << \"Filter: \" << start << \" \" << end  << \" \" <<  n_masks << std::endl;\n";

	if (config.n_intervention == 1 && config.incremental == true) {
		oss << R"(
		for (int iid : del_set) {
      int oid = lineage[iid];
      if (oid >= 0) {
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
	} else if (config.is_scalar || config.n_intervention < 512) {
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


/*
// if nested, then take the output of the previous agg as input
// level 1: select key, avg(col1) as out0
// level 2: select count(key),  sum(key*out0), sum(key), sum(out0), sum(key*key)
// SELECT ((COUNT(R.x)*SUM(R.x*R.y))-(SUM(R.x)*SUM(R.y)))/
//    ((count(R.X)*(SUM(R.x*R.x))-(SUM(R.x))*(SUM(R.x))))
 */
string HashAggregateIntervene2DNested(EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                                int keys, int n_groups) {
	const int n_interventions = fade_data[op->id].n_interventions;
	fade_data[op->id].n_groups = n_groups;
	int child_agg_id = fade_data[op->id].child_agg_id;
	string fname = "agg_" + to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) + "_" +
	               to_string(config.is_scalar);

	string code;

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	bool include_count = false;
	int batches = 4;
	int agg_count = 0;

	// TODO: need to access the key
	string in_arr = "col_0";
	string input_type = fade_data[child_agg_id].alloc_vars_types["out_0"];
	string get_data_code = "\t\t" + input_type + "* " + in_arr + " =(" + input_type + " *)input_data_map[\"out_0\"][0];\n";

	// level 2: select count(key),  sum(key*out0), sum(key), sum(out0), sum(key*key)

	// alloc for count
	string out_var = "out_count";
	fade_data[op->id].alloc_vars[out_var].resize(config.num_worker);
	for (int t = 0; t < config.num_worker; ++t) {
		Fade::allocate_agg_output<int>("int", t, n_groups, n_interventions, out_var, op, fade_data);
	}
	fade_data[op->id].alloc_vars_index[out_var] = -1;

	// alloc for 4 sums
	for (int i = 0; i < 4; ++i) {
		int interventions_count = n_interventions;
		if (i < 2) {
			interventions_count = 1;
		}
		out_var = "sum_" + to_string(i);
		fade_data[op->id].alloc_vars[out_var].resize(config.num_worker);
		for (int t = 0; t < config.num_worker; ++t) {
			if (input_type == "int") {
				Fade::allocate_agg_output<int>("int", t, n_groups, interventions_count, out_var, op, fade_data);
			} else {
				Fade::allocate_agg_output<float>("float", t, n_groups, interventions_count, out_var, op, fade_data);
			}
		}
		fade_data[op->id].alloc_vars_index[out_var] = i;
	}

	std::ostringstream oss;
	oss << R"(extern "C" int )" << fname
	    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
             std::unordered_map<std::string, std::vector<void*>>& input_data_map, std::set<int>& del_set) {
)";

	idx_t chunk_count = op->children[0]->lineage_op->chunk_collection.ChunkCount();
	int n_masks = n_interventions / config.mask_size;

	oss << "\tconst int chunk_count = " << chunk_count << ";\n";
	oss << "\tconst int row_count = " << row_count << ";\n";
	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";
	oss << "\tconst int num_threads  = " << config.num_worker << ";\n";
	oss << "\tconst int mask_size = " << config.mask_size << ";\n";
	oss << "\tconst int n_masks  = " << n_masks << ";\n";

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
	oss << get_data_code;
	// change this according to if it is batched or single, vectorized or scalar
	oss << R"(
int*__restrict__ out_count = (int*)alloc_vars["out_count"][thread_id];
int*__restrict__ sum_0 = (int*)alloc_vars["sum_0"][thread_id];
int*__restrict__ sum_1 = (int*)alloc_vars["sum_1"][thread_id];
int*__restrict__ sum_2 = (int*)alloc_vars["sum_2"][thread_id];
int*__restrict__ sum_3 = (int*)alloc_vars["sum_3"][thread_id];
)";

 	if (config.n_intervention == 1) {
		// level 2: select count(key),  sum(key*out0), sum(key), sum(out0), sum(key*key)
		oss << R"(
for (int i=start; i < end; ++i) {
	int oid = lineage[i];
	out_count[oid] += 1;
	sum_0[oid] += (i * col_0[i]);
	sum_1[oid] += i;
	sum_2[oid] += col_0[i];
	sum_3[oid] += (i*i);
}
)";
	} else  {
		// level 2: select count(key),  sum(key*out0), sum(key), sum(out0), sum(key*key)
		string core_loop  = "";
		if (config.is_scalar) {
			core_loop = R"(
for (int j=0; j < n_masks; ++j) {
	int row = j * mask_size;
	for (int k=0; k < mask_size; k++) {
		sum_2[col + (row + k) ] += (i * col_0[ i + (row + k) ]);
		sum_3[col + (row + k) ] += col_0[ i + (row + k) ];
	}
})";
		} else {
			core_loop = R"(
__m512i scalar = _mm512_set1_epi32(i); // Broadcast x to all elements of the 512-bit vector
for (int j=0; j < n_masks; ++j) {
	int row = j * mask_size;
	__m512i b_0  = _mm512_load_si512((__m512i*) &col_0[col + row]);
	__m512i a_2  = _mm512_load_si512((__m512i*) &sum_2[col + row]);
	__m512i a_3  = _mm512_load_si512((__m512i*) &sum_3[col + row]);
	__m512i a_4 = _mm512_mullo_epi32(a_3, scalar);
	_mm512_store_si512((__m512i*) &sum_2[col + row], _mm512_add_epi32(a_2, b_0)); // a_2 = a_2 + b_0
	_mm512_store_si512((__m512i*) &sum_3[col + row], _mm512_add_epi32(a_4, b_0)); // a_3 = a_3 + b_0 * i
})";
		}

		oss << R"(
for (int i=start; i < end; ++i) {
	int oid = lineage[i];
	int col = oid*n_interventions;
	out_count[oid] += 1;
	sum_0[oid] += i;
	sum_1[oid] += (i*i);
)";
		oss << core_loop;
		oss << R"(

}
)";
	}

oss << R"(
return 0;
})";
	return oss.str();
}

string HashAggregateIntervene2D(EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                                int keys, int n_groups) {
	const int n_interventions = fade_data[op->id].n_interventions;

	string eval_code;
	string code;
	string alloc_code;
	string get_data_code;
	string get_vals_code;

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
			int col_idx = i + keys;

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
      if (config.intervention_type == InterventionType::SCALE_RANDOM) {
        if (output_type == "int") {
			    get_vals_code += "\t\t\t__m512i vec_"  + in_val + " = _mm512_set1_epi32(" + in_val + ");\n";
			    get_vals_code += "\t\t\t__m512i x_vec_"  + in_val + " = _mm512_set1_epi32(" + in_val + "*(0.8+1));\n";
        } else {
			    get_vals_code += "\t\t\t__m512 x_vec_"  + in_val + " = _mm512_set1_ps(" + in_val + ");\n";
			    get_vals_code += "\t\t\t__m512 vec_"  + in_val + " = _mm512_set1_ps(" + in_val + "*(0.8+1));\n";
        }
      }
			// access output arrays
			alloc_code += Fade::get_agg_alloc(i, "sum", output_type);
			// core agg operation
			eval_code += get_agg_eval(config, aggregates.size(), agg_count++, "sum", out_var, in_val,  in_arr, output_type);
		}
	}

	if (include_count == true) {
		string out_var = "out_count";
    if (config.intervention_type == InterventionType::SCALE_RANDOM) {
      get_vals_code += "\t\t\t__m512i vec_1 = _mm512_set1_epi32(1);\n";
      get_vals_code += "\t\t\t__m512i x_vec_1 = _mm512_set1_epi32(0.8+1);\n";
    }
		alloc_code += Fade::get_agg_alloc(0, "count", "int");
		eval_code += get_agg_eval(config, aggregates.size(), agg_count++, "count", out_var, "1", "", "int");
	}

  

	string init_code;
	if (config.intervention_type == InterventionType::SCALE_UNIFORM) {
		init_code = get_agg_init_no_intervention(config, aggregates.size(), row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(),
		                                         op->id,  fade_data[op->id].n_interventions, "agg", alloc_code, get_data_code, get_vals_code);
	} else {
		init_code = get_agg_init(config, aggregates.size(), row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(), op->id,
		                         fade_data[op->id].n_interventions, "agg", alloc_code, get_data_code, get_vals_code);
	}
	string end_code = Fade::get_agg_finalize(config, fade_data[op->id]);

	code = init_code + eval_code + end_code;

	return code;
}

void  HashAggregateIntervene2DEval(int thread_id, EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                  PhysicalOperator* op, void* handle, void* var_0) {
	// check if this has child aggregate, if so then get its id
  int opid = fade_data[op->children[0]->id].opid;
  	
  idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
  idx_t chunk_count = op->children[0]->lineage_op->chunk_collection.ChunkCount();
  int start = 0;
  int end = 0;
  int batch_size = chunk_count / config.num_worker;
  if (config.use_duckdb) {
    if (chunk_count % config.num_worker > 0)
      batch_size++;
    start = thread_id * batch_size;;
    end = start + batch_size;
    if (end >= chunk_count) { end = chunk_count; }
  } else {
    batch_size = row_count / config.num_worker;
    if (row_count % config.num_worker > 0)
      batch_size++;
    start = thread_id * batch_size;;
    end = start + batch_size;
    if (end >= chunk_count) { end = row_count; }
  }
  if (config.debug)
    std::cout << "Count summary: " << row_count << " " << chunk_count << " " << batch_size << " " << start << " " << end << " " << config.num_worker << " " << thread_id << std::endl;
	if (config.use_duckdb && fade_data[op->id].has_agg_child == false) {
		fade_data[op->id].agg_duckdb_fn(thread_id, op->lineage_op->forward_lineage[0].data(), var_0, fade_data[op->id].alloc_vars,
		                                op->children[0]->lineage_op->chunk_collection, fade_data[opid].del_set, start, end);
	} else {
		if (fade_data[op->id].has_agg_child) {
			fade_data[op->id].agg_fn_nested(thread_id, op->lineage_op->forward_lineage[0].data(), var_0, fade_data[op->id].alloc_vars,
			                         fade_data[fade_data[op->id].child_agg_id].alloc_vars,  fade_data[opid].del_set, start, end);
		} else {
			fade_data[op->id].agg_fn(thread_id, op->lineage_op->forward_lineage[0].data(), var_0, fade_data[op->id].alloc_vars,
										 fade_data[op->id].input_data_map,  fade_data[opid].del_set, start, end);
		}
	}
}



void GenCodeAndAlloc(EvalConfig& config, string& code, PhysicalOperator* op,
                    std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                     std::unordered_map<std::string, std::vector<std::string>>& spec) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		GenCodeAndAlloc(config, code, op->children[i].get(), fade_data, spec);
	}

	// two cases for scaling intervention: 1) scaling with selection vector (SCALE_RANDOM), 2) uniform scaling (SCALE_UNIFORM)
	// uniform: only allocate memory for the aggregate results
	// random: allocate single intervention/selection vector per table with 0s/1s with prob
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
    string table_name  = op->lineage_op->table_name;
    fade_data[op->id].opid = op->id;
		if (spec.find(table_name) == spec.end() && !spec.empty()) {
      if (config.debug)
      		std::cout << "skip this scan " << table_name <<std::endl;
			fade_data[op->id].n_interventions = 0;
			fade_data[op->id].n_masks = 0;
			return;
		} else if (!spec.empty()) {
      string col_spec = spec[table_name][0];
      if (col_spec.substr(0, 3) == "npy") {
        use_preprep_tm = true;
        if (config.debug) std::cout << "Use Pre generated interventions" << std::endl;
        // load interventions
        FILE * fname = fopen((table_name + ".npy").c_str(), "r");
        if (fname == nullptr) {
          std::cerr << "Error: Unable to open file." << std::endl;
          return;
        }

        std::stringstream ss(col_spec);
        string prefix, rows_str, cols_str;
        getline(ss, prefix, '_');
        getline(ss, rows_str, '_');
        getline(ss, cols_str, '_');

        int rows = std::stoi(rows_str);
        int cols = std::stoi(cols_str);
        if (config.debug)
          std::cout << table_name << " " << col_spec << " " << rows << " " << cols << std::endl;
			  uint8_t* temp  = (uint8_t*)aligned_alloc(64, sizeof(uint8_t) * rows * cols);
        size_t fbytes = fread(temp, sizeof(uint8_t), sizeof(uint8_t) * rows * cols,  fname);
        if ( fbytes != sizeof(uint8_t) * rows * cols) {
          fprintf(stderr, "read failed");
           exit(EXIT_FAILURE);
        }
        int n_masks =  cols / 2;
        int n_interventions = cols * 8;

        fade_data[op->id].base_target_matrix = (__mmask16*)temp;
		    fade_data[op->id].n_masks = n_masks;;
		    fade_data[op->id].n_interventions = n_interventions;;
			  int output_count = op->lineage_op->backward_lineage[0].size();
        fade_data[op->id].base_rows = rows;
        // 1. iterate over base_target matrix and check that there are bits set
        if (config.debug) {
          vector<int> nonzero_count(n_interventions);
          std::cout << "n_masks: " << n_masks << " n_interventions: " << n_interventions << " rows: " << rows << std::endl;
          for (int r=0; r < rows; r++) {
            for (int c=0; c < n_masks; c++)
            for (int k=0; k < 16; ++k) {
              if (fade_data[op->id].base_target_matrix[r * n_masks + c] & (1 << k) )
                nonzero_count[c*16+k]++;
            }
          }

          for (int c=0; c < nonzero_count.size(); c++)
            std::cout << c << " " << nonzero_count[c] << std::endl;
        }
        if (rows != output_count) {
          fade_data[op->id].del_interventions = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * output_count * fade_data[op->id].n_masks);
          if (config.debug)
            std::cout << rows << " alloc del_interventions " << output_count << " " << fade_data[op->id].n_masks
              << " " << fade_data[op->id].del_interventions << std::endl;
				  code += FilterCodeAndAlloc(config, op, op->lineage_op, fade_data);
        } else {
          fade_data[op->id].del_interventions = fade_data[op->id].base_target_matrix;
        }
        // record number of rows, cols
        fclose(fname);
        return;
      }
    }
    
		idx_t n_masks = std::ceil(config.n_intervention / config.mask_size);
		fade_data[op->id].n_interventions = config.n_intervention;
    
    if (config.debug)
		  std::cout << "check this scan " << op->lineage_op->table_name << std::endl;
		idx_t row_count = op->lineage_op->log_index->table_size;
		idx_t out_count = op->lineage_op->backward_lineage[0].size();
    idx_t max_row_count = op->lineage_op->backward_lineage[0][out_count-1];
    for (int i=0; i < out_count; ++i)
      if (op->lineage_op->backward_lineage[0][i] > max_row_count)
        max_row_count = op->lineage_op->backward_lineage[0][i];
    std::cout << "table scan -> " << out_count << " " << max_row_count << " " << row_count << std::endl;
    
    int input_target_matrix_count = max_row_count;
    if (config.prune) input_target_matrix_count = out_count;
    fade_data[op->id].base_rows = input_target_matrix_count;

		fade_data[op->id].n_masks = n_masks;

		if (config.n_intervention == 1) { // 1D
		  if (config.incremental == false) { // we don't need to pre allocate memory for incremental eval. It uses  std::set<int>
				fade_data[op->id].base_single_del_interventions = new int8_t[input_target_matrix_count];
        if (out_count != max_row_count && !config.prune) {
				  fade_data[op->id].single_del_interventions = new int8_t[out_count];
				  code += FilterCodeAndAlloc(config, op, op->lineage_op, fade_data);
        } else {
				  fade_data[op->id].single_del_interventions = fade_data[op->id].base_single_del_interventions;
        }
		  }
		} else {	// 2D
      fade_data[op->id].base_target_matrix = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * input_target_matrix_count * n_masks);
      if (out_count != max_row_count && !config.prune) {
			  fade_data[op->id].del_interventions = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * out_count * n_masks);
				code += FilterCodeAndAlloc(config, op, op->lineage_op, fade_data);
        if (config.debug)
          std::cout << "alloc del_interventions " << out_count << " " << n_masks
            << " " << fade_data[op->id].del_interventions << std::endl;
      } else {
          fade_data[op->id].del_interventions = fade_data[op->id].base_target_matrix;
      }
		}
	} else if (op->type == PhysicalOperatorType::FILTER) {
    if (config.prune)
      fade_data[op->id].opid = fade_data[op->children[0]->id].opid;
    else
      fade_data[op->id].opid = op->id;

		fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		idx_t n_masks = fade_data[op->id].n_masks;
		if (n_masks > 0 || fade_data[op->id].n_interventions == 1) {
			idx_t row_count = op->lineage_op->backward_lineage[0].size();
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
    fade_data[op->id].opid = op->id;
		int lhs_n = fade_data[op->children[0]->id].n_interventions;
		int rhs_n = fade_data[op->children[1]->id].n_interventions;
		fade_data[op->id].n_interventions = (lhs_n > 0) ? lhs_n : rhs_n;
		fade_data[op->id].n_masks = (lhs_n > 0) ? fade_data[op->children[0]->id].n_masks : 
      fade_data[op->children[1]->id].n_masks;
		idx_t n_masks = fade_data[op->id].n_masks;
		if (n_masks > 0 || fade_data[op->id].n_interventions == 1) {
			idx_t row_count = op->lineage_op->backward_lineage[0].size();
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
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
    	fade_data[op->id].opid = op->id;
		fade_data[op->id].del_interventions  = fade_data[op->children[0]->id].del_interventions;
		fade_data[op->id].single_del_interventions  = fade_data[op->children[0]->id].single_del_interventions;
		if (config.intervention_type == InterventionType::SCALE_UNIFORM) {
			fade_data[op->id].n_interventions = config.n_intervention;
		} else {
			fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		}
		fade_data[op->id].n_masks = fade_data[op->children[0]->id].n_masks;


		// To support nested agg, check if any descendants is an agg
		fade_data[op->id].has_agg_child = false;
		PhysicalOperator* cur_op = op->children[0].get();
		while (cur_op && !cur_op->children.empty() && !(cur_op->type == PhysicalOperatorType::HASH_GROUP_BY
		                  || cur_op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
		                  || cur_op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE)) {

			cur_op = cur_op->children[0].get();
		}
		if (cur_op->type == PhysicalOperatorType::HASH_GROUP_BY
		     || cur_op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
		     || cur_op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
			fade_data[op->id].has_agg_child=true;
			fade_data[op->id].child_agg_id=cur_op->id;
		}

		if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
			PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
			auto &aggregates = gb->grouped_aggregate_data.aggregates;
			int n_groups = op->lineage_op->log_index->ha_hash_index.size();
			if (fade_data[op->id].has_agg_child) {
			  code += HashAggregateIntervene2DNested(config, op->lineage_op, fade_data, op, aggregates, gb->grouped_aggregate_data.groups.size(), n_groups);
			} else {
			  Fade::GroupByAlloc(config, op->lineage_op, fade_data, op, aggregates, gb->grouped_aggregate_data.groups.size(), n_groups);
			  code += HashAggregateIntervene2D(config, op->lineage_op, fade_data, op, aggregates, gb->grouped_aggregate_data.groups.size(), n_groups);
			}
		} else if (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
			PhysicalPerfectHashAggregate * gb = dynamic_cast<PhysicalPerfectHashAggregate *>(op);
			auto &aggregates = gb->aggregates;
			int n_groups = op->lineage_op->log_index->pha_hash_index.size();
			if (fade_data[op->id].has_agg_child) {
			  code += HashAggregateIntervene2DNested(config, op->lineage_op, fade_data, op, aggregates, gb->groups.size(), n_groups);
			} else {
			  Fade::GroupByAlloc(config, op->lineage_op, fade_data, op, aggregates, gb->groups.size(), n_groups);
			  code += HashAggregateIntervene2D(config, op->lineage_op, fade_data, op, aggregates, gb->groups.size(), n_groups);
			}
		} else {
			PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(op);
			auto &aggregates = gb->aggregates;
			int n_groups = 1;
			if (fade_data[op->id].has_agg_child) {
			  code += HashAggregateIntervene2DNested(config, op->lineage_op, fade_data, op, aggregates, 0, n_groups);
			} else {
			  Fade::GroupByAlloc(config, op->lineage_op, fade_data, op, aggregates, 0, n_groups);
			  code += HashAggregateIntervene2D(config, op->lineage_op, fade_data, op, aggregates, 0, n_groups);
			}
		}

	}  else if (op->type == PhysicalOperatorType::PROJECTION) {
    fade_data[op->id].opid = fade_data[op->children[0]->id].opid;
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

void Intervention2DEval(int thread_id, EvalConfig& config, void* handle, PhysicalOperator* op,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		Intervention2DEval(thread_id, config, handle, op->children[i].get(), fade_data);
	}
    
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		int row_count = op->lineage_op->backward_lineage[0].size();
		int batch_size = row_count / config.num_worker;
		if (row_count % config.num_worker > 0)
			batch_size++;
		int start = thread_id * batch_size;
		int end   = start + batch_size;
		if (end >= row_count) { end = row_count; }

    if (config.prune || fade_data[op->id].base_rows == row_count || !fade_data[op->id].filter_fn) return;
		if (config.n_intervention == 1 && config.incremental == false) {
      int result = fade_data[op->id].filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
                                               fade_data[op->id].base_single_del_interventions,
                                               fade_data[op->id].single_del_interventions,
                                               fade_data[op->id].del_set,
                                               fade_data[op->id].del_set, start, end);
			} else {
      int result = fade_data[op->id].filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
                                               fade_data[op->id].base_target_matrix,
                                               fade_data[op->id].del_interventions,
                                               fade_data[op->id].del_set,
                                               fade_data[op->id].del_set, start, end);
      }
  } else if (op->type == PhysicalOperatorType::FILTER) {
		if (config.prune) return;
		if (fade_data[op->id].n_masks > 0 || fade_data[op->id].n_interventions == 1) {
			idx_t row_count = op->lineage_op->backward_lineage[0].size();
      int batch_size = row_count / config.num_worker;
      if (row_count % config.num_worker > 0)
        batch_size++;
      int start = thread_id * batch_size;
      int end   = start + batch_size;
      if (end >= row_count) { end = row_count; }

      int opid = fade_data[op->children[0]->id].opid;
			if (config.n_intervention == 1 && config.incremental == true) {
				int result = fade_data[op->id].filter_fn(thread_id, op->lineage_op->forward_lineage[0].data(),
				                                         fade_data[op->children[0]->id].single_del_interventions,
				                                         fade_data[op->id].single_del_interventions,
                                                 fade_data[opid].del_set,
				                                         fade_data[op->id].del_set, start, end);
			} else if (config.n_intervention == 1 && config.incremental == false) {
				int result = fade_data[op->id].filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
				                                         fade_data[op->children[0]->id].single_del_interventions,
				                                         fade_data[op->id].single_del_interventions,
                                                 fade_data[opid].del_set,
				                                         fade_data[op->id].del_set, start, end);
			} else {
				int result = fade_data[op->id].filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
				                                         fade_data[op->children[0]->id].del_interventions,
				                                         fade_data[op->id].del_interventions,
				                                         fade_data[opid].del_set,
				                                         fade_data[op->id].del_set, start, end);
			}
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
      int lhs_opid = fade_data[op->children[0]->id].opid;
      int rhs_opid = fade_data[op->children[1]->id].opid;
      
      int row_count = op->lineage_op->backward_lineage[0].size();
      int batch_size = row_count / config.num_worker;
      if (row_count % config.num_worker > 0)
        batch_size++;
      int start = thread_id * batch_size;
      int end   = start + batch_size;
      if (end >= row_count) { end = row_count; }

		if (fade_data[op->id].n_masks > 0 || fade_data[op->id].n_interventions == 1) {
		    idx_t row_count = op->lineage_op->backward_lineage[0].size();
			if (config.n_intervention == 1 && config.incremental == true) {
      //  std::cout << op->id << " Join start " << lhs_opid << " " << rhs_opid << " " << row_count << std::endl;
     //   std::cout << fade_data[op->id].forward_lineage[0].size() << " "
       //   << fade_data[op->id].forward_lineage[1].size() << std::endl;
				int result = fade_data[op->id].join_fn_forward(thread_id, op->lineage_op->forward_lineage_list[0],
				                                       op->lineage_op->forward_lineage_list[1],
				                                       fade_data[lhs_opid].single_del_interventions,
				                                       fade_data[rhs_opid].single_del_interventions,
				                                       fade_data[op->id].single_del_interventions,
				                                       fade_data[lhs_opid].del_set,
				                                       fade_data[rhs_opid].del_set,
				                                       fade_data[op->id].del_set, start, end);
     //   std::cout << "Join end" << std::endl;
			} else if (config.n_intervention == 1 && config.incremental == false) {
				int result = fade_data[op->id].join_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
				                                       op->lineage_op->backward_lineage[1].data(),
				                                       fade_data[lhs_opid].single_del_interventions,
				                                       fade_data[rhs_opid].single_del_interventions,
				                                       fade_data[op->id].single_del_interventions,
					                                   fade_data[lhs_opid].del_set,
					                                   fade_data[rhs_opid].del_set,
					                                   fade_data[op->id].del_set, start, end);
			} else {
        if (config.debug) {
          int rows = op->children[0]->lineage_op->backward_lineage[0].size();
          int n_masks = fade_data[op->id].n_masks;
            vector<int> nonzero_count(n_masks * 16);
            std::cout << op->id << " join summary " << rows << std::endl;
            for (int r=0; r < rows; r++) {
              for (int c=0; c < n_masks; c++)
              for (int k=0; k < 16; ++k) {
                if (fade_data[op->children[0]->id].del_interventions[r * n_masks + c] & (1 << k) )
                  nonzero_count[c*16+k]++;
              }
            }

            std::cout << op->id << " before join : " << std::endl;
            for (int c=0; c < nonzero_count.size(); c++)
              std::cout << "\t " << c << "  -> " << nonzero_count[c] << " ; ";
            std::cout << std::endl;
        }
				int result = fade_data[op->id].join_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
				                                       op->lineage_op->backward_lineage[1].data(),
                                               fade_data[lhs_opid].del_interventions,
				                                       fade_data[rhs_opid].del_interventions, fade_data[op->id].del_interventions,
				                                       fade_data[lhs_opid].del_set,
				                                       fade_data[rhs_opid].del_set,
				                                       fade_data[op->id].del_set, start, end);
        if (config.debug) {
          int rows = op->lineage_op->backward_lineage[0].size();
          int n_masks = fade_data[op->id].n_masks;
            vector<int> nonzero_count(n_masks * 16);
            for (int r=0; r < rows; r++) {
              for (int c=0; c < n_masks; c++)
              for (int k=0; k < 16; ++k) {
                if (fade_data[op->id].del_interventions[r * n_masks + c] & (1 << k) )
                  nonzero_count[c*16+k]++;
              }
            }

            std::cout << op->id << " after join : " << std::endl;
            for (int c=0; c < nonzero_count.size(); c++)
              std::cout << "\t " << c << "  -> " << nonzero_count[c] << " ; ";
            std::cout << std::endl;
        }
      }
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
    int opid = fade_data[op->children[0]->id].opid;
		if (config.n_intervention == 1) {
			HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op, handle,
			                             fade_data[opid].single_del_interventions);
		} else {
      if (config.debug) {
          std::cout << op->id << " agg " << op->lineage_op->forward_lineage[0].size() << " " << opid << std::endl;
          int rows = op->lineage_op->forward_lineage[0].size();
          int n_masks = fade_data[opid].n_masks;
            vector<int> nonzero_count(n_masks * 16);
            for (int r=0; r < rows; r++) {
              for (int c=0; c < n_masks; c++)
              for (int k=0; k < 16; ++k) {
                if (fade_data[opid].del_interventions[r * n_masks + c] & (1 << k) )
                  nonzero_count[c*16+k]++;
              }
            }

            std::cout << op->id << " agg : " << std::endl;
            for (int c=0; c < nonzero_count.size(); c++)
              std::cout << "\t " << c << "  -> " << nonzero_count[c] << " ; ";
            std::cout << std::endl;
      }
			HashAggregateIntervene2DEval(thread_id, config, op->lineage_op, fade_data, op, handle,  fade_data[opid].del_interventions);
		}
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

/*
  1. traverse plan to construct template
  2. compile
  2. traverse plan to bind variables and execute code
*/
// TODO: (3) nested aggregate
// TODO: (2.3) scaling and deletion interventions combined
// TODO: (2.5) change values from 0 to 1 to represent target list
// TODO: (4) add register dependency (execute query, store lineage, generate&compile template, clear up all other memory)
// TODO: (5) batch process interventions (given N interventions, process them B interventions at a time. If K is specified, then keep K interventions at a time.
// TODO: (6) add intervention generation module (1. check if there is any dependency that the current query doesn't satisfy,
//		2. if yes, then use dependency lineage stored to compute them, 3. ..
string Fade::Whatif(PhysicalOperator *op, EvalConfig config) {
	std::cout << op->ToString() << std::endl;
	// timing vars
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;

	// holds any extra data needed during exec
	std::unordered_map<idx_t, FadeDataPerNode> fade_data;

	// 1. Parse Spec = table_name.col:scale
	std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec( config);

	// TODO: run post processing, code gen, and compilation after query execution
    // TODO: combine post process and gen. materialize all lineage as 1D array and free lineage temp data

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


  double intervention_gen_time = 0;
  if (!use_preprep_tm) {
    string fname = "fill_random";
    random_fn = (int(*)(int, int, float, int, void*, std::set<int>&, int, std::vector<__mmask16>&))dlsym(handle, fname.c_str());
    // 3. Prepare base interventions; should be one time cost per DB
    std::vector<std::thread> workers_random;
    start_time = std::chrono::steady_clock::now();
    // using leaf lineage can reduce this to only the tuples used by the final output
    if (config.num_worker > 1) {
      for (int i = 0; i < config.num_worker; ++i) {
        workers_random.emplace_back(GenRandomWhatifIntervention, i, std::ref(config), op, std::ref(fade_data), handle, std::ref(columns_spec));
      }
      // Wait for all tasks to complete
      for (std::thread& worker : workers_random) {
        worker.join();
      }
    } else {
      GenRandomWhatifIntervention(0, config, op, fade_data, handle, columns_spec);
    }
    end_time = std::chrono::steady_clock::now();
    time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
    intervention_gen_time = time_span.count();
  }

  std::cout << "bindfunctions" << std::endl;
	BindFunctions(config, handle,  op, fade_data);

  std::cout << "intervention" << std::endl;
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
		ReleaseFade(config, handle, op, fade_data);
    std::cout <<  result  << std::endl;
    return "select " + to_string(intervention_gen_time) + " as intervention_gen_time, "
           + to_string(prep_time) + " as prep_time, "
           + to_string(compile_time) + " as compile_time, "
           + to_string(eval_time) + " as eval_time, "
           + to_string(rank_time) + " as rank_time";
  } else {
    ReleaseFade(config, handle, op, fade_data);

    return "select " + to_string(intervention_gen_time) + " as intervention_gen_time, "
           + to_string(prep_time) + " as prep_time, "
           + to_string(compile_time) + " as compile_time, "
           + to_string(eval_time) + " as eval_time";
  }
}

} // namespace duckdb
#endif

