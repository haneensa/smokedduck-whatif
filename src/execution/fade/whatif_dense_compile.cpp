#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include <fstream>
#include <dlfcn.h>
#include <immintrin.h>
#include <thread>
#include <vector>

namespace duckdb {

string fill_random_code(EvalConfig& config) {
	std::ostringstream oss;
	oss << R"(
extern "C" int fill_random(int thread_id, int row_count, float prob, int n_masks, void* del_interventions_ptr, int rand_count, std::vector<__mmask16>& base) {
  int count = 0;
)";
	if (config.n_intervention == 1) {
		oss << "\tint8_t* __restrict__ del_interventions = (int8_t* __restrict__)del_interventions_ptr;\n";
	} else {
		oss << "\t__mmask16* __restrict__ del_interventions = (__mmask16* __restrict__)del_interventions_ptr;\n";
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
	//std::mt19937 gen(42);
	std::mt19937 gen(rd());
	std::uniform_int_distribution<int> dist(0, rand_count);
)";
		config.rand_base.resize(config.rand_count);
		for (int i = 0; i < config.rand_count; ++i) {
			config.rand_base[i] = 0x0;
			for (int k = 0; k < 16; ++k) {
				if ((((double)rand() / RAND_MAX) < config.probability)) {
					config.rand_base[i] |= (1 << k);
				}
			}
		}
  }
	oss << "\n\tfor (int i = start; i < end; ++i) {";
	if (config.n_intervention == 1) {
		oss << "\n\t\tdel_interventions[i] = (((double)rand() / RAND_MAX) < prob); \n\t";
    if (config.debug) {
      oss << "\n\tcount += del_interventions[i];\n";
    }
    oss << "\n\t}";
	} else {
		oss << R"(
		for (int j = 0; j < n_masks; ++j) {
		  int r = dist(gen);
		  del_interventions[i*n_masks+j] = base[r];
		}
	}
)";
	}

  if (config.debug) {
    oss << R"(
	std::cout << thread_id << " random done-> start: "  << start << ", end: " << end <<  ", count: " << count << std::endl;
)";
  }
	oss << R"(
	return 0;
}
)";
	return oss.str();
}

// ADD get_agg_init for SCALE_UNIFORM which doesn't use any interventions below aggregates
string get_agg_init_no_intervention(EvalConfig& config, int total_agg_count, int row_count,
                                    int chunk_count, int opid, int n_interventions, string fn, string alloc_code,
                                    string get_data_code) {
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


	return oss.str();
}

string get_agg_init(EvalConfig& config, int total_agg_count, int row_count, int chunk_count, int opid, int n_interventions, string fn, string alloc_code,
                    string get_data_code) {
	int n_masks = n_interventions / config.mask_size;
	string fname = "agg_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	if (config.use_duckdb) {
			oss << R"(extern "C" int )"
			    << fname
			    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
duckdb::ChunkCollection &chunk_collection, const int start, const int end) {
)";
	} else if (config.use_gb_backward_lineage) {
		oss << R"(extern "C" int bw_)"
		    << fname
		    << R"((int thread_id, std::vector<std::vector<int>> lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
              std::unordered_map<int, void*>& input_data_map) {
)";
  } else {
		oss << R"(extern "C" int )"
		    << fname
		    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
              std::unordered_map<int, void*>& input_data_map, const int start, const int end) {
)";
	}


	if (config.n_intervention == 1) {
		oss << "\tint8_t* __restrict__ var_0 = (int8_t* __restrict__)var_0_ptr;\n";
	} else {
		oss << "\t__mmask16* __restrict__ var_0 = (__mmask16* __restrict__)var_0_ptr;\n";
	}

	oss << "\tconst int mask_size = " << config.mask_size << ";\n";
	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";
	oss << "\tconst int n_masks  = " << n_masks << ";\n";
	oss << "\tconst int num_threads  = " << config.num_worker << ";\n";
  oss << "\tint sf = 0.8;\n";

  if (config.debug) {
    if (config.use_gb_backward_lineage)
      oss << "\tstd::cout << \"Specs: \"  << \" \" << mask_size << \" \" << n_interventions << \" \" << n_masks << \" \" << lineage.size()  << std::endl;";
    else
      oss << "\tstd::cout << \"Specs: \"  << \" \" << mask_size << \" \" << n_interventions << \" \" << n_masks << \" \" << start << \" \" << end << std::endl;";
  }

	oss << alloc_code;
  
  if (config.use_duckdb == false)
    oss << get_data_code;

  if (config.use_duckdb) {
    oss << R"(
  int offset = 0;
  for (int chunk_idx=start; chunk_idx < end; ++chunk_idx) {
    duckdb::DataChunk &collection_chunk = chunk_collection.GetChunk(chunk_idx);
)";
  oss << get_data_code;
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

	if (fn == "count" && (config.intervention_type == InterventionType::SCALE_RANDOM ||
	                             config.intervention_type == InterventionType::SCALE_UNIFORM)) {
	} else {
		if (fn == "sum") {
			oss << "\t\t";
			oss << out_var+"[oid] +="+ in_arr + "[i]";
		} else if (fn == "count") {
			oss << "\t\t";
     // oss << "\nstd::cout <<i << \" \" <<  var_0[i] <<  \" \" << out_count[oid] << std::endl;";
			oss << "out_count[oid] += 1";
		}

		// if there are more than one multiples, then iterate over multiples[] array
		if (config.intervention_type == InterventionType::SCALE_RANDOM) {
			if (config.use_duckdb) {
				oss << " * (sf * var_0[i+offset] + 1);\n";
			} else {
				oss << " * (sf * var_0[i] + 1);\n";
			}
		} else if (config.intervention_type == InterventionType::SCALE_UNIFORM) {
			oss << "* 1.8;\n";
		} else if (/*total_agg_count > 1 ||*/ config.intervention_type == InterventionType::SCALE_UNIFORM) {
				oss << ";\n";
		} else {
			if (config.use_duckdb) {
				oss << " * !var_0[i+offset];\n";
			} else {
				oss << " * !var_0[i];\n";
			}
		}
	}


	return oss.str();
}

string get_loop_close(EvalConfig& config) {
	std::ostringstream oss;
  if (config.n_intervention != 1) {
    if (config.is_scalar) {
      oss << "\n\t\t}\n"; // close for (int k=0; k < mask_size; k++)
    }
    oss << "\n\t\t\t}\n"; // close for (int j=0; j < n_masks; ++j)
                          //
  }
  oss << "\n\t\t}\n"; // close for (int i=start; .. end) or for (oid ..
  if (config.use_gb_backward_lineage) {
    oss << "\n\t}\n"; // close for (int iid
  }
	return oss.str();
}

string get_loop_opening(EvalConfig& config, string get_vals) {
	std::ostringstream oss;
  if (config.use_duckdb) {
    oss << R"(
      for (int i=0; i < collection_chunk.size(); ++i) {
      int oid = lineage[i+offset];
  )";
    if (config.n_intervention > 1)
      oss << "\tint col = oid*n_interventions;\n";
  } else if (config.use_gb_backward_lineage) {
    oss << R"(
  // for each oid
  for (int oid=0; oid < lineage.size(); ++oid) {
      // see if this oid is assigned to this worker
      if (thread_id != oid % num_threads) continue;
)";
    if (config.n_intervention > 1)
      oss << "\tint col = oid*n_interventions;\n";
    oss << R"(
      for (int iid=0; iid < lineage[oid].size(); ++iid) {
        int i = lineage[oid][iid];
)";
  } else {
    oss << R"(
  for (int i=start; i < end; ++i) {
     int oid = lineage[i];
)";
    if (config.n_intervention > 1)
      oss << "\tint col = oid*n_interventions;\n";
	}

  if (config.n_intervention == 1)
	  return oss.str();
  
  oss << get_vals;

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
	return oss.str();
}

string get_batch_agg_template(EvalConfig& config, int total_agg_count, int agg_count, string fn,
    string out_var="", string in_var="", string data_type="int") {
	std::ostringstream oss;

	if (config.is_scalar) {
		if (config.intervention_type == InterventionType::SCALE_RANDOM) {
			string var = "(sf * (1 &  tmp_mask >> k) + 1)";
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


string get_agg_eval(EvalConfig& config, int total_agg_count, int agg_count, string fn, string out_var="",
    string in_var="", string in_arr="", string data_type="int") {
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
		oss << get_batch_agg_template(config, total_agg_count, agg_count, fn, out_var, in_var, data_type);
	}

	return oss.str();
}

string get_batch_join_template(EvalConfig &config, int n_left_n_masks, int n_right_n_masks) {
	std::ostringstream oss;
	if (config.is_scalar || config.n_intervention < 512) {
		oss << R"(
      int lhs_col = lhs_lineage[i] * n_masks;
      int rhs_col = rhs_lineage[i] * n_masks;
      int col = i*n_masks;
      for (int j=0; j < n_masks; ++j) {
)";
		if ( n_left_n_masks > 0 && n_right_n_masks > 0) {
			oss << R"(out[col+j] = lhs_var[lhs_col+j] | rhs_var[rhs_col+j];)";
		} else if (n_left_n_masks > 0) {
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
		if ( n_left_n_masks > 0 && n_right_n_masks > 0) {
			oss << R"(
		__m512i a = _mm512_stream_load_si512((__m512i*)&lhs_var[lhs_col+j]);
		__m512i b = _mm512_stream_load_si512((__m512i*)&rhs_var[rhs_col+j]);
		_mm512_store_si512((__m512i*)&out[col+j], _mm512_or_si512(a, b));
)";
		} else if (n_left_n_masks > 0) {
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

string get_single_join_template(EvalConfig &config, int n_left_interventions, int int_right_interventions) {
	std::ostringstream oss;

	if ( n_left_interventions > 0 && int_right_interventions > 0) {
		oss << "\t\tout[i] = lhs_var[lhs_lineage[i]] * rhs_var[rhs_lineage[i]];\n";
	} else if (n_left_interventions > 0) {
		oss << "\t\tout[i] = lhs_var[lhs_lineage[i]];\n";
	} else {
		oss << "\t\tout[i] = rhs_var[rhs_lineage[i]];\n";
	}

	return oss.str();
}
string GetJoinCode(EvalConfig& config, int opid, int n_left_interventions, int int_right_interventions,
                   int n_left_n_masks, int n_right_n_masks) {
	int n_masks = config.n_intervention / config.mask_size;
	string fname = "join_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
  oss << R"(extern "C" int )"
        << fname
        << R"((int thread_id, int* __restrict__ lhs_lineage, int* __restrict__  rhs_lineage,
        void* __restrict__ lhs_var_ptr,  void* __restrict__ rhs_var_ptr,  void* __restrict__ out_ptr, const int start, const int end) {
)";

	// 	output_annotation.insert(side_forward_lineage[input id])

	if (config.n_intervention == 1) {
		oss << "\tint8_t* __restrict__ out = (int8_t* __restrict__)out_ptr;\n";
		oss << "\tint8_t* __restrict__ rhs_var = (int8_t* __restrict__)rhs_var_ptr;\n";
		oss << "\tint8_t* __restrict__ lhs_var = (int8_t* __restrict__)lhs_var_ptr;\n";
	} else {
		oss << "\t__mmask16* __restrict__ out = (__mmask16* __restrict__)out_ptr;\n";
		oss << "\t__mmask16* __restrict__ rhs_var = (__mmask16* __restrict__)rhs_var_ptr;\n";
		oss << "\t__mmask16* __restrict__ lhs_var = (__mmask16* __restrict__)lhs_var_ptr;\n";
	}

  string n_masks_str = to_string(n_masks);

  if (config.debug)
	  oss << "\tstd::cout << \"JOIN -> start: \" << \" \" << start << \", end: \" << end << \", n_masks: " << n_masks_str << " \" << std::endl;";

	oss << "\n\tfor (int i=start; i < end; ++i) {\n";

	if (config.n_intervention == 1) {
		oss << get_single_join_template(config, n_left_interventions, int_right_interventions);
	} else {
    oss << "\tconst int n_masks = " + n_masks_str + ";\n";
		oss << get_batch_join_template(config, n_left_n_masks, n_right_n_masks);
	}

	if (config.num_worker > 1) {
		oss << "\n\t}\n\tsync_point.arrive_and_wait();\n\treturn 0; \n}\n";
	} else {
		oss << "\n\t}\n\treturn 0; \n}\n";
	}

	return oss.str();
}


string GetFilterCode(EvalConfig& config, int opid) {
	int n_masks = config.n_intervention / config.mask_size;
	string fname = "filter_"+ to_string(opid) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
	    << fname
	    << R"((int thread_id, int* __restrict__ lineage,  void* __restrict__ var_ptr, void* __restrict__ out_ptr, 
  const int start, const int end) {
)";

	if (config.n_intervention == 1) {
		oss << "\tint8_t* __restrict__ out = (int8_t* __restrict__)out_ptr;\n";
		oss << "\tint8_t* __restrict__ var = (int8_t* __restrict__)var_ptr;\n";
	} else {
		oss << "\t__mmask16* __restrict__ out = (__mmask16* __restrict__)out_ptr;\n";
		oss << "\t__mmask16* __restrict__ var = (__mmask16* __restrict__)var_ptr;\n";
	}


  string n_masks_str = to_string(n_masks);

	if (config.debug)
		oss << "\tstd::cout << \"Filter->  start: \" << start << \", end: \" << end  << \", n_masks: "  << n_masks_str << " \" << std::endl;\n";
	
  if (config.n_intervention > 1)
    oss << "\tconst int n_masks = " + n_masks_str + ";\n";

	if (config.n_intervention == 1) {
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
		oss << "\tsync_point.arrive_and_wait();\n\t return 0; \n}\n\n";
	} else {
		oss << "\treturn 0; \n}\n\n";
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
                                std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                                PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                                int keys) {
	const int n_interventions = fade_data[op->id]->n_interventions;
	int child_agg_id = fade_data[op->id]->child_agg_id;
	string fname = "agg_" + to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) + "_" +
	               to_string(config.is_scalar);

	string code;
	// TODO: need to access the key
	string in_arr = "col_0";
	string input_type = fade_data[child_agg_id]->alloc_vars_types["out_0"];
	string get_data_code = "\t\t" + input_type + "* " + in_arr + " =(" + input_type + " *)input_data_map[\"out_0\"][0];\n";

	// level 2: select count(key),  sum(key*out0), sum(key), sum(out0), sum(key*key)

	// alloc for count
	string out_var = "out_count";
	fade_data[op->id]->alloc_vars[out_var].resize(config.num_worker);
	for (int t = 0; t < config.num_worker; ++t) {
		fade_data[op->id]->allocate_agg_output<int>("int", t, n_interventions, out_var);
	}
	fade_data[op->id]->alloc_vars_index[out_var] = -1;

	// alloc for 4 sums
	for (int i = 0; i < 4; ++i) {
		int interventions_count = n_interventions;
		if (i < 2) {
			interventions_count = 1;
		}
		out_var = "sum_" + to_string(i);
		fade_data[op->id]->alloc_vars[out_var].resize(config.num_worker);
		for (int t = 0; t < config.num_worker; ++t) {
			if (input_type == "int") {
				fade_data[op->id]->allocate_agg_output<int>("int", t, interventions_count, out_var);
			} else {
				fade_data[op->id]->allocate_agg_output<float>("float", t, interventions_count, out_var);
			}
		}
		fade_data[op->id]->alloc_vars_index[out_var] = i;
	}

	std::ostringstream oss;
	oss << R"(extern "C" int )" << fname
	    << R"((int thread_id, int* lineage, void* __restrict__ var_0_ptr, std::unordered_map<std::string, std::vector<void*>>& alloc_vars,
             std::unordered_map<std::string, std::vector<void*>>& input_data_map,const int start, const int end) {
)";

	int n_masks = n_interventions / config.mask_size;

	oss << "\tconst int n_interventions  = " << n_interventions << ";\n";
	oss << "\tconst int num_threads  = " << config.num_worker << ";\n";
	oss << "\tconst int n_masks  = " << n_masks << ";\n";
	oss << "\tconst int mask_size  = " << config.mask_size << ";\n";

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
                                std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                                PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                                int keys) {
	string eval_code;
	string body_code;
	string code;
	string alloc_code;
	string get_data_code;
	string get_vals_code;

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	bool include_count = false;
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
      if (agg_count > 0 && agg_count % config.batch == 0) {
        body_code += get_loop_opening(config, get_vals_code) + eval_code + get_loop_close(config);
        get_vals_code = "";
        eval_code = "";
      }
			int col_idx = i + keys;
			string out_var = "out_" + to_string(i); // new output
			string in_arr = "col_" + to_string(i);  // input arrays
			string in_val = "val_" + to_string(i);  // input values val = col_x[i]

			string input_type = fade_data[op->id]->alloc_vars_types[out_var];
			string output_type = fade_data[op->id]->alloc_vars_types[out_var];

			if (config.use_duckdb) {
				// use CollectionChunk that stores input data
				get_data_code += "\t"+input_type+"* " + in_arr + " = reinterpret_cast<"+input_type+" *>(collection_chunk.data[" +
				                 to_string(col_idx) + "].GetData());\n";
			} else {
				// use unordered_map<int, void*> that stores pointers to input data
				get_data_code += "\t"+input_type+"* " + in_arr + " = reinterpret_cast<"+input_type+" *>(input_data_map[" +
				                 to_string(i) + "]);\n";
			}

			get_vals_code += "\t\t\t" + output_type +" " + in_val + "= " + in_arr + "[i];\n";
      if (config.intervention_type == InterventionType::SCALE_RANDOM) {
        if (output_type == "int") {
			    get_vals_code += "\t\t\t__m512i vec_"  + in_val + " = _mm512_set1_epi32(" + in_val + ");\n";
			    get_vals_code += "\t\t\t__m512i x_vec_"  + in_val + " = _mm512_set1_epi32(" + in_val + "*(sf+1));\n";
        } else {
			    get_vals_code += "\t\t\t__m512 x_vec_"  + in_val + " = _mm512_set1_ps(" + in_val + ");\n";
			    get_vals_code += "\t\t\t__m512 vec_"  + in_val + " = _mm512_set1_ps(" + in_val + "*(sf+1));\n";
        }
      }

			// access output arrays
			alloc_code += Fade::get_agg_alloc(config, i, "sum", output_type);

			// core agg operation
			eval_code += get_agg_eval(config, aggregates.size(), agg_count++, "sum", out_var, in_val,  in_arr, output_type);
		}
	}

	if (include_count == true) {
		string out_var = "out_count";
    if (config.intervention_type == InterventionType::SCALE_RANDOM) {
      get_vals_code += "\t\t\t__m512i vec_1 = _mm512_set1_epi32(1);\n";
      get_vals_code += "\t\t\t__m512i x_vec_1 = _mm512_set1_epi32(sf+1);\n";
    }
		alloc_code += Fade::get_agg_alloc(config, 0, "count", "int");
    if (agg_count > 0 && agg_count % config.batch == 0) {
      body_code += get_loop_opening(config, get_vals_code) + eval_code + get_loop_close(config);
      get_vals_code = "";
      eval_code = "";
    }
		eval_code += get_agg_eval(config, aggregates.size(), agg_count++, "count", out_var, "1", "", "int");
	}
    
  if (agg_count > 0 && !eval_code.empty()) {
      body_code += get_loop_opening(config, get_vals_code) + eval_code + get_loop_close(config);
      get_vals_code = "";
      eval_code = "";
  }

	string init_code;
	if (config.intervention_type == InterventionType::SCALE_UNIFORM) {
		init_code = get_agg_init_no_intervention(config, aggregates.size(), row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(),
		                                         op->id,  fade_data[op->id]->n_interventions, "agg", alloc_code, get_data_code);
	} else {
		init_code = get_agg_init(config, aggregates.size(), row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(), op->id,
		                         fade_data[op->id]->n_interventions, "agg", alloc_code, get_data_code);
	}
	string end_code = Fade::get_agg_finalize(config, fade_data[op->id]);

	code = init_code + body_code + end_code;

	return code;
}

void GenCode(EvalConfig& config, string& code, PhysicalOperator* op,
                    std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                     std::unordered_map<std::string, std::vector<std::string>>& spec) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		GenCode(config, code, op->children[i].get(), fade_data, spec);
	}

	FadeNodeDenseCompile* cur_node = dynamic_cast<FadeNodeDenseCompile*>(fade_data[op->id].get());

	// two cases for scaling intervention: 1) scaling with selection vector (SCALE_RANDOM), 2) uniform scaling (SCALE_UNIFORM)
	// uniform: only allocate memory for the aggregate results
	// random: allocate single intervention/selection vector per table with 0s/1s with prob
	if ( cur_node->gen && (op->type == PhysicalOperatorType::TABLE_SCAN ||
      op->type == PhysicalOperatorType::FILTER)) {
	  code += GetFilterCode(config, op->id);
	} else if (cur_node->gen && (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT)) {
	  FadeNodeDenseCompile* lnode = dynamic_cast<FadeNodeDenseCompile*>(fade_data[op->children[0]->id].get());
	  FadeNodeDenseCompile* rnode = dynamic_cast<FadeNodeDenseCompile*>(fade_data[op->children[1]->id].get());
	  code += GetJoinCode(config, op->id, lnode->n_interventions, rnode->n_interventions, lnode->n_masks, rnode->n_masks);
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		// To support nested agg, check if any descendants is an agg
		if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
			PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
			auto &aggregates = gb->grouped_aggregate_data.aggregates;
			if (fade_data[op->id]->has_agg_child) {
			  code += HashAggregateIntervene2DNested(config, op->lineage_op, fade_data, op, aggregates, gb->grouped_aggregate_data.groups.size());
			} else {
			  code += HashAggregateIntervene2D(config, op->lineage_op, fade_data, op, aggregates, gb->grouped_aggregate_data.groups.size());
			}
		} else if (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
			PhysicalPerfectHashAggregate * gb = dynamic_cast<PhysicalPerfectHashAggregate *>(op);
			auto &aggregates = gb->aggregates;
			if (fade_data[op->id]->has_agg_child) {
			  code += HashAggregateIntervene2DNested(config, op->lineage_op, fade_data, op, aggregates, gb->groups.size());
			} else {
			  code += HashAggregateIntervene2D(config, op->lineage_op, fade_data, op, aggregates, gb->groups.size());
			}
		} else {
			PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(op);
			auto &aggregates = gb->aggregates;
			if (fade_data[op->id]->has_agg_child) {
			  code += HashAggregateIntervene2DNested(config, op->lineage_op, fade_data, op, aggregates, 0);
			} else {
			  code += HashAggregateIntervene2D(config, op->lineage_op, fade_data, op, aggregates, 0);
			}
		}
	}
}

/*
  1. traverse plan to construct template
  2. compile
  2. traverse plan to bind variables and execute code
*/
// TODO: run post processing, code gen, and compilation after query execution
string Fade::Whatif(PhysicalOperator *op, EvalConfig config) {
	std::cout << op->ToString() << std::endl;
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;
	std::unordered_map<idx_t, unique_ptr<FadeNode>> fade_data;
	std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec( config);

  	Fade::Clear(op);

	start_time = std::chrono::steady_clock::now();

	if (config.n_intervention == 1) {
		Fade::AllocSingle(config, op, fade_data, columns_spec);
	} else {
		Fade::AllocDense(config, op, fade_data, columns_spec);
	}

	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double prep_time = time_span.count();
  	start_time = std::chrono::steady_clock::now();
	Fade::GetCachedData(config, op, fade_data, columns_spec);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double data_time = time_span.count();
	
  	string code;
	start_time = std::chrono::steady_clock::now();
	GenCode(config, code, op, fade_data, columns_spec);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double code_gen_time = time_span.count();

	std::ostringstream oss;
	oss << get_header(config) << "\n" << fill_random_code(config) << "\n"  << code;
	string final_code = oss.str();

	if (true || config.debug) std::cout << "compile: " << final_code << std::endl;

	start_time = std::chrono::steady_clock::now();
	void* handle = Fade::compile(final_code, 0);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double compile_time = time_span.count();

	if (handle == nullptr) return "select 0";

	double intervention_gen_time = 0;
	if (!config.use_preprep_tm) {
		std::cout << "fill random: " << std::endl;
		string fname = "fill_random";
		fade_random_fn = (int(*)(int, int, float, int, void*, int, std::vector<__mmask16>&))dlsym(handle, fname.c_str());
		// 3. Prepare base interventions; should be one time cost per DB
    	std::vector<std::thread> workers_random;
    	start_time = std::chrono::steady_clock::now();
    	// using leaf lineage can reduce this to only the tuples used by the final output
    	for (int i = 0; i < config.num_worker; ++i) {
			workers_random.emplace_back(Fade::GenRandomWhatifIntervention, i, std::ref(config), op, std::ref(fade_data), std::ref(columns_spec), true);
		}
		// Wait for all tasks to complete
		for (std::thread& worker : workers_random) {
			worker.join();
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
	for (int i = 0; i < config.num_worker; ++i) {
		workers.emplace_back(Fade::Intervention2DEval, i, std::ref(config),  op, std::ref(fade_data), true);
	}
	// Wait for all tasks to complete
	for (std::thread& worker : workers) {
		worker.join();
	}

	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double eval_time = time_span.count();

	if (dlclose(handle) != 0) {
		std::cerr << "Error: %s\n" << dlerror() << std::endl;
	}

	system("rm loop.cpp loop.so");
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
           + to_string(compile_time) + " as compile_time, "
           + to_string(code_gen_time) + " as code_gen_time, "
           + to_string(data_time) + " as data_time, "
           + to_string(eval_time) + " as eval_time, "
           + to_string(rank_time) + " as rank_time";
  } else {
    return "select " + to_string(intervention_gen_time) + " as intervention_gen_time, "
           + to_string(prep_time) + " as prep_time, "
           + to_string(compile_time) + " as compile_time, "
           + to_string(code_gen_time) + " as code_gen_time, "
           + to_string(data_time) + " as data_time, "
           + to_string(eval_time) + " as eval_time";
  }
}

} // namespace duckdb
#endif

