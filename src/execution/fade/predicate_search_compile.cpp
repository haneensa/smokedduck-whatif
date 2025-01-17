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


string FilterCodeAndAllocPredicate(EvalConfig& config, PhysicalOperator *op,
    shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid); // + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
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


string JoinCodeAndAllocPredicate(EvalConfig config, PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid); // + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
	std::ostringstream oss;
	oss << R"(extern "C" int )"
	    << fname
	    << R"((int thread_id, int* lhs_lineage, int* rhs_lineage,  void* __restrict__ lhs_var_ptr,
void* __restrict__ rhs_var_ptr,  void* __restrict__ out_ptr, const int start, const int end) {
)";

	oss << "\t int* __restrict__ out = (int* __restrict__)out_ptr;\n";
	oss << "\t int* __restrict__ rhs_var = (int* __restrict__)rhs_var_ptr;\n";
	oss << "\t int* __restrict__ lhs_var = (int* __restrict__)lhs_var_ptr;\n";

	oss << "\tconst int right_n_interventions = " + to_string(fade_data[op->children[1]->id].n_interventions) + ";\n";
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
	string fname = "agg_"+ to_string(opid) + "_" + to_string(config.qid); // + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
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




string HashAggregateCodeAndAllocPredicate(EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                          std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                          PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                                          int keys, int n_groups) {
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
        body_code += get_loop_opening2(config, get_vals_code, pre_vals_code) + eval_code + get_loop_close2(config, post_vals_code);
        get_vals_code = "";
        pre_vals_code = "";
        post_vals_code = "";
        eval_code = "";
      }
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
      body_code += get_loop_opening2(config, get_vals_code, pre_vals_code) + eval_code + get_loop_close2(config, post_vals_code);
      get_vals_code = "";
      pre_vals_code = "";
      post_vals_code = "";
      eval_code = "";
    }
    if (config.is_scalar == false) {
		  get_vals_code += "\t\t\ __m512i one = _mm512_set1_epi32(1);\n";
		  eval_code += get_agg_eval_predicate(config, agg_count++, "count", out_var, "one", "int");
    } else {
		  eval_code += get_agg_eval_predicate(config, agg_count++, "count", out_var, "1", "int");
    }
    pre_vals_code += "\t\tint " + out_var + "_temp = " + out_var  + "[col + row];\n";
    post_vals_code += "\t\t"  + out_var + "[col + row] = " + out_var + "_temp;\n";
	}
  
  if (agg_count > 0 && !eval_code.empty()) {
      body_code += get_loop_opening2(config, get_vals_code, pre_vals_code) + eval_code + get_loop_close2(config, post_vals_code);
      get_vals_code = "";
      pre_vals_code = "";
      post_vals_code = "";
      eval_code = "";
  }


	string init_code = get_agg_init_predicate(config, row_count, op->children[0]->lineage_op->chunk_collection.ChunkCount(), op->id,  fade_data[op->id].n_interventions, "agg", alloc_code, get_data_code, get_vals_code);
	string end_code;

	if (config.use_duckdb) {
		end_code += "\t} \n offset +=  collection_chunk.size();\n}";
	} 

	end_code += Fade::group_partitions(config, fade_data[op->id]);
	end_code +=  "\treturn 0;\n}\n";

	code = init_code + body_code + end_code;

	return code;
}

void GenInterventionPredicate(EvalConfig& config, PhysicalOperator* op,
                              std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                              std::unordered_map<std::string, std::vector<std::string>>& columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenInterventionPredicate(config, op->children[i].get(), fade_data, columns_spec);
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
    string table_name = op->lineage_op->table_name;
    fade_data[op->id].gen = false;
		if (columns_spec.find(table_name) == columns_spec.end()) {
      if (config.debug) std::cout << "skip " << table_name << std::endl;
			return;
		}

    string col_spec = columns_spec[table_name][0];
    if (col_spec.substr(0, 3) == "npy") {
        if (config.debug) std::cout << "Use Pre generated interventions" << std::endl;
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
		    int* temp = new int[rows];
        size_t fbytes = fread(temp, sizeof(int), rows ,  fname);
        if ( fbytes != rows) {
          fprintf(stderr, "read failed");
          exit(EXIT_FAILURE);
        }
			  fade_data[op->id].base_annotations = temp;

			  fade_data[op->id].n_interventions = cols;
        fade_data[op->id].base_rows = rows;
    } else if (config.n_intervention == 0) {
      // TODO: map annotations to predicate
      bool use_factorize = false;
      for (const auto& col : columns_spec[table_name]) {
        std::ifstream rowsfile((table_name + "_" + col + ".rows").c_str());
        int rows, n_interventions;
        if ( rowsfile >> rows >> n_interventions || !(fade_data[op->id].n_interventions > 0 && fade_data[op->id].base_rows != rows) ) {
            std::cout << "annotations card for " << table_name << " " << col << " has " << rows  << " with  " << n_interventions<< std::endl;
        } else {
            std::cerr << "Error opening metadata file or erros in # rows " << fade_data[op->id].n_interventions
            << " " <<fade_data[op->id].base_rows << " " << rows << std::endl;
            use_factorize = true;
            break;
        }

        rowsfile.close();
        fade_data[op->id].base_rows = rows;
        
        FILE * fname = fopen((table_name + "_" + col + ".npy").c_str(), "r");
        if (fname == nullptr) {
          std::cerr << "Error: Unable to open file." << std::endl;
          use_factorize = true;
          break;
        }

        // read the first line to get cardinality
        int* temp = new int[rows];
        size_t fbytes = fread(temp, sizeof(int), rows ,  fname);
        if ( fbytes != rows ) {
          std::cerr << "Error: Unable to open file." << std::endl;
          use_factorize = true;
          break;
        }

        if (fade_data[op->id].n_interventions > 0) {
          for (int i = 0 ; i < rows;  ++i) {
            fade_data[op->id].base_annotations[i] = fade_data[op->id].base_annotations[i] * n_interventions + temp[i];
          }
          fade_data[op->id].n_interventions *= n_interventions;
        } else {
          fade_data[op->id].base_annotations = temp;
          fade_data[op->id].n_interventions = n_interventions;
        }
      }
      if (use_factorize) {
        std::pair<int*, idx_t> res = Fade::factorize(op, op->lineage_op, columns_spec);
        if (config.debug)
          std::cout << " annotations: " << res.second << std::endl;
        fade_data[op->id].base_annotations = res.first;
        fade_data[op->id].n_interventions = res.second;
        fade_data[op->id].base_rows =  op->lineage_op->backward_lineage[0].size();
      }
		} else {
			// random need access to  config.n_intervention
      if (config.debug)
        std::cout << "generate random unique: " << config.n_intervention << std::endl;
			fade_data[op->id].n_interventions = config.n_intervention;
	    //idx_t row_count = op->lineage_op->log_index->table_size;
			fade_data[op->id].base_annotations = Fade::random_unique(op->lineage_op, config.n_intervention);
      fade_data[op->id].base_rows =  op->lineage_op->backward_lineage[0].size();
		}
	}
}


void GenCodeAndAllocPredicate(EvalConfig& config, string& code, PhysicalOperator* op,
                              std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                              std::unordered_map<std::string, std::vector<std::string>>& columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenCodeAndAllocPredicate(config, code, op->children[i].get(), fade_data, columns_spec);
	}

  fade_data[op->id].gen = false;
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
    if (columns_spec.find(op->lineage_op->table_name) == columns_spec.end()) {
      fade_data[op->id].n_interventions = 1;
      return;
    }
    if (fade_data[op->id].base_rows > op->lineage_op->backward_lineage[0].size()) {
      fade_data[op->id].gen = true;
		  code += FilterCodeAndAllocPredicate(config, op, op->lineage_op, fade_data);
    }
    fade_data[op->id].annotations = fade_data[op->id].base_annotations;
  } else if (op->type == PhysicalOperatorType::FILTER) {
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		if ( fade_data[op->id].n_interventions <= 1) {
			return;
		}
    // alloc
		fade_data[op->id].annotations = new int[op->lineage_op->backward_lineage[0].size()];
		if (config.prune == false) {
      fade_data[op->id].gen = true;
		  code += FilterCodeAndAllocPredicate(config, op, op->lineage_op, fade_data);
    }
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		idx_t left_n_interventions =  fade_data[op->children[0]->id].n_interventions;
		idx_t right_n_interventions =  fade_data[op->children[1]->id].n_interventions;
		fade_data[op->id].n_interventions = left_n_interventions * right_n_interventions;
		if (fade_data[op->id].n_interventions <= 1) return;
		fade_data[op->id].annotations = new int[op->lineage_op->backward_lineage[0].size()];
		code += JoinCodeAndAllocPredicate(config, op, op->lineage_op, fade_data);
    fade_data[op->id].gen = true;
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY ||
      op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY ||
	    op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE
      ) {
    fade_data[op->id].gen = true;
		idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		//fade_data[op->id].annotations = new int[row_count];
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
		// get n_groups: max(oid)+1
		fade_data[op->id].n_groups = op->lineage_op->log_index->ha_hash_index.size(); //lop->chunk_collection.Count();
		// if n_groups * n_interventions > rows then use single thread
		int n_threads = config.num_worker;
		if (fade_data[op->id].n_groups * fade_data[op->id].n_interventions  > row_count) {
		  config.num_worker = 1;
		}

		if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		  PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
		  auto &aggregates = gb->grouped_aggregate_data.aggregates;
		  int n_groups = op->lineage_op->log_index->ha_hash_index.size();
		  Fade::GroupByAlloc(config, op->lineage_op, fade_data, op, aggregates, gb->grouped_aggregate_data.groups.size(), n_groups);
		  code += HashAggregateCodeAndAllocPredicate(config, op->lineage_op, fade_data, op, aggregates, gb->grouped_aggregate_data.groups.size(), n_groups);
		} else if (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
		  PhysicalPerfectHashAggregate * gb = dynamic_cast<PhysicalPerfectHashAggregate *>(op);
		  auto &aggregates = gb->aggregates;
		  int n_groups = op->lineage_op->log_index->pha_hash_index.size();
		  Fade::GroupByAlloc(config, op->lineage_op, fade_data, op, aggregates, gb->groups.size(), n_groups);
		  code += HashAggregateCodeAndAllocPredicate(config, op->lineage_op, fade_data, op, aggregates, gb->groups.size(), n_groups);
		} else {
		  PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(op);
		  auto &aggregates = gb->aggregates;
		  int n_groups = 1;
		  Fade::GroupByAlloc(config, op->lineage_op, fade_data, op, aggregates, 0, n_groups);
		  code += HashAggregateCodeAndAllocPredicate(config, op->lineage_op, fade_data, op, aggregates, 0, n_groups);
		}
    config.num_worker = n_threads;
	}  else if (op->type == PhysicalOperatorType::PROJECTION) {
		fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
    fade_data[op->id].annotations = fade_data[op->children[0]->id].annotations;
	}
}

void Intervention2DEvalPredicate(int thread_id, EvalConfig& config, void* handle, PhysicalOperator* op,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		Intervention2DEvalPredicate(thread_id, config, handle, op->children[i].get(), fade_data);
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN || op->type == PhysicalOperatorType::FILTER) {
    if (!fade_data[op->id].gen || fade_data[op->id].n_interventions <= 1) {
      if (op->type == PhysicalOperatorType::FILTER) {
		    fade_data[op->id].annotations = fade_data[op->children[0]->id].annotations;
      } else {
		    fade_data[op->id].annotations = fade_data[op->id].base_annotations;
      }
    } else {
      int row_count = op->lineage_op->backward_lineage[0].size();
      int batch_size = row_count / config.num_worker;
      if (row_count % config.num_worker > 0)
        batch_size++;
      int start = thread_id * batch_size;
      int end   = start + batch_size;
      if (end >= row_count)  end = row_count;
      int* input_annotations = fade_data[op->id].base_annotations;
      if (op->type == PhysicalOperatorType::FILTER) {
        input_annotations = fade_data[op->children[0]->id].annotations;
      }
      int result = fade_data[op->id].filter_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
                                                 input_annotations,
                                                 fade_data[op->id].annotations,
                                                 start, end);
    }
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		if (fade_data[op->id].n_interventions <= 1) return;
		idx_t row_count = op->lineage_op->backward_lineage[0].size();

    int batch_size = row_count / config.num_worker;
    if (row_count % config.num_worker > 0)
      batch_size++;
    int start = thread_id * batch_size;
    int end   = start + batch_size;
    if (end >= row_count) { end = row_count; }
		int result = fade_data[op->id].join_fn(thread_id, op->lineage_op->backward_lineage[0].data(),
		                                       op->lineage_op->backward_lineage[1].data(), fade_data[op->children[0]->id].annotations,
			                                       fade_data[op->children[1]->id].annotations, fade_data[op->id].annotations,
                                            start, end);
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		if (fade_data[op->id].n_interventions <= 1) return;
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
      if (end >= row_count) { end = row_count; }
    }
    if (config.debug)
      std::cout << "Count summary: " << row_count << " " << chunk_count << " " << batch_size << " " << start << " " << end << " " << config.num_worker << " " << thread_id << std::endl;
		if (config.use_duckdb) {
			int result = fade_data[op->id].agg_duckdb_fn(thread_id, op->lineage_op->forward_lineage[0].data(),
			                                             fade_data[op->children[0]->id].annotations, fade_data[op->id].alloc_vars,
			                                             op->children[0]->lineage_op->chunk_collection,
                                                   start, end);
		} else {
			int result = fade_data[op->id].agg_fn(thread_id, op->lineage_op->forward_lineage[0].data(),
			                                      fade_data[op->children[0]->id].annotations, fade_data[op->id].alloc_vars,
			                                      fade_data[op->id].input_data_map, 
                                            start, end);
		}
	} else if (op->type == PhysicalOperatorType::PROJECTION) {
    // TODO: fix pass pointer instead of copying
		fade_data[op->id].annotations = fade_data[op->children[0]->id].annotations;
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
	std::unordered_map<idx_t, FadeDataPerNode> fade_data;
	
  std::cout << "gen intervention" << std::endl;
  start_time = std::chrono::steady_clock::now();
	GenInterventionPredicate(config, op, fade_data, columns_spec);
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
		workers.emplace_back(Intervention2DEvalPredicate, i, std::ref(config), handle,  op, std::ref(fade_data));
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


  config.topk=10;
	if (config.topk > 0) {
		// rank final agg result
		std::vector<int> topk_vec = rank(op, config, fade_data);
		std::string result = std::accumulate(topk_vec.begin(), topk_vec.end(), std::string{},
		                                     [](const std::string& a, int b) {
			                                     return a.empty() ? std::to_string(b) : a + "," + std::to_string(b);
		                                     });
		ReleaseFade(config, handle, op, fade_data);
    std::cout << result << std::endl;
		// return "select '" + result + "'";
		return "select " + to_string(gen_time) + " as intervention_gen_time, "
				   + to_string(prep_time) + " as prep_time, "
				   + to_string(compile_time) + " as compile_time, "
           + to_string(0) + " as code_gen_time, "
           + to_string(data_time) + " as data_time, "
				   + to_string(eval_time) + " as eval_time";
	} else {
	  ReleaseFade(config, handle, op, fade_data);
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
