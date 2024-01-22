#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include <fstream>

/*
 *           j1    j2    j3  ...    j64
       +---------------------------------
  oid1  |   v11   v21   v31  ...   v641
  oid2  |   v12   v22   v32  ...   v642
  oid3  |   v13   v23   v33  ...   v643
  ...   |   ...   ...   ...  ...   ...
  oidN  |   v1N   v2N   v3N  ...   v64N
*/


namespace duckdb {

idx_t mask_size = 16;

string compile(string code, idx_t id) {
	// Write the loop code to a temporary file
	string program_name = "temp" + to_string(id);
	std::ofstream file(program_name + ".cpp");
	file << code;
	file.close();
	// Compile the temporary file
	system(("g++ -O3 " + program_name + ".cpp -o " + program_name).c_str());
	return program_name;
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
static void test_loop(idx_t row_count, idx_t child_n_masks2, idx_t n_groups,
    std::vector<idx_t> &lineage,
    std::vector<std::vector<float>> &new_vals,
    std::vector<T> input_values, __mmask16* child_del_interventions
    ) {

  /*
  idx_t child_n_masks = 64;
  for (idx_t i=0; i < row_count; ++i) {
    idx_t oid = lineage[i];
    T val = input_values[i];
    for (idx_t j=0; j < child_n_masks; j++) {
      int16_t randomValue =  child_del_interventions[i*child_n_masks+j];
      int j64 = j * 16;
      for (idx_t k=0; k < 16; k++) {
        new_vals[oid][j64 + k] += val * (1 &  randomValue >> k);
      }
    }
  }*/
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
    
  std::string loopCode = R"(
	  #include <iostream>
    #include <vector>
	  int main() {
      int row_count = 5916591;
      int child_n_masks = 64;
      int n_groups = 4;
      std::vector<int> lineage(row_count, 0);
      std::vector<std::vector<float>> new_vals(n_groups, std::vector<float> (child_n_masks*16, 0));
      for (int i=0; i < row_count; ++i) {
        int oid = lineage[i];
        //T val = input_values[i];
        for (int j=0; j < child_n_masks; j++) {
          int16_t randomValue = 10; // child_del_interventions[i*child_n_masks+j];
          int j64 = j * 16;
          for (int k=0; k < 16; k++) {
              int del = (1 &  randomValue >> k);
            new_vals[oid][j64 + k] += 1 * del;
          }
        }
      }
      std::cout << "done " << std::endl;

		  return 0;
	  }
  )";
  string out = compile(loopCode, 0);

  start_time = std::chrono::steady_clock::now();
  // Run the compiled program
  system(("./"+out).c_str());
  //test_loop<T>(row_count, child_n_masks, n_groups, lineage, new_vals, input_values, child_del_interventions);
  end_time = std::chrono::steady_clock::now();
  time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  execution_time = time_span.count();

  // Remove the temporary files
  system(("rm "+out+".cpp "+out).c_str());
  std::cout << "phase 3: " << execution_time << std::endl;
	return new_vals2;
}

template<class T>
vector<vector<T>> CountRecompute2D(PhysicalOperator* op,
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

	idx_t child_n_masks = fade_data[op->children[0]->id].n_masks;
	__mmask16* child_del_interventions = fade_data[op->children[0]->id].del_interventions;
	//vector<vector<T>> new_vals(n_interventions, vector<T> (n_groups, 0));
	vector<vector<T>> new_vals(n_groups, vector<T> (child_n_masks*mask_size, 0));

	if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		    idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		    for (idx_t i=0; i < row_count; ++i) {
			    for (idx_t j=0; j < child_n_masks; j++) {
				    __mmask16 randomValue = child_del_interventions[i*child_n_masks+j];
				    for (idx_t k=0; k < mask_size; k++) {
					    int del = (1 &  randomValue >> k);
					    new_vals[0][j*mask_size + k] += 1 * del;
				    }
			    }
		    }
	}

	if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		    return new_vals;
	}

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
			    for (idx_t j=0; j < child_n_masks; j++) {
				    __mmask16 randomValue = child_del_interventions[iid*child_n_masks+j];
				    for (idx_t k=0; k < mask_size; k++) {
					    int del = (1 &  randomValue >> k);
					    new_vals[oid][j*mask_size + k] += 1 * del;
				    }
			    }
		    }
	} while (cache_on || result.size() > 0);
	return new_vals;
}

void  HashAggregateIntervene2D(shared_ptr<OperatorLineage> lop,
                            std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                            PhysicalOperator* op) {
	PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
	auto &aggregates = gb->grouped_aggregate_data.aggregates;
	vector<pair<idx_t, idx_t>> aggregate_input_idx;

	// get n_groups: max(oid)+1
	idx_t n_groups = 1;
	if (op->type != PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		    n_groups = lop->chunk_collection.Count();
	}

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
		    if (name == "sum" || name == "sum_no_overflow") {
		      vector<vector<float>> new_vals;
			    // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
			    new_vals = SumRecompute2D<float>(op, fade_data, lop, aggr,
			                                 fade_data[op->id].n_interventions, n_groups,
			                                 aggregate_input_idx);
		    } else if (name == "count" || name == "count_star") {
		      vector<vector<int>> new_vals;
			    // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
			    new_vals = CountRecompute2D<int>(op, fade_data, lop, aggr,
			                                   fade_data[op->id].n_interventions, n_groups,
			                                   aggregate_input_idx);
		    } else if (name == "avg") {
			    vector<vector<int>> new_vals_count = CountRecompute2D<int>(op, fade_data, lop, aggr,
			                                                             fade_data[op->id].n_interventions,
			                                                             n_groups, aggregate_input_idx);
			    vector<vector<float>> new_vals_sum = SumRecompute2D<float>(op, fade_data, lop, aggr,
			                                                         fade_data[op->id].n_interventions,
			                                                         n_groups, aggregate_input_idx);
		    }
		    // rank and discard?
	}
}

void Intervention2D(PhysicalOperator* op,
                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                  std::unordered_map<std::string, float> columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		Intervention2D(op->children[i].get(), fade_data, columns_spec);
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
		HashAggregateIntervene2D(op->lineage_op, fade_data, op);
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

  // TODO: add pass to allocate data structures for interventions

  // 4. run intervention
  // traverse query plan bottom up. for each one, ...
  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
  Intervention2D(op, fade_data, columns_spec);
  std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
  std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double execution_time = time_span.count();
  std::cout << "INTERVENTION_TIME : " << execution_time << std::endl;

}


} // namespace duckdb
#endif
