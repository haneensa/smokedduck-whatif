#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

std::unordered_map<std::string, std::vector<std::string>> parseString(const std::string& input) {
	std::unordered_map<std::string, std::vector<std::string>> result;
	std::istringstream iss(input);
	std::string token;

	while (std::getline(iss, token, '|')) {
		std::istringstream tokenStream(token);
		std::string table, column;

		if (std::getline(tokenStream, table, '.')) {
			if (std::getline(tokenStream, column)) {
				// Convert column name to uppercase (optional)
				for (char& c : column) {
					c = std::tolower(c);
				}
				// Add the table name and column to the dictionary
				result[table].push_back(column);
			}
		}
	}

	return result;
}

template<class T>
pair<vector<int>, int> local_factorize(shared_ptr<OperatorLineage> lop, idx_t col_idx) {
	std::unordered_map<T, int> dict;
	vector<int> codes;
	idx_t chunk_count = lop->chunk_collection.ChunkCount();
	for (idx_t chunk_idx=0; chunk_idx < chunk_count; ++chunk_idx) {
		DataChunk &collection_chunk = lop->chunk_collection.GetChunk(chunk_idx);
		for (idx_t i=0; i < collection_chunk.size(); ++i) {
			T v = collection_chunk.GetValue(col_idx, i).GetValue<T>();
			if (dict.find(v) == dict.end()) {
				dict[v] = dict.size();
			}
			codes.push_back(dict[v]);
		}
	}
	return make_pair(codes, dict.size());
}

std::pair<vector<int>, int> factorize(PhysicalOperator* op, shared_ptr<OperatorLineage> lop,
                        std::unordered_map<std::string, std::vector<std::string>> columns_spec) {
	string col_name = columns_spec[lop->table_name].back();
	PhysicalTableScan * scan = dynamic_cast<PhysicalTableScan *>(op);
	std::pair<vector<int>, int> fade_data;
	std::pair< vector<int>, int> res;
	for (idx_t i=0; i < scan->names.size(); i++) {
		if (scan->names[i] == col_name) {
			vector<idx_t> col_codes;
			if (scan->types[i] == LogicalType::INTEGER) {
				res = local_factorize<int>(lop, i);
			} else if (scan->types[i] == LogicalType::VARCHAR) {
				res = local_factorize<string>(lop, i);
			} else if (scan->types[i] == LogicalType::FLOAT) {
				res = local_factorize<float>(lop, i);
			}
			// TODO: combine independent offsets
			fade_data.first = res.first;
			fade_data.second = res.second;
			break;
		}
	}

	return fade_data;
}


vector<int> random_unique(shared_ptr<OperatorLineage> lop, idx_t distinct) {
	vector<int> codes;
	// Seed the random number generator
	std::random_device rd;
	std::mt19937 gen(rd());
	idx_t row_count = lop->chunk_collection.Count();

	// Generate random values
	std::uniform_int_distribution<int> distribution(0, distinct - 1);

	for (idx_t i = 0; i < row_count; ++i) {
		int random_value = distribution(gen);
		codes.push_back(random_value);
	}

	return codes;
}

void TableIntervene(shared_ptr<OperatorLineage> lop,
                    std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                    PhysicalOperator* op,
                    std::unordered_map<std::string, std::vector<std::string>> columns_spec) {
	if (fade_data[op->id].annotations.empty()) return;

	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;

	do {
		cache_on = false;
		result.Reset();
		result.Destroy();
		op->lineage_op->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
		result.Flatten();
		if (result.size() == 0) continue;
		unsigned int * in_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
		int * out_index = reinterpret_cast<int *>(result.data[1].GetData());
		for (idx_t i=0; i < result.size(); ++i) {
			idx_t iid = in_index[i];
			idx_t oid = out_index[i];
			// e.g. rowids, base values, f(base_values)
			fade_data[op->id].annotations[oid] = fade_data[op->id].annotations[iid];
		}
	} while (cache_on || result.size() > 0);
}

void  FilterIntervene(shared_ptr<OperatorLineage> lop,
                     std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                     PhysicalOperator* op) {
  fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
  if ( fade_data[op->id].n_interventions == 1) {
		return;
  }
  bool cache_on = false;
  DataChunk result;
  idx_t global_count = 0;
  idx_t local_count = 0;
  idx_t current_thread = 0;
  idx_t log_id = 0;
  vector<int> child_annotations = fade_data[op->children[0]->id].annotations;

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
			fade_data[op->id].annotations.push_back(child_annotations[iid]);
		}
  } while (cache_on || result.size() > 0);
}

void  JoinIntervene(shared_ptr<OperatorLineage> lop,
                   std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                   PhysicalOperator* op) {
  idx_t left_n_interventions =  fade_data[op->children[0]->id].n_interventions;
  idx_t right_n_interventions =  fade_data[op->children[1]->id].n_interventions;
  fade_data[op->id].n_interventions = left_n_interventions * right_n_interventions;

  if ( fade_data[op->id].n_interventions== 1) {
		return;
  }

  // need to get the total number of rows to pre allocate
  vector<int> annotations;

  bool cache_on = false;
  DataChunk result;
  idx_t global_count = 0;
  idx_t local_count = 0;
  idx_t current_thread = 0;
  idx_t log_id = 0;

  vector<int> lhs_child_annotations = fade_data[op->children[0]->id].annotations;
  vector<int> rhs_child_annotations = fade_data[op->children[1]->id].annotations;
  if (left_n_interventions > 1 && right_n_interventions > 1) {
		do {
			cache_on = false;
			result.Reset();
			result.Destroy();
			op->lineage_op->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
			result.Flatten();
			if (result.size() == 0) continue;
			unsigned int * lhs_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
			unsigned int * rhs_index = reinterpret_cast<unsigned int *>(result.data[1].GetData());
			for (idx_t i=0; i < result.size(); ++i) {
				idx_t lhs = lhs_index[i];
				idx_t rhs = rhs_index[i];
				annotations.push_back(lhs_child_annotations[lhs] * right_n_interventions + rhs_child_annotations[rhs]);
			}
		} while (cache_on || result.size() > 0);
  } else if (left_n_interventions > 1) {
		do {
			cache_on = false;
			result.Reset();
			result.Destroy();
			op->lineage_op->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
			result.Flatten();
			if (result.size() == 0) continue;
			unsigned int * lhs_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
			for (idx_t i=0; i < result.size(); ++i) {
				idx_t lhs = lhs_index[i];
				annotations.push_back(lhs_child_annotations[lhs]);
			}
		} while (cache_on || result.size() > 0);
  } else if (right_n_interventions > 1) {
		do {
			cache_on = false;
			result.Reset();
			result.Destroy();
			op->lineage_op->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
			result.Flatten();
			if (result.size() == 0) continue;
			unsigned int * rhs_index = reinterpret_cast<unsigned int *>(result.data[1].GetData());
			for (idx_t i=0; i < result.size(); ++i) {
				idx_t rhs = rhs_index[i];
				annotations.push_back(rhs_child_annotations[rhs]);
			}
		} while (cache_on || result.size() > 0);
  }

  fade_data[op->id].annotations = std::move(annotations);
}

template<class T>
vector<vector<T>> SumRecompute(PhysicalOperator* op,
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
  vector<T> input_values;

  idx_t chunk_count = op->children[0]->lineage_op->chunk_collection.ChunkCount();

  // to parallelize, first need to materialize input chunks, then divide them between threads
  // this is necessary only if we are computing aggregates without the subtraction property
  // since we need to iterate over all interventions and recompute the aggregates
  vector<vector<T>> new_vals(n_interventions, vector<T> (n_groups, 0));
  vector<int> child_annotations = fade_data[op->children[0]->id].annotations;
  idx_t offset = 0;
  for (idx_t chunk_idx=0; chunk_idx < chunk_count; ++chunk_idx) {
	  DataChunk &collection_chunk = op->children[0]->lineage_op->chunk_collection.GetChunk(chunk_idx);
	  idx_t col_idx = aggregate_input_idx[0];
	  T* col = reinterpret_cast<T*>(collection_chunk.data[col_idx].GetData());
	  if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		  for (idx_t i=0; i < collection_chunk.size(); ++i) {
				new_vals[child_annotations[i+offset]][0] += col[i];
		  }
	  } else {
		  for (idx_t i=0; i < collection_chunk.size(); ++i) {
				input_values.push_back(col[i]);
		  }
	  }
	  offset +=  collection_chunk.size();
  }

  // then specialize recomputation based on the aggregate type with possible SIMD implementations?
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
	  if (result.size() == 0) continue;
	  int64_t * in_index = reinterpret_cast<int64_t *>(result.data[0].GetData());
	  int * out_index = reinterpret_cast<int *>(result.data[1].GetData());
	  for (idx_t i=0; i < result.size(); ++i) {
		  idx_t iid = in_index[i];
		  idx_t oid = out_index[i];
		  T val = input_values[iid];
		  // for i in interventions
		  // 	if intervention[i][iid]==1 /* not deleted */ then update(new_vals[i][oid], val)
		  new_vals[child_annotations[iid]][oid] += val;
	  }

  } while (cache_on || result.size() > 0);
  return new_vals;
}

template<class T>
vector<vector<T>> CountRecompute(PhysicalOperator* op,
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
  vector<vector<T>> new_vals(n_interventions, vector<T> (n_groups, 0));
  vector<int> child_annotations = fade_data[op->children[0]->id].annotations;

  if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	  idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	  for (idx_t i=0; i < row_count; ++i) {
		  new_vals[child_annotations[i]][0] += 1;
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
		  // for i in interventions
		  // if intervention[i][iid]==1 /* not deleted */ then update(new_vals[i][oid], val)
		  new_vals[child_annotations[iid]][oid] += 1;
	  }
  } while (cache_on || result.size() > 0);
  return new_vals;
}


void  HashAggregateIntervene(shared_ptr<OperatorLineage> lop,
                            std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                            PhysicalOperator* op) {
  PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
  auto &aggregates = gb->grouped_aggregate_data.aggregates;
  vector<pair<idx_t, idx_t>> aggregate_input_idx;
  fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
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
	  vector<vector<int>> new_vals;
	  if (name == "sum" || name == "sum_no_overflow") {
		  // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
		  new_vals = SumRecompute<int>(op, fade_data, lop, aggr,
			                           fade_data[op->id].n_interventions, n_groups,
			                           aggregate_input_idx);
	  } else if (name == "count" || name == "count_star") {
		  // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
		  new_vals = CountRecompute<int>(op, fade_data, lop, aggr,
			                             fade_data[op->id].n_interventions, n_groups,
			                             aggregate_input_idx);
	  } else if (name == "avg") {
		  vector<vector<int>> new_vals_count = CountRecompute<int>(op, fade_data, lop, aggr,
			                                                       fade_data[op->id].n_interventions,
			                                                       n_groups, aggregate_input_idx);
		  vector<vector<int>> new_vals_sum = SumRecompute<int>(op, fade_data, lop, aggr,
			                                                   fade_data[op->id].n_interventions,
			                                                   n_groups, aggregate_input_idx);
		  new_vals = new_vals_count;
	  }
	  // rank and discard?
  }
}


void  UngroupedAggregateIntervene(shared_ptr<OperatorLineage> lop,
                                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                                 PhysicalOperator* op) {
  PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(op);
  auto &aggregates = gb->aggregates;
  vector<pair<idx_t, idx_t>> aggregate_input_idx;
  fade_data[op->id].n_interventions = fade_data[op->children[0]->id].n_interventions;
  idx_t n_groups = 1;
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

	  vector<vector<int>> new_vals;
	  if (name == "sum" || name == "sum_no_overflow") {
		  // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
		  new_vals = SumRecompute<int>(op, fade_data, lop, aggr,
			                           fade_data[op->id].n_interventions,
			                           n_groups, aggregate_input_idx);
	  } else if (name == "count" || name == "count_star") {
		  // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
		  new_vals = CountRecompute<int>(op, fade_data, lop, aggr,
			                             fade_data[op->id].n_interventions,
			                             n_groups, aggregate_input_idx);
	  } else if (name == "avg") {
		  vector<vector<int>> new_vals_count = CountRecompute<int>(op, fade_data,lop,
			                                                       aggr, fade_data[op->id].n_interventions,
			                                                       n_groups, aggregate_input_idx);
		  vector<vector<int>> new_vals_sum = SumRecompute<int>(op, fade_data, lop,
			                                                   aggr, fade_data[op->id].n_interventions,
			                                                   n_groups, aggregate_input_idx);
		  new_vals = new_vals_count;
	  }
	  // rank and discard?
  }
}

void GenIntervention(PhysicalOperator* op, std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                     std::unordered_map<std::string, std::vector<std::string>> columns_spec) {
  for (idx_t i = 0; i < op->children.size(); i++) {
	  GenIntervention(op->children[i].get(), fade_data, columns_spec);
  }

  if (op->type == PhysicalOperatorType::TABLE_SCAN) {
	  if (columns_spec.find(op->lineage_op->table_name) == columns_spec.end()) {
		  return;
	  }
	  // 1. access base table, 2. factorize
	  std::pair<vector<int>, idx_t> res = factorize(op, op->lineage_op, columns_spec);

	  fade_data[op->id].annotations = res.first;
	  fade_data[op->id].n_interventions = res.second;
  }
}

void GenRandomIntervention(PhysicalOperator* op,
                           std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                           std::unordered_map<std::string, std::vector<std::string>> columns_spec,
                           idx_t distinct) {
  for (idx_t i = 0; i < op->children.size(); i++) {
	  GenRandomIntervention(op->children[i].get(), fade_data, columns_spec, distinct);
  }

  if (op->type == PhysicalOperatorType::TABLE_SCAN) {
	  if (columns_spec.find(op->lineage_op->table_name) == columns_spec.end()) {
		  return;
	  }


	  // conjunctive predicate intervention
	  fade_data[op->id].annotations = random_unique(op->lineage_op, distinct);
	  fade_data[op->id].n_interventions = distinct;

	  // 2D matrix intervention
	  // fade_data[op->id].del_intervention
	  // fade_data[op->id].scale_intervention

	  // single intervention
	  // fade_data[op->id].single_del_intervention
	  // fade_data[op->id].single_scale_intervention

  }
}

void Intervention(PhysicalOperator* op,
                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                  std::unordered_map<std::string, std::vector<std::string>> columns_spec) {
  for (idx_t i = 0; i < op->children.size(); i++) {
	  Intervention(op->children[i].get(), fade_data, columns_spec);
  }

 // if (op->type == PhysicalOperatorType::TABLE_SCAN) {
	//  TableIntervene(op->lineage_op, op, columns_spec);
 // } else
  if (op->type == PhysicalOperatorType::FILTER) {
	  FilterIntervene(op->lineage_op, fade_data, op);
  } else if (op->type == PhysicalOperatorType::HASH_JOIN
	         || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	         || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	         || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	         || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
	  JoinIntervene(op->lineage_op, fade_data, op);
  } else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
	  HashAggregateIntervene(op->lineage_op, fade_data, op);
  } else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	  UngroupedAggregateIntervene(op->lineage_op, fade_data, op);
  } else if (op->type == PhysicalOperatorType::PROJECTION) {
	  fade_data[op->id].annotations  = fade_data[op->children[0]->id].annotations;
	  fade_data[op->id].n_interventions  = fade_data[op->children[0]->id].n_interventions;
  }
}

vector<idx_t> Rank(PhysicalOperator* op, int k) {
  // go over last intervention matrix
  // get the original output value
  // compute influence and find top k
  return {};
}

void Fade::Why(PhysicalOperator* op, int k, string columns_spec_str, int distinct=0) {
  std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseString( columns_spec_str);

  // holds any extra data needed during exec
  std::unordered_map<idx_t, FadeDataPerNode> fade_data;

  // 2. Post Process
  LineageManager::PostProcess(op);

  // 3. Prune: 	needed only when intervention eval would generate heavy computations in JOIN/FILTER
  // 			or if it'd help to use less memory space.
  // Prune(op, {});

  // 4. Prepare base interventions; should be one time cost per DB
  if (distinct <= 0) {
	  GenIntervention(op, fade_data, columns_spec);
  } else {
	  GenRandomIntervention(op, fade_data, columns_spec, distinct);
  }

  // TODO: add pass to allocate data structures for interventions

  // 4. run intervention
  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
  Intervention(op, fade_data, columns_spec);
  std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
  std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double execution_time = time_span.count();
  std::cout << "INTERVENTION_TIME : " << execution_time << std::endl;

  // 5. rank results
  // vector<idx_t> topk = Rank(op, k);

  // 4. generate disjunctive predicate iteratively and rank them
  // std::vector<int> inputSet = {1, 2, 3};
  // subsets(inputSet);
}

/*


void Prune(PhysicalOperator* op, set<idx_t> parent_pruning_set, bool root=false) {
    DataChunk result;
    idx_t global_count = 0;
    idx_t local_count = 0;
    idx_t current_thread = 0;
    idx_t log_id = 0;
    bool cache_on = false;
    vector<set<idx_t>> pruning_set_per_child(op->children.size(), {});
    do {
        cache_on = false;
        result.Reset();
        result.Destroy();
        op->lineage_op->GetLineageAsChunk(result, global_count, local_count,
                                          current_thread, log_id, cache_on);
        result.Flatten();
        if (op->type == PhysicalOperatorType::HASH_JOIN) {
            unsigned int * lhs_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
            unsigned int * rhs_index = reinterpret_cast<unsigned int *>(result.data[1].GetData());
            int * out_index = reinterpret_cast<int *>(result.data[1].GetData());
            for (idx_t i=0; i < result.size(); ++i) {
                idx_t oid = out_index[i];
                idx_t lhs = lhs_index[i];
                idx_t rhs = rhs_index[i];
                if (root == false && parent_pruning_set.find(oid) == parent_pruning_set.end()) {
                    continue;
                }
                pruning_set_per_child[0].insert(lhs);
                pruning_set_per_child[1].insert(rhs);
            }
        } else {
            unsigned int * in_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
            int * out_index = reinterpret_cast<int *>(result.data[1].GetData());
            for (idx_t i=0; i < result.size(); ++i) {
                idx_t oid = out_index[i];
                idx_t iid = in_index[i];
                if (root == false && parent_pruning_set.find(oid) == parent_pruning_set.end()) {
                    continue;
                }
                pruning_set_per_child[0].insert(iid);
            }
        }
    } while (cache_on || result.size() > 0);

for (idx_t i = 0; i < op->children.size(); i++) {
  Prune(op->children[i].get(), pruning_set_per_child[i]);
}
}

void backtrack(const std::vector<int>& nums, const std::vector<int>& path, std::vector<std::vector<int>>& res, size_t size, int subset_sum, std::vector<int>& powerset_sum_opt) {
  if (path.size() == size) {
	  res.push_back(path);
	  powerset_sum_opt.push_back(subset_sum);
	  return;
  }
  for (size_t i = 0; i < nums.size(); ++i) {
	  std::vector<int> new_path = path;
	  new_path.push_back(nums[i]);
	  backtrack(std::vector<int>(nums.begin() + i + 1, nums.end()), new_path, res, size, subset_sum+nums[i], powerset_sum_opt);
  }
}

std::vector<std::vector<int>> subsets(const std::vector<int>& nums) {
  std::vector<std::vector<int>> powerset;
  std::vector<int> powerset_sum_opt;

  for (size_t size = 0; size <= nums.size(); ++size) {
	  backtrack(nums, {}, powerset, size, 0, powerset_sum_opt);
  }


  // Print the powerset
  for (size_t i=0; i < powerset.size(); i++) {
	  auto subset = powerset[i];
	  std::cout << powerset_sum_opt[i] << " -> {";
	  for (size_t i = 0; i < subset.size(); ++i) {
		  std::cout << subset[i];
		  if (i < subset.size() - 1) {
				std::cout << ", ";
		  }
	  }
	  std::cout << "}\n";
  }
  return powerset;
}

*/
} // namespace duckdb
#endif

