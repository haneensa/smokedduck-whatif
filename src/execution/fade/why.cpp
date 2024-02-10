#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

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

void Fade::Why(PhysicalOperator* op,  EvalConfig config) {
  /*
  std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec( config);

  // holds any extra data needed during exec
  std::unordered_map<idx_t, FadeDataPerNode> fade_data;

  // 2. Post Process
  LineageManager::PostProcess(op);

  // 4. Prepare base interventions; should be one time cost per DB
  if (config.n_intervention <= 0) {
	  Fade::GenIntervention(op, fade_data, columns_spec);
  } else {
	  GenRandomIntervention(op, fade_data, columns_spec, config.n_intervention);
  }

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
   */
}

/*

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

