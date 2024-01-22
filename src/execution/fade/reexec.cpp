#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <immintrin.h>
#include <random>

namespace duckdb {

template<class T>
vector<T> SumRecompute(PhysicalOperator* op,
                               shared_ptr<OperatorLineage> lop,
                               BoundAggregateExpression& aggr,
                               idx_t n_groups, vector<idx_t> aggregate_input_idx,
                                vector<int>& lineage) {
  idx_t chunk_count = op->children[0]->lineage_op->chunk_collection.ChunkCount();

  vector<T> new_vals(n_groups, 0);
  int offset = 0;
  for (idx_t chunk_idx=0; chunk_idx < chunk_count; ++chunk_idx) {
	  DataChunk &collection_chunk = op->children[0]->lineage_op->chunk_collection.GetChunk(chunk_idx);
	  idx_t col_idx = aggregate_input_idx[0];
	  T* col = reinterpret_cast<T*>(collection_chunk.data[col_idx].GetData());
	  if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		  for (idx_t i=0; i < collection_chunk.size(); ++i) {
				new_vals[0] += col[i];
		  }
	  } else {
		  for (idx_t i=0; i < collection_chunk.size(); ++i) {
        new_vals[lineage[i+offset]] += col[i];
		  }
	  }
    offset += collection_chunk.size();
  }

	return new_vals;
}

vector<int> CountRecompute(PhysicalOperator* op, shared_ptr<OperatorLineage> lop,
                                 BoundAggregateExpression& aggr, idx_t n_groups,
                                 vector<int>& lineage) {
  vector<int> new_vals(n_groups, 0);

	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
  if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	  for (idx_t i=0; i < row_count; ++i) {
		  new_vals[0] += 1;
	  }
  } else {
	  for (idx_t i=0; i < row_count; ++i) {
      new_vals[lineage[i]] += 1;
	  }
  }

	return new_vals;
}


void get_lineage(shared_ptr<OperatorLineage> lop, vector<int>& lineage) {
  DataChunk result;
  idx_t global_count = 0;
  idx_t local_count = 0;
  idx_t current_thread = 0;
  idx_t log_id = 0;
  bool cache_on = false;
  do {
	  cache_on = false;
	  result.Reset();
	  result.Destroy();
	  lop->GetLineageAsChunk(result, global_count, local_count,
		                     current_thread, log_id, cache_on);
	  result.Flatten();
	  if (result.size() == 0) continue;
	  int64_t * in_index = reinterpret_cast<int64_t *>(result.data[0].GetData());
	  int* out_index = reinterpret_cast<int*>(result.data[1].GetData());
	  for (idx_t i=0; i < result.size(); ++i) {
		  idx_t iid = in_index[i];
		  idx_t oid = out_index[i];
      lineage[iid] = oid;
	  }

  } while (cache_on || result.size() > 0);
}

void  HashAggregateRexec(shared_ptr<OperatorLineage> lop,
                            PhysicalOperator* op) {
  PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
  auto &aggregates = gb->grouped_aggregate_data.aggregates;
  vector<pair<idx_t, idx_t>> aggregate_input_idx;
  bool include_count = false;

  vector<int> lineage;
  vector<int> new_count;
	idx_t n_groups = lop->chunk_collection.Count();
	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
  lineage.resize(row_count);
  get_lineage(lop, lineage);
  
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
	  vector<int> new_vals;
	  if (name == "sum" || name == "sum_no_overflow") {
		  // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
		  new_vals = SumRecompute<int>(op, lop, aggr, n_groups, aggregate_input_idx, lineage);
	  } else if (include_count == false && (name == "count" || name == "count_star")) {
		  // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
		  new_count = CountRecompute(op, lop, aggr, n_groups, lineage);
      include_count = true;
	  } else if (name == "avg") {
		  new_vals = SumRecompute<int>(op, lop, aggr, n_groups, aggregate_input_idx, lineage);
      if (include_count == false) {
		    new_count = CountRecompute(op, lop, aggr, n_groups, lineage);
        include_count = true;
      }
	  }
	  // rank and discard?
  }
}


void  UngroupedAggregateRexec(shared_ptr<OperatorLineage> lop, PhysicalOperator* op) {
  PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(op);
  auto &aggregates = gb->aggregates;
  vector<pair<idx_t, idx_t>> aggregate_input_idx;
  idx_t n_groups = 1;
  vector<int> lineage;
  vector<int> new_count;
  bool include_count = false;
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
    
	  vector<int> new_vals;
	  if (name == "sum" || name == "sum_no_overflow") {
		  // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
		  new_vals = SumRecompute<int>(op, lop, aggr,
			                           n_groups, aggregate_input_idx, lineage);
	  } else if (include_count == false && (name == "count" || name == "count_star")) {
		  // use case statement to choose the right Recompute function. Specialize it for data type and aggregate function
		  new_vals = CountRecompute(op, lop, aggr,n_groups, lineage);
      include_count = true;
	  } else if (name == "avg") {
      if (include_count == false) {
		    new_count = CountRecompute(op,lop,aggr,  n_groups, lineage);
        include_count = true;
      }
		  new_vals = SumRecompute<int>(op, lop, aggr, n_groups, aggregate_input_idx, lineage);
	  }
	  // rank and discard?
  }
}

void RexecInternal(PhysicalOperator* op) {
  for (idx_t i = 0; i < op->children.size(); i++) {
	  RexecInternal(op->children[i].get());
  }

 if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
	  HashAggregateRexec(op->lineage_op, op);
  } else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	  UngroupedAggregateRexec(op->lineage_op, op);
  }
}

void Fade::Rexec(PhysicalOperator *op) {
  // 1. Post Process
  LineageManager::PostProcess(op);
  // 2. rexec
  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
  RexecInternal(op);
  std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
  std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
  double execution_time = time_span.count();
  std::cout << "INTERVENTION_TIME : " << execution_time << std::endl;
}

} // namespace duckdb
#endif
