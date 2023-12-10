//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_lineage_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#ifdef LINEAGE
#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/function/table/table_scan.hpp"

#include <duckdb/function/function.hpp>
#include <duckdb/planner/table_filter.hpp>

namespace duckdb {

//! Return a relational view over in-memory lineage
class PhysicalLineageScan : public PhysicalOperator {
public:
	//! Regular Scan
	PhysicalLineageScan(shared_ptr<OperatorLineage> lineage_op, vector<LogicalType> types, unique_ptr<FunctionData> bind_data,
	                  vector<column_t> column_ids, vector<string> names, unique_ptr<TableFilterSet> table_filters,
	                  idx_t estimated_cardinality, idx_t stage_idx);
	//! scan that immediately projects out filter columns that are unused in the remainder of the query plan
	PhysicalLineageScan(shared_ptr<OperatorLineage> lineage_op, vector<LogicalType> types, unique_ptr<FunctionData> bind_data,
	                  vector<LogicalType> returned_types, vector<column_t> column_ids, vector<idx_t> projection_ids,
	                  vector<string> names, unique_ptr<TableFilterSet> table_filters, idx_t estimated_cardinality, idx_t stage_idx);

	//! Bind data of the function
	unique_ptr<FunctionData> bind_data;
	//! The types of ALL columns that can be returned by the table function
	vector<LogicalType> returned_types;
	//! The column ids used within the table function
	vector<column_t> column_ids;
	//! The projected-out column ids
	vector<idx_t> projection_ids;
	//! The names of the columns
	vector<string> names;
	//! The table filters
	unique_ptr<TableFilterSet> table_filters;

	//! sub-operator index
	idx_t stage_idx;
	//! artifact log for this operator
	shared_ptr<OperatorLineage> lineage_op;

public:
	public:
	  unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                  GlobalSourceState &gstate) const override;
		SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

		bool IsSource() const override {
			return true;
		}

 };
} // namespace duckdb
#endif
