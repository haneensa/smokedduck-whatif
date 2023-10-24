//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_lineage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class PhysicalLineage : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::LINEAGE_ADJUST;

public:
	PhysicalLineage(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                   idx_t estimated_cardinality);

	vector<unique_ptr<Expression>> select_list;


	// bool delim_handled = false;
	bool drop_annotations = false;
	int left_annotation_index = -1;
	bool drop_left = false;
	bool extract_and_reindex = false;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string ParamsToString() const override;
};

} // namespace duckdb
