#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

class ProjectionState : public OperatorState {
public:
	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
	    : executor(context.client, expressions) {
	}

	ExpressionExecutor executor;
	idx_t range_start = 0;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "projection", 0);
	}
};

PhysicalProjection::PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PROJECTION, std::move(types), estimated_cardinality),
      select_list(std::move(select_list)) {
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<ProjectionState>();


	state.executor.Execute(input, chunk);

	if (special) {
		unique_ptr<LineageData> lineage_left = nullptr;
		if (drop_annotations) {
			DataChunk temp1;
			input.Split(temp1, input.ColumnCount()-1);
			lineage_left = make_uniq<LineageVec>(temp1.data[0],  input.size());
			//std::cout << "Right " << input.size() << " " << children[0]->id << " " <<  std::endl;
			//lineage->Debug();
		}

		if (add_annotations) {
			DataChunk temp2;
			temp2.Initialize(context.client, {LogicalType::BIGINT}, input.size());
			temp2.SetCardinality(input.size());
			temp2.data[0].Sequence(state.range_start, 1, input.size()); // out_index
			// generate a sequence
			input.Fuse(temp2);
			state.range_start += input.size();
		}
		// drop column from the middle of input -- hard to drop from the middle
		// log the column
		Vector annotations = std::move(input.data[left_annotation_index]);
		auto lineage_right = make_uniq<LineageVec>(annotations,  input.size());
		//std::cout << "Left " << input.size() << " " << left_annotation_index << " " << children[0]->id << " " <<  std::endl;
		//lineage->Debug();
		// Log operator ID this annotations belong to
		auto lineage = make_uniq<LineageBinary>(std::move(lineage_left), std::move(lineage_right));
		children[0]->lineage_op->Capture(make_shared<LogRecord>(move(lineage), 0), 1, 0);
	}
 	/*

	 1. drop annotations
	 chunk.Split()

	 2. add annotations (after projection)
	 3. normal projection (nothing)

	 */

	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<ProjectionState>(context, select_list);
}

unique_ptr<PhysicalOperator>
PhysicalProjection::CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types,
                                         const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map,
                                         const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality) {

	vector<unique_ptr<Expression>> proj_selects;
	proj_selects.reserve(proj_types.size());

	if (left_projection_map.empty()) {
		for (storage_t i = 0; i < lhs_types.size(); ++i) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
		}
	} else {
		for (auto i : left_projection_map) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
		}
	}
	const auto left_cols = lhs_types.size();

	if (right_projection_map.empty()) {
		for (storage_t i = 0; i < rhs_types.size(); ++i) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
		}

	} else {
		for (auto i : right_projection_map) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
		}
	}

	return make_uniq<PhysicalProjection>(std::move(proj_types), std::move(proj_selects), estimated_cardinality);
}

string PhysicalProjection::ParamsToString() const {
	string extra_info;
	for (auto &expr : select_list) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}

} // namespace duckdb
