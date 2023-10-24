#include "duckdb/execution/operator/projection/physical_lineage.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#ifdef LINEAGE
#include "duckdb/common/vector_operations/vector_operations.hpp"
#endif
namespace duckdb {

class LineageState : public OperatorState {
public:
	explicit LineageState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
	    : executor(context.client, expressions) {
	}

	ExpressionExecutor executor;
	idx_t range_start = 0;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "lineage_adjust", 0);
	}
};

PhysicalLineage::PhysicalLineage(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::LINEAGE_ADJUST, std::move(types), estimated_cardinality),
      select_list(std::move(select_list)) {
}
// drop column from the middle of input -- hard to drop from the middle
// log the column


unique_ptr<LineageData> Extract(DataChunk &input, idx_t column_pos) {
	DataChunk annotation_chunk;
	if (column_pos == input.ColumnCount()-1) {
		input.Split(annotation_chunk, column_pos);
	} else {
		DataChunk right_side;
		input.Split(right_side, column_pos+1);
		// temp1 last column has annotations
		input.Split(annotation_chunk, input.ColumnCount()-1);
		input.Fuse(right_side);
	}
	// std::cout << "annotation_chunk: " << annotation_chunk.ToString() << std::endl;
	// annotation_chunk.data[0].Flatten(input.size()); // why o we need this?

	Vector annotations(annotation_chunk.data[0].GetType());
	VectorOperations::Copy(annotation_chunk.data[0], annotations, input.size(), 0, 0);
	// annotations.Verify(input.size());

	auto lineage = make_uniq<LineageVec>(  annotations, input.size());
	return lineage;
}

void Reindex(ExecutionContext &context, DataChunk &input, idx_t range_start) {
	// add annotations
	DataChunk replacement_annotation;
	replacement_annotation.Initialize(context.client, {LogicalType::BIGINT});
	replacement_annotation.SetCardinality(input.size());
	replacement_annotation.data[0].Sequence(range_start, 1, input.size()); // out_index
	input.Fuse(replacement_annotation);
}

OperatorResultType PhysicalLineage::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<LineageState>();

	DataChunk copy_chunk;
	copy_chunk.Initialize(context.client, input.GetTypes());
	// copy input to output -- is it copy or reference?
	state.executor.Execute(input, copy_chunk);
	copy_chunk.SetCardinality(input);

	// this is if we are extracting lineage for between operators
	if (extract_and_reindex) {
		//std::cout << "drop_annotations and reindex" << copy_chunk.size() << " " << children[0]->id << " " << std::endl;
		unique_ptr<LineageData>  lineage = Extract(copy_chunk, copy_chunk.ColumnCount()-1);
		//lineage->Debug();
		// reindex before passing the
		Reindex(context, copy_chunk, state.range_start);
		//std::cout << "Reindex: " << copy_chunk.ToString() << std::endl;

		state.range_start += input.size();
		if (drop_left) {
			unique_ptr<LineageData> lineage_left = Extract(copy_chunk, left_annotation_index);

			//std::cout << "drop left: " << copy_chunk.ToString() << std::endl;

			auto lineage_binary = make_uniq<LineageBinary>(std::move(lineage_left), std::move(lineage));
			children[0]->lineage_op->Capture(make_shared<LogRecord>(move(lineage_binary), 0), 0, 0);
		} else {
			children[0]->lineage_op->Capture(make_shared<LogRecord>(move(lineage), 0), 0, 0);
		}
	} else if (drop_annotations) {
		DataChunk annotations_split;
		// droping last column
		unique_ptr<LineageData> lineage = Extract(copy_chunk, copy_chunk.ColumnCount() - 1);
		// lineage->Debug();
		if (drop_left) {
			unique_ptr<LineageData> lineage_left = Extract(copy_chunk, left_annotation_index - 1);
			auto lineage_binary = make_uniq<LineageBinary>(std::move(lineage_left), std::move(lineage));
			children[0]->lineage_op->Capture(make_shared<LogRecord>(move(lineage_binary), 0), 0, 0);
		} else {
			children[0]->lineage_op->Capture(make_shared<LogRecord>(move(lineage), 0), 0, 0);
		}
	}

		//annotations_split.data[0].Flatten(input.size());
		//annotations_split.data[0].Verify(input.size());



	chunk.Reference(copy_chunk);
	//std::cout << chunk.ToString() << std::endl;
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalLineage::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<LineageState>(context, select_list);
}

string PhysicalLineage::ParamsToString() const {
	string extra_info;
	for (auto &expr : select_list) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}

} // namespace duckdb
