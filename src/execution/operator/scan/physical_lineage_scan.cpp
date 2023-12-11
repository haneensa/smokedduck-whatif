#ifdef LINEAGE
#include "duckdb/execution/operator/scan/physical_lineage_scan.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include <utility>

namespace duckdb {


PhysicalLineageScan::PhysicalLineageScan(shared_ptr<OperatorLineage> lineage_op, vector<LogicalType> types,
                                     unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality, idx_t stage_idx)
    : PhysicalOperator(PhysicalOperatorType::LINEAGE_SCAN, std::move(types), estimated_cardinality),
      bind_data(std::move(bind_data_p)), column_ids(std::move(column_ids_p)),
      names(std::move(names_p)), table_filters(std::move(table_filters_p)), stage_idx(stage_idx), lineage_op(lineage_op) {
	if (!lineage_op->processed) {
		lineage_op->PostProcess();
	}
}

PhysicalLineageScan::PhysicalLineageScan(shared_ptr<OperatorLineage> lineage_op, vector<LogicalType> types,
                                         unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types,
                                         vector<column_t> column_ids_p, vector<idx_t> projection_ids_p,
                                         vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                         idx_t estimated_cardinality, idx_t stage_idx)
    : PhysicalOperator(PhysicalOperatorType::LINEAGE_SCAN, std::move(types), estimated_cardinality),
      bind_data(std::move(bind_data_p)), column_ids(std::move(column_ids_p)),
      projection_ids(std::move(projection_ids_p)),
      names(std::move(names_p)), table_filters(std::move(table_filters_p)), stage_idx(stage_idx), lineage_op(lineage_op) {
	if (!lineage_op->processed) {
		lineage_op->PostProcess();
	}
}



class LineageScanLocalSourceState : public LocalSourceState {
public:
	LineageScanLocalSourceState(ExecutionContext &context) {
	}

	idx_t global_count = 0;
	idx_t log_id = 0;
	idx_t current_thread = 0;
	idx_t local_count = 0;
	idx_t chunk_index = 0;
};

unique_ptr<LocalSourceState> PhysicalLineageScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<LineageScanLocalSourceState>(context);
}


SourceResultType PhysicalLineageScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = input.local_state.Cast<LineageScanLocalSourceState>();

	DataChunk result;
 	bool cache_on = false;
	if (stage_idx == 100) {
		vector<LogicalType> types = lineage_op->chunk_collection.Types();
		types.push_back(LogicalType::INTEGER);
		result.InitializeEmpty(types);


		if (lineage_op->chunk_collection.Count() == 0) {
			return SourceResultType::FINISHED;
		}
		if (state.chunk_index >= lineage_op->chunk_collection.ChunkCount()) {
			return SourceResultType::FINISHED;
		}
		DataChunk &collection_chunk = lineage_op->chunk_collection.GetChunk(state.chunk_index);
		result.Reference(collection_chunk);
		state.chunk_index++;
		state.global_count += result.size();
		state.local_count += result.size();
	} else {
		lineage_op->GetLineageAsChunk(result, state.global_count, state.local_count,
		                                  state.current_thread, state.log_id, cache_on);
	}

 	// Apply projection list
	chunk.Reset();
	chunk.SetCardinality(result.size());
	if (result.size() > 0) {
		for (uint col_idx=0; col_idx < column_ids.size(); ++col_idx) {
			idx_t column = column_ids[col_idx];
			if (column == COLUMN_IDENTIFIER_ROW_ID) {
				// row id column: fill in the row ids
				D_ASSERT(chunk.data[col_idx].GetType().InternalType() == PhysicalType::INT64);
				chunk.data[col_idx].Sequence(state.global_count-result.size(), 1, result.size());
			}  else {
				chunk.data[col_idx].Reference(result.data[column]);
			}
		}
	}

	if (cache_on || chunk.size() > 0) {
		// add flag if there is a cache, don't make progress
		return SourceResultType::HAVE_MORE_OUTPUT;
	} else {
		return SourceResultType::FINISHED;
	}
}



} // namespace duckdb
#endif
