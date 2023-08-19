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
      names(std::move(names_p)), table_filters(std::move(table_filters_p)), stage_idx(stage_idx), lineage_op(lineage_op) {}

PhysicalLineageScan::PhysicalLineageScan(shared_ptr<OperatorLineage> lineage_op, vector<LogicalType> types,
                                         unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types,
                                         vector<column_t> column_ids_p, vector<idx_t> projection_ids_p,
                                         vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                         idx_t estimated_cardinality, idx_t stage_idx)
    : PhysicalOperator(PhysicalOperatorType::LINEAGE_SCAN, std::move(types), estimated_cardinality),
      bind_data(std::move(bind_data_p)), column_ids(std::move(column_ids_p)),
      projection_ids(std::move(projection_ids_p)),
      names(std::move(names_p)), table_filters(std::move(table_filters_p)), stage_idx(stage_idx), lineage_op(lineage_op) {}



class PhysicalLineageScanState : public GlobalSourceState {
public:
	explicit PhysicalLineageScanState(shared_ptr<OperatorLineage> lineage_op) : initialized(false) {
		for (auto &log : lineage_op->log_per_thead) {
			thread_ids.push_back(log.first);
		}
	}

	bool initialized;
	idx_t count_so_far = 0;
	idx_t thread_count = 0;
	idx_t log_id = 0;
	idx_t current_thread = 0;
	vector<idx_t> thread_ids;
	idx_t chunk_index = 0;
};


unique_ptr<GlobalSourceState> PhysicalLineageScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PhysicalLineageScanState>(lineage_op);
}

SourceResultType PhysicalLineageScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<PhysicalLineageScanState>();
	auto& thread_ids = state.thread_ids;

	DataChunk result;
	idx_t res_count = 0;
	bool cache_on = false;
	if (stage_idx == 100) {
		bool must_add_rowid = state.chunk_index == lineage_op->intermediate_chunk_processed_counter;

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
		if (must_add_rowid) {
			collection_chunk.data.push_back(Vector(LogicalType::INTEGER));
			collection_chunk.data[collection_chunk.ColumnCount() - 1].Sequence(state.count_so_far, 1,
			                                                                   collection_chunk.size());
			lineage_op->intermediate_chunk_processed_counter++;
		}
		result.Reference(collection_chunk);
		state.chunk_index++;
		state.count_so_far += result.size();
	} else {
		while (state.current_thread < thread_ids.size() && lineage_op->log_per_thead[thread_ids[state.current_thread]].GetLogSize(stage_idx) == 0) {
			state.current_thread++;
			state.log_id = 0;
			state.thread_count = 0;
		}

		if (state.current_thread >= thread_ids.size()) {
			return SourceResultType::FINISHED;
		}

		idx_t thread_id = thread_ids[state.current_thread];
		res_count =
		    lineage_op->GetLineageAsChunk(state.thread_count, result, thread_id, state.log_id, stage_idx, cache_on);
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
				chunk.data[col_idx].Sequence(state.count_so_far , 1, result.size());
			}  else {
				chunk.data[col_idx].Reference(result.data[column]);
			}
		}
	}

	state.thread_count += res_count;
	state.count_so_far += res_count;

	if (chunk.size() == 0 && state.current_thread >= thread_ids.size()) {
		return SourceResultType::FINISHED;
	} else if (chunk.size() == 0) {
		state.current_thread++;
		state.log_id = 0;
		state.thread_count = 0;
		return SourceResultType::HAVE_MORE_OUTPUT;
	} else if (cache_on) {
		// add flag if there is a cache, don't make progress
		return SourceResultType::HAVE_MORE_OUTPUT;
	} else {
			state.log_id++;
			return SourceResultType::HAVE_MORE_OUTPUT;
	}
}



} // namespace duckdb
#endif