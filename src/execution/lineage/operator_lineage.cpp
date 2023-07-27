#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

void OperatorLineage::Capture(const shared_ptr<LineageData>& datum, idx_t in_start, idx_t stage_idx, idx_t thread_id) {
	if (!trace_lineage || datum->Count() == 0) return;
	log_per_thead[thread_id].Append(make_uniq<LogRecord>(datum, in_start), stage_idx);
}

void OperatorLineage::Capture(shared_ptr<LogRecord> log_record, idx_t stage_idx, idx_t thread_id) {
	if (!trace_lineage) return;
	log_per_thead[thread_id].Append(log_record, stage_idx);
}

idx_t OperatorLineage::Size() {
	idx_t size = 0;
	for (auto& log : log_per_thead) {
		size +=  log.second.GetLogSize(log.first);
	}

	return size;
}


idx_t Log::GetLogSizeBytes() {
	idx_t size_bytes = 0;
	for (idx_t i = 0; i < log->size(); i++) {
		for (const auto& lineage_data : log[i]) {
			size_bytes += lineage_data->data->Size();
		}
	}
	return size_bytes;
}
//! Get the column types for this operator
//! Returns 1 vector of ColumnDefinitions for each table that must be created
vector<vector<ColumnDefinition>> OperatorLineage::GetTableColumnTypes() {
    vector<vector<ColumnDefinition>> res;
    switch (type) {
	case PhysicalOperatorType::TOP_N:
    case PhysicalOperatorType::LIMIT:
    case PhysicalOperatorType::FILTER:
    case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::PROJECTION:
    case PhysicalOperatorType::ORDER_BY: {
        vector<ColumnDefinition> source;
        source.emplace_back("in_index", LogicalType::BIGINT);
        source.emplace_back("out_index", LogicalType::INTEGER);
        source.emplace_back("thread_id", LogicalType::INTEGER);
        res.emplace_back(move(source));
        break;
    }
    case PhysicalOperatorType::HASH_GROUP_BY:
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// LINEAGE_SOURCE stage_idx=0
		vector<ColumnDefinition> source;
		source.emplace_back("in_index", LogicalType::LIST(LogicalType::BIGINT));
		source.emplace_back("out_index", LogicalType::BIGINT);
		source.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source));

		break;
	}
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		vector<ColumnDefinition> source;
		source.emplace_back("lhs_index", LogicalType::BIGINT);
		source.emplace_back("rhs_index", LogicalType::BIGINT);
		source.emplace_back("out_index", LogicalType::INTEGER);
		source.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source));
		break;
	}
		default: {
			// Lineage unimplemented! TODO all of these :)
		}
	}

	return res;
}

idx_t OperatorLineage::GetLineageAsChunk(idx_t count_so_far, DataChunk &insert_chunk, idx_t thread_id, idx_t data_idx, idx_t stage_idx) {
	idx_t log_size = log_per_thead[thread_id].GetLogSize(stage_idx);
	if (log_size > data_idx) {
		LogRecord* data_woffset = log_per_thead[thread_id].GetLogRecord(stage_idx, data_idx).get();
		Vector thread_id_vec(Value::INTEGER(thread_id));

		auto table_types = GetTableColumnTypes();
		vector<LogicalType> types;

		for (const auto& col_def : table_types[stage_idx]) {
			types.push_back(col_def.GetType());
		}
		insert_chunk.InitializeEmpty(types);

		switch (this->type) {
		case PhysicalOperatorType::TOP_N:
		case PhysicalOperatorType::PROJECTION:
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::TABLE_SCAN: {
			D_ASSERT(stage_idx == LINEAGE_SOURCE);
			// Seq Scan, Filter, Limit, Order By, TopN, etc...
			// schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
			idx_t res_count = data_woffset->data->Count();
			insert_chunk.SetCardinality(res_count);
			Vector in_index = data_woffset->data->GetVecRef(types[0], data_woffset->in_start);
			insert_chunk.data[0].Reference(in_index);
			insert_chunk.data[1].Sequence(count_so_far, 1, res_count); // out_index
			insert_chunk.data[2].Reference(thread_id_vec);  // thread_id
			break;
		}
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
			// Hash Aggregate / Perfect Hash Aggregate
			// sink schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
 				// in_index | LogicalType::INTEGER, out_index|LogicalType::BIGINT, thread_id|LogicalType::INTEGER
				idx_t res_count = data_woffset->data->Count();

				Vector in_index = data_woffset->data->GetVecRef(types[0], 0);

				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Reference(in_index);
			    insert_chunk.data[1].Sequence(count_so_far, 1, res_count);
				insert_chunk.data[2].Reference(thread_id_vec);

			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN:
		case PhysicalOperatorType::HASH_JOIN: {
			// in_index | LogicalType::INTEGER, out_index|LogicalType::BIGINT, thread_id|LogicalType::INTEGER
			idx_t res_count = data_woffset->data->Count();
			Vector lhs_payload = dynamic_cast<LineageBinary&>(*data_woffset->data).left->GetVecRef(types[0], 0);
			Vector rhs_payload = dynamic_cast<LineageBinary&>(*data_woffset->data).right->GetVecRef(types[1], 0);
			data_woffset->data->Debug();
			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Reference(lhs_payload);
			insert_chunk.data[1].Reference(rhs_payload);
			insert_chunk.data[2].Sequence(count_so_far, 1, res_count);
			insert_chunk.data[3].Reference(thread_id_vec);
			break;
		}

		default:
			// We must capture lineage for everything getting processed
			D_ASSERT(false);
		}
	}

	return insert_chunk.size();
}


} // namespace duckdb
#endif
