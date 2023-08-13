#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

void OperatorLineage::Capture(const shared_ptr<LineageData>& datum, idx_t in_start, idx_t stage_idx, idx_t thread_id) {
	if (!trace_lineage || datum->Count() == 0) return;
	log_per_thead[thread_id].Append(make_uniq<LogRecord>(datum, in_start), stage_idx);
}

void OperatorLineage::Capture(shared_ptr<LogRecord> log_record, idx_t stage_idx, idx_t thread_id) {
	if (!trace_lineage || log_record->data->Count() == 0) return;
	log_per_thead[thread_id].Append(log_record, stage_idx);
}

void OperatorLineage::Capture(shared_ptr<vector<shared_ptr<LogRecord>>> log_record, idx_t stage_idx, idx_t thread_id) {
	if (!trace_lineage || !log_record) return;
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
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::STREAMING_LIMIT:
    case PhysicalOperatorType::LIMIT:
    case PhysicalOperatorType::FILTER:
    case PhysicalOperatorType::TABLE_SCAN:
    case PhysicalOperatorType::ORDER_BY: {
        vector<ColumnDefinition> source;
        source.emplace_back("in_index", LogicalType::INTEGER);
        source.emplace_back("out_index", LogicalType::INTEGER);
        source.emplace_back("thread_id", LogicalType::INTEGER);
        res.emplace_back(move(source));
        break;
    }
    case PhysicalOperatorType::HASH_GROUP_BY:
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// LINEAGE_SOURCE stage_idx=0
		vector<ColumnDefinition> source;
		if (type == PhysicalOperatorType::HASH_GROUP_BY)
			source.emplace_back("in_index", LogicalType::BIGINT);
		else
			source.emplace_back("in_index", LogicalType::INTEGER);
		source.emplace_back("out_index", LogicalType::INTEGER);
		source.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source));

		// LINEAGE_SCAN stage_idx=1
        vector<ColumnDefinition> sink;
        sink.emplace_back("in_index", LogicalType::INTEGER);

		if (type == PhysicalOperatorType::HASH_GROUP_BY)
			sink.emplace_back("out_index", LogicalType::BIGINT);
		else
			sink.emplace_back("out_index", LogicalType::INTEGER);

		sink.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(sink));

		// LINEAGE_COMBINE stage_idx=2
		vector<ColumnDefinition> combine;
		res.emplace_back(move(combine));

		// LINEAGE_FINALIZE stage_idx=3
		vector<ColumnDefinition> finalize;
		if (type == PhysicalOperatorType::HASH_GROUP_BY) {
			finalize.emplace_back("in_index", LogicalType::BIGINT);
			finalize.emplace_back("out_index", LogicalType::BIGINT);
		} else {
			finalize.emplace_back("in_index", LogicalType::INTEGER);
			finalize.emplace_back("out_index", LogicalType::INTEGER);
		}
		finalize.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(finalize));
		break;
	}
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		vector<ColumnDefinition> source;
		source.emplace_back("lhs_index", LogicalType::INTEGER);

		if (type == PhysicalOperatorType::INDEX_JOIN || (type == PhysicalOperatorType::HASH_JOIN && !use_perfect_hash))
			// if perfect hash join -> integer else bigint?
			source.emplace_back("rhs_index", LogicalType::BIGINT);
		else
			source.emplace_back("rhs_index", LogicalType::INTEGER);

		source.emplace_back("out_index", LogicalType::INTEGER);
		source.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source));


		vector<ColumnDefinition> sink;
		sink.emplace_back("in_index", LogicalType::INTEGER);

		if (type == PhysicalOperatorType::HASH_JOIN) {
			sink.emplace_back("out_index", LogicalType::BIGINT);
		} else {
			sink.emplace_back("out_index", LogicalType::INTEGER);
		}

		sink.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(sink));

		// combine
		vector<ColumnDefinition> combine;
		combine.emplace_back("in_index", LogicalType::INTEGER);
		combine.emplace_back("out_index", LogicalType::INTEGER);
		combine.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(combine));

		// LINEAGE_FINALIZE stage_idx=3
		vector<ColumnDefinition> finalize;
		finalize.emplace_back("in_index", LogicalType::BIGINT);
		finalize.emplace_back("out_index", LogicalType::INTEGER);
		finalize.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(finalize));

		break;
	}
		default: {
			// Lineage unimplemented! TODO all of these :)
		}
	}

	return res;
}

void fillBaseChunk(DataChunk &insert_chunk, idx_t res_count, Vector &lhs_payload, Vector &rhs_payload, idx_t count_so_far, Vector &thread_id_vec) {
	insert_chunk.SetCardinality(res_count);
	insert_chunk.data[0].Reference(lhs_payload);
	insert_chunk.data[1].Reference(rhs_payload);
	insert_chunk.data[2].Sequence(count_so_far, 1, res_count);
	insert_chunk.data[3].Reference(thread_id_vec);
}

idx_t OperatorLineage::GetLineageAsChunk(idx_t count_so_far, DataChunk &insert_chunk, idx_t thread_id, idx_t data_idx, idx_t stage_idx, bool &cache) {
	idx_t log_size = log_per_thead[thread_id].GetLogSize(stage_idx);
	if (log_size > data_idx) {
		LogRecord* data_woffset = log_per_thead[thread_id].GetLogRecord(stage_idx, data_idx).get();
		Vector thread_id_vec(Value::INTEGER(thread_id));

		// TODO: check if LogRecord is cached, then iterate over the lineage inside the cache

		auto table_types = GetTableColumnTypes();
		vector<LogicalType> types;

		for (const auto& col_def : table_types[stage_idx]) {
			types.push_back(col_def.GetType());
		}
		insert_chunk.InitializeEmpty(types);

		switch (this->type) {
		case PhysicalOperatorType::COLUMN_DATA_SCAN:
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::STREAMING_LIMIT:
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
			if (stage_idx == LINEAGE_SINK) {
				// in_index | LogicalType::INTEGER, out_index|LogicalType::BIGINT, thread_id|LogicalType::INTEGER
				//  data_woffset->data is CollectionLineage
				idx_t res_count  = 0;
				if (type == PhysicalOperatorType::HASH_GROUP_BY) {
					// encode grouping set in the table name
					//dynamic_cast<CollectionLineage &>(*data_woffset->data).Debug();
					auto lineage_vec = dynamic_cast<CollectionLineage &>(*data_woffset->data).lineage_vec;
					res_count =  lineage_vec->at(0)->Count();
					Vector out_index(types[1], lineage_vec->at(0)->Process(0));
					insert_chunk.data[1].Reference(out_index);
				} else {
					Vector out_index =  data_woffset->data->GetVecRef(types[1], 0);
					res_count = data_woffset->data->Count();
					insert_chunk.data[1].Reference(out_index);
				}

				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Sequence(count_so_far, 1, res_count);
				insert_chunk.data[2].Reference(thread_id_vec);
			} else if (stage_idx == LINEAGE_FINALIZE) {
				//dynamic_cast<CollectionLineage &>(*data_woffset->data).Debug();
				auto lineage_vec = dynamic_cast<CollectionLineage &>(*data_woffset->data).lineage_vec;
				auto nested_lineage_vec =  dynamic_cast<CollectionLineage &>(*lineage_vec->at(0)).lineage_vec;
				idx_t res_count =  0;
				for (idx_t i=0; i < nested_lineage_vec->size(); i++) {
					res_count +=  nested_lineage_vec->at(i)->Count();
					Vector source_payload(types[0], nested_lineage_vec->at(i)->Process(0));
					Vector new_payload(types[1], nested_lineage_vec->at(i)->Process(0));
					insert_chunk.data[0].Reference(source_payload);
					insert_chunk.data[1].Reference(new_payload);
					break;
				}

				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[2].Reference(thread_id_vec);
			} else {
				// in_index|LogicalType::BIGINT, out_index|LogicalType::INTEGER, thread_id| LogicalType::INTEGER
				idx_t res_count = data_woffset->data->Count();

				// Vector in_index(types[0], this_data.data->GetLineageAsChunk(0));
				Vector in_index = data_woffset->data->GetVecRef(types[0], 0);
				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Reference(in_index);
				insert_chunk.data[1].Sequence(count_so_far, 1, res_count); // out_index
				insert_chunk.data[2].Reference(thread_id_vec);
			}
			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT:
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN: {
			if (stage_idx == LINEAGE_SOURCE) {
				// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

				// This is pretty hacky, but it's fine since we're just validating that we haven't broken HashJoins
				// when introducing LineageNested
				Vector lhs_payload(types[0]);
				Vector rhs_payload(types[1]);

				idx_t res_count = data_woffset->data->Count();

				// Left side / probe side
				if (dynamic_cast<LineageBinary &>(*data_woffset->data).left == nullptr) {
					lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(lhs_payload, true);
				} else {
					Vector temp = dynamic_cast<LineageBinary&>(*data_woffset->data).left->GetVecRef(types[0], data_woffset->in_start);
					lhs_payload.Reference(temp);
				}

				// Right side / build side
				if (dynamic_cast<LineageBinary &>(*data_woffset->data).right == nullptr) {
					rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(rhs_payload, true);
				} else {
					Vector temp = dynamic_cast<LineageBinary&>(*data_woffset->data).right->GetVecRef(types[1], 0);
					rhs_payload.Reference(temp);
				}

				fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id_vec);
				count_so_far += res_count;
			}
			break;
		}
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN: {
			if (stage_idx == LINEAGE_SOURCE) {
				// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

				// This is pretty hacky, but it's fine since we're just validating that we haven't broken HashJoins
				// when introducing LineageNested
				Vector lhs_payload(types[0]);
				Vector rhs_payload(types[1]);

				idx_t res_count = data_woffset->data->Count();

				// Left side / probe side
				if (dynamic_cast<LineageBinary&>(*data_woffset->data).left == nullptr) {
					lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(lhs_payload, true);
				} else {
					if (type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN &&
					    typeid(* dynamic_cast<LineageBinary&>(*data_woffset->data).left) == typeid(LineageBinary)) {
							auto left = dynamic_cast<LineageBinary&>(*data_woffset->data).left;
							auto order_data = dynamic_cast<LineageBinary&>(*left).left;
							auto sel_data = dynamic_cast<LineageBinary&>(*left).right;

							auto temp = order_data->GetVecRef(types[0], data_woffset->in_start);
							temp.Slice(dynamic_cast<LineageSelVec&>(*sel_data).vec, dynamic_cast<LineageSelVec&>(*sel_data).count);
							lhs_payload.Reference(temp);
							res_count = dynamic_cast<LineageSelVec&>(*sel_data).count;
					} else {
						Vector temp(types[0], data_woffset->data->Process(data_woffset->in_start));
						lhs_payload.Reference(temp);
					}
				}

				// Right side / build side
				if (dynamic_cast<LineageBinary&>(*data_woffset->data).right == nullptr) {
					rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(rhs_payload, true);
				} else {
					Vector temp(types[1], dynamic_cast<LineageBinary&>(*data_woffset->data).right->Process(0));
					rhs_payload.Reference(temp);
				}

				fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id_vec);
				count_so_far += res_count;
			} else 	if (stage_idx == LINEAGE_SINK) {
				// schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
				idx_t res_count = data_woffset->data->Count();
				insert_chunk.SetCardinality(res_count);
				Vector in_index = data_woffset->data->GetVecRef(types[0], data_woffset->in_start);
				insert_chunk.data[0].Reference(in_index);
				insert_chunk.data[1].Sequence(count_so_far, 1, res_count); // out_index
				insert_chunk.data[2].Reference(thread_id_vec);  // thread_id
			}
			break;
		} case PhysicalOperatorType::HASH_JOIN: {
			// Hash Join - other joins too?
			if (stage_idx == LINEAGE_SINK) {
				// sink: [BIGINT in_index, INTEGER out_index, INTEGER thread_id]
				idx_t res_count = data_woffset->data->Count();
				insert_chunk.SetCardinality(res_count);
				// if data_woffset->data is Binary, then there is a null in the input build
				// else it is just a single vector
				if (typeid(*data_woffset->data) == typeid(LineageBinary)) {
					// get selection vector
					Vector payload = dynamic_cast<LineageBinary&>(*data_woffset->data).right->GetVecRef(types[1], 0);
					insert_chunk.data[1].Reference(payload);
					// in_index
					Vector sel = dynamic_cast<LineageBinary&>(*data_woffset->data).left->GetVecRef(types[0], data_woffset->in_start);
					insert_chunk.data[0].Reference(sel);
				} else {
					Vector payload = data_woffset->data->GetVecRef(types[1], 0);
					insert_chunk.data[1].Reference(payload);
					// in_index
					insert_chunk.data[0].Sequence(data_woffset->in_start, 1, res_count);
				}
				insert_chunk.data[2].Reference(thread_id_vec);
				count_so_far += res_count;
			} else if (stage_idx == LINEAGE_FINALIZE) {
				if (cache_offset < cache_size) {
					idx_t diff = (cache_size - cache_offset);
					if (diff / STANDARD_VECTOR_SIZE >= 1) {
						diff = STANDARD_VECTOR_SIZE;
						cache = true;
					} else {
						// last batch
						cache = false;
						cache_offset = 0;
						cache_size = 0;
					}

					Vector lhs_payload(types[0],  data_woffset->data->Process(0));
					Vector rhs_payload(types[1], data_woffset->data->Process(0));
					SelectionVector remaining_sel(diff);
					idx_t remaining_count = 0;
					for (idx_t i = 0; i < diff; i++) {
						remaining_sel.set_index(remaining_count++, cache_offset + i);
					}

					// adjust the offset
					insert_chunk.Slice(remaining_sel, remaining_count);
					insert_chunk.data[0].Reference(lhs_payload);
					insert_chunk.data[1].Reference(rhs_payload);
					//insert_chunk.data[1].Sequence(count_so_far, 1, res_count);
					insert_chunk.data[2].Reference(thread_id_vec);
					insert_chunk.SetCardinality(remaining_count);
					count_so_far += remaining_count;
					if (cache) {
						cache_offset += remaining_count;
					}
				} else {
					idx_t res_count = data_woffset->data->Count();

					Vector lhs_payload(types[0],  data_woffset->data->Process(0));
					Vector rhs_payload(types[1], data_woffset->data->Process(0));

					if (res_count > STANDARD_VECTOR_SIZE) {
						cache = true;
						SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
						idx_t remaining_count = 0;
						for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
							remaining_sel.set_index(remaining_count++, cache_offset + i);
						}
						cache_offset = remaining_count;
						cache_size = res_count;
						// adjust the offset
						insert_chunk.Slice(remaining_sel, remaining_count);
						res_count = remaining_count;
					}

					insert_chunk.data[0].Reference(lhs_payload);
					insert_chunk.data[1].Reference(rhs_payload);
					//insert_chunk.data[1].Sequence(count_so_far, 1, res_count);
					insert_chunk.data[2].Reference(thread_id_vec);
					insert_chunk.SetCardinality(res_count);
					count_so_far += res_count;
				}
			} else if (stage_idx == LINEAGE_SOURCE) {
					// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

					// This is pretty hacky, but it's fine since we're just validating that we haven't broken HashJoins
					// when introducing LineageNested
					Vector lhs_payload(types[0]);
					Vector rhs_payload(types[1]);

					idx_t res_count = data_woffset->data->Count();

					// Left side / probe side
					if (dynamic_cast<LineageBinary&>(*data_woffset->data).left == nullptr) {
						lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
						ConstantVector::SetNull(lhs_payload, true);
					} else {
						Vector temp(types[0],  data_woffset->data->Process(data_woffset->in_start));
						lhs_payload.Reference(temp);
					}

					// Right side / build side
					if (dynamic_cast<LineageBinary&>(*data_woffset->data).right == nullptr) {
						rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
						ConstantVector::SetNull(rhs_payload, true);
					} else {
						Vector temp(types[1], data_woffset->data->Process(0));
						rhs_payload.Reference(temp);
					}

					fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id_vec);
					count_so_far += res_count;
			}
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
