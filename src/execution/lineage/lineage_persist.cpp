#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

//! Get the column types for this operator
//! Returns 1 vector of ColumnDefinitions for each table that must be created
vector<vector<ColumnDefinition>> OperatorLineage::GetTableColumnTypes() {
  vector<vector<ColumnDefinition>> res;
  switch (type) {
  case PhysicalOperatorType::HASH_GROUP_BY:
  case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
  case PhysicalOperatorType::COLUMN_DATA_SCAN:
  case PhysicalOperatorType::STREAMING_LIMIT:
  case PhysicalOperatorType::LIMIT:
  case PhysicalOperatorType::FILTER:
  case PhysicalOperatorType::TABLE_SCAN:
  case PhysicalOperatorType::PROJECTION:
  case PhysicalOperatorType::ORDER_BY: {
    vector<ColumnDefinition> source;
    if (type == PhysicalOperatorType::ORDER_BY
		|| type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
		|| type == PhysicalOperatorType::HASH_GROUP_BY)
      source.emplace_back("in_index", LogicalType::BIGINT);
    else
      source.emplace_back("in_index", LogicalType::INTEGER);
    source.emplace_back("out_index", LogicalType::INTEGER);
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
	source.emplace_back("lhs_index", LogicalType::INTEGER);
	source.emplace_back("rhs_index", LogicalType::INTEGER);
	source.emplace_back("out_index", LogicalType::INTEGER);
	res.emplace_back(move(source));
	break;
  }
  default: {
    // Lineage unimplemented! TODO all of these :)
  }
  }
  return res;
}

idx_t OperatorLineage::GetLineageAsChunk(DataChunk &insert_chunk,
                                         idx_t& global_count, idx_t& local_count,
                                         idx_t &thread_id, idx_t &data_idx,  bool &cache) {

  auto table_types = GetTableColumnTypes();
  vector<LogicalType> types;

  for (const auto& col_def : table_types[0]) {
    types.push_back(col_def.GetType());
  }

  insert_chunk.InitializeEmpty(types);
  if (thread_vec.size() <= thread_id) {
	  return 0;
  }

  auto thread_val  = thread_vec[thread_id];
  log_per_thread[thread_val]->GetLineageAsChunk(insert_chunk, global_count, local_count, data_idx, cache_offset, cache_size, cache);
  global_count += insert_chunk.size();
  local_count += insert_chunk.size();

  if (insert_chunk.size() == 0) {
    thread_id++;
    cache = true;
    data_idx = 0;
    // std::cout << "done " << thread_vec.size() << " " << thread_id << " " << data_idx << std::endl;
  }

  return insert_chunk.size();
}

void fillBaseChunk(DataChunk &insert_chunk, idx_t res_count, Vector &lhs_payload,
                   Vector &rhs_payload, idx_t count_so_far) {
  insert_chunk.SetCardinality(res_count);
  insert_chunk.data[0].Reference(lhs_payload);
  insert_chunk.data[1].Reference(rhs_payload);
  insert_chunk.data[2].Sequence(count_so_far, 1, res_count);
}

void  getchunk(idx_t res_count, idx_t global_count,
              DataChunk &insert_chunk, data_ptr_t ptr, idx_t child_offset) {
  insert_chunk.SetCardinality(res_count);
  if (ptr != nullptr) {
    Vector in_index(LogicalType::INTEGER, ptr); // TODO: add offset
    insert_chunk.data[0].Reference(in_index);
  } else {
    insert_chunk.data[0].Sequence(child_offset, 1, res_count); // in_index
  }
  insert_chunk.data[1].Sequence(global_count, 1, res_count); // out_index
}

// schema: [INTEGER in_index, INTEGER out_index]
idx_t FilterLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                   idx_t& global_count, idx_t& local_count,
                                   idx_t& data_idx,
                                   idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }
    
  if (!processed)  BuildIndexes();

  idx_t res_count = lineage[data_idx].count;
  idx_t child_offset = lineage[data_idx].child_offset;
  data_ptr_t ptr = nullptr;
  if (lineage[data_idx].sel != nullptr) {
    auto vec_ptr = lineage[data_idx].sel.get();
    ptr = (data_ptr_t)vec_ptr;
  }
  getchunk(res_count, global_count, insert_chunk,  ptr, child_offset);

  data_idx++;
  return res_count;
}
    
// TableScan
// schema: [INTEGER in_index, INTEGER out_index]
idx_t TableScanLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                      idx_t& global_count, idx_t& local_count,
                                      idx_t& data_idx,
                                      idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }
  
  if (!processed)  BuildIndexes();
    
  idx_t res_count = lineage[data_idx].count;
  idx_t child_offset = lineage[data_idx].start + lineage[data_idx].vector_index;
  data_ptr_t ptr = nullptr;
  if (lineage[data_idx].sel != nullptr) {
    auto vec_ptr = lineage[data_idx].sel->owned_data.get();
    ptr = (data_ptr_t)vec_ptr;
  }
  getchunk(res_count, global_count, insert_chunk,  ptr, child_offset);

 // std::cout << data_idx << " " << res_count << " " << insert_chunk.size() << " " << child_offset << " " << lineage[data_idx].start << " " << lineage[data_idx].vector_index << " " << global_count << " " << local_count << std::endl;
  data_idx++;

  return res_count;
}

// Limit
// schema: [INTEGER in_index, INTEGER out_index]
idx_t LimitLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                  idx_t& global_count, idx_t& local_count,
                                  idx_t& data_idx,
                                  idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }
  
  idx_t current_offset = lineage[data_idx].start;
  idx_t res_count = lineage[data_idx].end;
  idx_t offset = lineage[data_idx].child_offset;

  auto start = current_offset == 0 ? offset : current_offset;
  //auto end = start + res_count;
  insert_chunk.SetCardinality(res_count);
  insert_chunk.data[0].Sequence(start, 1, res_count); // in_index
  insert_chunk.data[1].Sequence(global_count, 1, res_count); // out_index
  data_idx++;
  return res_count;
}
    
// Order By
// schema: [INTEGER in_index, INTEGER out_index]
idx_t OrderByLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                    idx_t& global_count, idx_t& local_count,
                                    idx_t& data_idx,
                                    idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= lineage.size()) {
    cache = false;
    cache_size = 0;
    cache_offset = 0;
	return 0;
  }
  
  idx_t res_count = lineage[data_idx].size();
  data_ptr_t ptr = (data_ptr_t)lineage[data_idx].data();
  if (cache_offset < cache_size) {
    res_count = (cache_size - cache_offset);
    if (res_count / STANDARD_VECTOR_SIZE >= 1) {
      res_count = STANDARD_VECTOR_SIZE;
      cache = true;
    } else {
      // last batch
      cache = false;
    }

    ptr += cache_offset;
		cache_offset += res_count;

    if (!cache) {
      cache_offset = 0;
      cache_size = 0;
	  data_idx++;
    }
  } else {
    if (res_count > STANDARD_VECTOR_SIZE) {
      cache = true;
      cache_size = res_count;
      res_count = STANDARD_VECTOR_SIZE;
      cache_offset += res_count;
    }
  }
  insert_chunk.SetCardinality(res_count);
  Vector in_index(LogicalType::BIGINT, ptr); // TODO: add offset
  insert_chunk.data[0].Reference(in_index);
  insert_chunk.data[1].Sequence(global_count, 1, res_count);
  return res_count;
}
    
// Cross Product
// schema: [INTEGER lhs_index, INTEGER rhs_index, INTEGER out_index]
idx_t CrossLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                  idx_t& global_count, idx_t& local_count,
                                  idx_t& data_idx,
                                  idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }

  idx_t branch_scan_lhs = lineage[data_idx].branch_scan_lhs;
  idx_t res_count = lineage[data_idx].count;
  idx_t out_start = global_count; // lineage[data_idx].out_start;
  idx_t position_in_chunk = lineage[data_idx].position_in_chunk;
  idx_t scan_position = lineage[data_idx].scan_position;

  //std::cout << branch_scan_lhs << " " << res_count << " " << out_start <<
  //  " " << position_in_chunk << " " << scan_position << std::endl;
  if (branch_scan_lhs) {
    Vector rhs_payload(Value::Value::INTEGER(scan_position + position_in_chunk));
    Vector lhs_payload(LogicalType::INTEGER, res_count);
    lhs_payload.Sequence(out_start, 1, res_count);
    fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, global_count);
  } else {
    Vector rhs_payload(LogicalType::INTEGER, res_count);
    Vector lhs_payload(Value::Value::INTEGER(position_in_chunk + out_start));
    lhs_payload.Sequence(scan_position, 1, res_count);
    fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, global_count);
  }
  data_idx++;
  return res_count;
}

// NLJ
// schema: [INTEGER lhs_index, INTEGER rhs_index, INTEGER out_index]
idx_t NLJLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                idx_t& global_count, idx_t& local_count,
                                idx_t& data_idx,
                                idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }
  
  idx_t res_count = lineage[data_idx].count;
  //idx_t out_start = lineage[data_idx].out_start;
  //idx_t current_row_index = lineage[data_idx].current_row_index;
  Vector lhs_payload(LogicalType::INTEGER);
  Vector rhs_payload(LogicalType::INTEGER);
  if (lineage[data_idx].left) {
	  data_ptr_t left_ptr = (data_ptr_t)lineage[data_idx].left->owned_data.get();
	  Vector temp(LogicalType::INTEGER, left_ptr);
	  lhs_payload.Reference(temp);
  } else {
	  lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
	  ConstantVector::SetNull(lhs_payload, true);
  }
  if (lineage[data_idx].right) {
	  data_ptr_t right_ptr = (data_ptr_t)lineage[data_idx].right->owned_data.get();
	  Vector temp(LogicalType::INTEGER, right_ptr);
	  rhs_payload.Reference(temp);
  } else {
	  rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
	  ConstantVector::SetNull(rhs_payload, true);
  }

  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, global_count);
  data_idx++;
  return res_count;
}
    
// BNLJ
// schema: [INTEGER lhs_index, INTEGER rhs_index, INTEGER out_index]
idx_t BNLJLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                 idx_t& global_count, idx_t& local_count,
                                 idx_t& data_idx,
                                 idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }
  data_idx++;
  return 0;
}
    
// Merge
// schema: [INTEGER lhs_index, INTEGER rhs_index, INTEGER out_index]
idx_t MergeLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                  idx_t& global_count, idx_t& local_count,
                                  idx_t& data_idx,
                                  idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }
  data_idx++;

  /*
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

				fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, global_count, thread_id_vec);
				global_count += res_count;
			} else 	if (stage_idx == LINEAGE_SINK) {
				// schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
				idx_t res_count = data_woffset->data->Count();
				insert_chunk.SetCardinality(res_count);
				Vector in_index = data_woffset->data->GetVecRef(types[0], data_woffset->in_start);
				insert_chunk.data[0].Reference(in_index);
				insert_chunk.data[1].Sequence(global_count, 1, res_count); // out_index
				insert_chunk.data[2].Reference(thread_id_vec);  // thread_id
				global_count += res_count;
			}
			break;
   */
	return 0;
}
/*
idx_t HashJoinLog::GetBuildSideIndex(idx_t cur) {
  if (lineage_finalize.size() > 0 && lineage_finalize.back().added_count > cur) {
    // substitute cur with the address it refer to
    // using lineage_finalize data
    auto count = lineage_finalize.back().added_count;
    auto new_cur = lineage_finalize.back().sel->owned_data[cur];
    std::cout << count << " old cur: " << cur << " New : " << new_cur << std::endl;
    // assert new_cur < count
    auto addr =  lineage_finalize.back().scatter.get())[new_cur];
    cur =  addr;
  }

  return 0;
}*/

// Hash Join
// schema: [INTEGER lhs_index, INTEGER rhs_index, INTEGER out_index]
idx_t HashJoinLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                     idx_t& global_count, idx_t& local_count,
                                     idx_t& data_idx,
                                     idx_t &cache_offset, idx_t &cache_size, bool &cache) {
  
  if (data_idx >= output_index.size()) {
	  return 0;
  }
  
  if (hash_index.size() == 0) {
    BuildIndexes();
  }

  idx_t lsn = output_index[data_idx].first;
  if (lsn == 0) { // something is wrong
	return 0;
  }

  lsn -= 1;

  idx_t res_count = lineage_binary[lsn].count;
  //idx_t out_offset = lineage_binary[lsn].out_offset;
  data_ptr_t left_ptr = (data_ptr_t)lineage_binary[lsn].left.get();
  data_ptr_t right_ptr;
  data_ptr_t* right_build_ptr = lineage_binary[lsn].right.get();

  Vector lhs_payload(LogicalType::INTEGER);
  Vector rhs_payload(LogicalType::INTEGER);
  
  // Left side / probe side
  if (left_ptr == nullptr) {
    if (res_count == STANDARD_VECTOR_SIZE) {
      lhs_payload.Sequence(global_count, 1, res_count); // out_index
    } else {
      lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
      ConstantVector::SetNull(lhs_payload, true);
    }
  } else {
    // TODO:  add in_start offset
    Vector temp(LogicalType::INTEGER, left_ptr);
    lhs_payload.Reference(temp);
  }
  
  
  // Right side / build side
  if (right_build_ptr == nullptr) {
    rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
    ConstantVector::SetNull(rhs_payload, true);
  } else {
    if (right_val_log.size() < (lsn+1)) {
      unique_ptr<sel_t[]>  right_val(new sel_t[res_count]);
      for (idx_t i=0; i < res_count; i++) {
        right_val[i] = hash_index[right_build_ptr[i]];
        std::cout << i << " " << right_build_ptr[i] << " " << right_val[i] << std::endl;
      }
      right_val_log.push_back(move(right_val));
      right_ptr = (data_ptr_t)right_val_log.back().get();
    } else {
      right_ptr = (data_ptr_t)right_val_log[lsn].get();
    }
    Vector temp(LogicalType::INTEGER, (data_ptr_t)right_ptr);
    rhs_payload.Reference(temp);
  }

  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, global_count);
  data_idx++;
  return res_count;
}

// Hash Agg
// schema: [INTEGER in_index, INTEGER out_index]
idx_t HALog::GetLineageAsChunk(DataChunk &insert_chunk,
                               idx_t& global_count, idx_t& local_count,
                               idx_t& data_idx,
                               idx_t &cache_offset, idx_t &cache_size, bool &cache) {

	if (data_idx >= scan_log.size()) {
		return 0;
	}

	if (!processed) {
		BuildIndexes();
	}

	idx_t scan_count = scan_log[data_idx].count;
	if (current_key >= scan_count) {
		data_idx++;
		current_key = 0;
		return 0;
	}
	data_ptr_t* payload = scan_log[data_idx].addchunk_lineage.get();
	data_ptr_t output_key = payload[current_key];
	// current scan , current offset into scan, current offset into groups of scan
	vector<idx_t>& la = hash_index[output_key];
	// read from offset_within_key to max(1024, la.size());
	idx_t end_offset = la.size() - offset_within_key;
	if (end_offset > STANDARD_VECTOR_SIZE) {
		end_offset = STANDARD_VECTOR_SIZE;
	}
	insert_chunk.SetCardinality(end_offset);

	// TODO: only display between offset_within_key to end_offset
	data_ptr_t ptr = (data_ptr_t)(la.data() + offset_within_key);
	Vector in_index(LogicalType::BIGINT, ptr);
	// use in index to loop up hash_map_agg
	insert_chunk.data[0].Reference(in_index);
	insert_chunk.data[1].Reference(Value::INTEGER(current_key)); // out_index

 	offset_within_key += end_offset;
	if (offset_within_key >= la.size()) {
		offset_within_key = 0;
		current_key++;
	}
	if (current_key >= scan_log[data_idx].count) {
		cache = false;
		current_key = 0;
		data_idx++;
	} else {
		cache = true;
	}

	return end_offset;
	/*
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
	global_count += res_count;
} else {
}
break;
}
	 */
}


// Perfect Hash Agg
// schema: [INTEGER in_index, INTEGER out_index]
idx_t PHALog::GetLineageAsChunk(DataChunk &insert_chunk,
                                idx_t& global_count, idx_t& local_count,
                                idx_t& data_idx,
                                idx_t &cache_offset, idx_t &cache_size, bool &cache) {

	if (data_idx >= scan_lineage.size()) {
		return 0;
	}

   	if (hash_index.size() == 0) {
   		BuildIndexes();
   	}

	//idx_t scan_count = scan_lineage[data_idx].count;
	uint32_t* payload = scan_lineage[data_idx].gather.get();
	uint32_t output_key = payload[current_key];
	// current scan , current offset into scan, current offset into groups of scan
	vector<idx_t>& la = hash_index[output_key];
	// read from offset_within_key to max(1024, la.size());
	idx_t end_offset = la.size() - offset_within_key;
	if (end_offset > STANDARD_VECTOR_SIZE) {
		end_offset = STANDARD_VECTOR_SIZE;
	}
	insert_chunk.SetCardinality(end_offset);

	// TODO: only display between offset_within_key to end_offset
	data_ptr_t ptr = (data_ptr_t)(la.data() + offset_within_key);
	Vector in_index(LogicalType::BIGINT, ptr);
	// use in index to loop up hash_map_agg
	insert_chunk.data[0].Reference(in_index);
	insert_chunk.data[1].Reference(Value::INTEGER(current_key)); // out_index

	global_count += end_offset;
	offset_within_key += end_offset;
	if (offset_within_key >= la.size()) {
		offset_within_key = 0;
		current_key++;
	}
	if (current_key >= scan_lineage[data_idx].count) {
		cache = false;
		current_key = 0;
		data_idx++;
	} else {
		cache = true;
	}

	return end_offset;
}
    
/*
   case PhysicalOperatorType::PROJECTION: {
           D_ASSERT(stage_idx == LINEAGE_SOURCE);
           // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
           idx_t res_count = data_woffset->data->Count();
           insert_chunk.SetCardinality(res_count);
           Vector in_index = data_woffset->data->GetVecRef(types[0], global_count);
           insert_chunk.data[0].Reference(in_index);
           insert_chunk.data[1].Sequence(global_count, 1, res_count); // out_index
           insert_chunk.data[2].Reference(thread_id_vec);  // thread_id
           break;
       }
*/

} // namespace duckdb
#endif
