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

	if (type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN)
	  source.emplace_back("lhs_index", LogicalType::BIGINT);
	else
		source.emplace_back("lhs_index", LogicalType::INTEGER);
	if (type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN)
		source.emplace_back("rhs_index", LogicalType::BIGINT);
	else
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
  log_per_thread[thread_val]->GetLineageAsChunk(insert_chunk, global_count, local_count, data_idx,
	                                            cache_offset, cache_size, cache, log_index);
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
                                   idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                   shared_ptr<LogIndex> logIdx) {
  if (data_idx >= output_index.size()) {
	return 0;
  }

  idx_t lsn = output_index[data_idx].first;
  if (lsn == 0) { // something is wrong
	return 0;
  }

  lsn -= 1;


  idx_t res_count = lineage[lsn].count;
  idx_t child_offset = lineage[lsn].child_offset;
  data_ptr_t ptr = nullptr;
  if (lineage[lsn].sel != nullptr) {
    auto vec_ptr = lineage[lsn].sel.get();
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
                                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                      shared_ptr<LogIndex> logIdx) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }
    
  idx_t res_count = lineage[data_idx].count;
  idx_t child_offset = lineage[data_idx].start + lineage[data_idx].vector_index;
  data_ptr_t ptr = nullptr;
  if (lineage[data_idx].sel != nullptr) {
    auto vec_ptr = lineage[data_idx].sel->owned_data.get();
    ptr = (data_ptr_t)vec_ptr;
  }
  getchunk(res_count, global_count, insert_chunk,  ptr, child_offset);

  data_idx++;

  return res_count;
}

// Limit
// schema: [INTEGER in_index, INTEGER out_index]
idx_t LimitLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                  idx_t& global_count, idx_t& local_count,
                                  idx_t& data_idx,
                                  idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                  shared_ptr<LogIndex> logIdx) {
  
  if (data_idx >= lineage.size()) {
	  return 0;
  }
  
  idx_t start = lineage[data_idx].start;
  idx_t res_count = lineage[data_idx].end;
  idx_t offset = lineage[data_idx].child_offset;
  insert_chunk.SetCardinality(res_count);
  insert_chunk.data[0].Sequence(start+offset, 1, res_count); // in_index
  insert_chunk.data[1].Sequence(global_count, 1, res_count); // out_index
  data_idx++;
  return res_count;
}
    
// Order By
// schema: [INTEGER in_index, INTEGER out_index]
idx_t OrderByLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                    idx_t& global_count, idx_t& local_count,
                                    idx_t& data_idx,
                                    idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                    shared_ptr<LogIndex> logIdx) {
  
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
    } else {
	  data_idx++;
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
                                  idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                  shared_ptr<LogIndex> logIdx) {
  
  if (data_idx >= output_index.size()) {
	return 0;
  }

  idx_t lsn = output_index[data_idx].first;
  if (lsn == 0) { // something is wrong
	return 0;
  }

  lsn -= 1;

  idx_t branch_scan_lhs = lineage[lsn].branch_scan_lhs;
  idx_t res_count = lineage[lsn].count;
  idx_t out_start = lineage[lsn].out_start;
  idx_t position_in_chunk = lineage[lsn].position_in_chunk;
  idx_t scan_position = lineage[lsn].scan_position;

  if (branch_scan_lhs == false) {
    Vector rhs_payload(Value::Value::INTEGER(scan_position + position_in_chunk));
    Vector lhs_payload(LogicalType::INTEGER, res_count);
    lhs_payload.Sequence(out_start, 1, res_count);
    fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, global_count);
  } else {
    Vector rhs_payload(LogicalType::INTEGER, res_count);
    Vector lhs_payload(Value::Value::INTEGER(position_in_chunk + out_start));
    rhs_payload.Sequence(scan_position, 1, res_count);
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
                                idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                shared_ptr<LogIndex> logIdx) {
  // TODO: add instart offset
  if (data_idx >= output_index.size()) {
	return 0;
  }

  idx_t lsn = output_index[data_idx].first;
  if (lsn == 0) { // something is wrong
	return 0;
  }

  lsn -= 1;

  idx_t res_count = lineage[lsn].count;
  //idx_t out_start = lineage[data_idx].out_start;
  //idx_t current_row_index = lineage[data_idx].current_row_index;
  Vector lhs_payload(LogicalType::INTEGER);
  Vector rhs_payload(LogicalType::INTEGER);
  if (lineage[lsn].left) {
	  data_ptr_t left_ptr = (data_ptr_t)lineage[lsn].left->owned_data.get();
	  Vector temp(LogicalType::INTEGER, left_ptr);
	  lhs_payload.Reference(temp);
  } else {
	  lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
	  ConstantVector::SetNull(lhs_payload, true);
  }
  if (lineage[lsn].right) {
	  data_ptr_t right_ptr = (data_ptr_t)lineage[lsn].right->owned_data.get();
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
                                 idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                 shared_ptr<LogIndex> logIdx) {
  // TODO: add instart offset
  if (data_idx >= output_index.size()) {
	  return 0;
  }

  idx_t lsn = output_index[data_idx].first;
  if (lsn == 0) { // something is wrong
	  return 0;
  }

  lsn -= 1;

  idx_t res_count = lineage[lsn].count;
  Vector lhs_payload(LogicalType::INTEGER);
  Vector rhs_payload(LogicalType::INTEGER);
  idx_t branch = lineage[lsn].branch;
  data_ptr_t data_ptr = (data_ptr_t)lineage[lsn].sel->owned_data.get();
  Vector temp(LogicalType::INTEGER, data_ptr);
  if (lineage[lsn].branch_scanlhs == false && branch < 2) {
	  lhs_payload.Reference(temp);
  } else if (branch < 2) {
	  lhs_payload.Reference(Value::INTEGER(lineage[lsn].scan_position+lineage[lsn].inchunk));
  } else if (branch == 2) {
	  lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
	  ConstantVector::SetNull(lhs_payload, true);
  }

  if ((lineage[lsn].branch_scanlhs == true && branch == 0) || branch == 2) {
	  rhs_payload.Reference(temp);
  } else if (lineage[lsn].branch_scanlhs == false && branch == 0){
	  rhs_payload.Reference(Value::INTEGER(lineage[lsn].scan_position+lineage[lsn].inchunk));
  } else if (branch == 1) {
	  rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
	  ConstantVector::SetNull(rhs_payload, true);
  }

  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, global_count);
  data_idx++;
  std::cout << insert_chunk.ToString() << std::endl;
  return res_count;
}
    
// Merge
// schema: [INTEGER lhs_index, INTEGER rhs_index, INTEGER out_index]
idx_t MergeLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                  idx_t& global_count, idx_t& local_count,
                                  idx_t& data_idx,
                                  idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                  shared_ptr<LogIndex> logIdx) {
  if (data_idx >= output_index.size()) {
	  return 0;
  }

  idx_t lsn = output_index[data_idx].first;
  if (lsn == 0) { // something is wrong
	  return 0;
  }

  lsn -= 1;

  idx_t res_count = lineage[lsn].count;
  Vector lhs_payload(LogicalType::BIGINT);
  Vector rhs_payload(LogicalType::BIGINT);
  idx_t branch = lineage[lsn].branch;
  // lhs
  if (branch == 1 || branch == 2 || branch == 3) {
	  Vector temp1(LogicalType::BIGINT, (data_ptr_t)lineage[data_idx].lhs_sort.back().data());
	  // add in_start
	  std::cout << "in start :" << lineage[lsn].out_start << std::endl;
	  auto sel = SelectionVector(lineage[lsn].left->owned_data.get());
	  temp1.Slice(SelectionVector(lineage[lsn].left->owned_data.get()), res_count);
	  std::cout << temp1.ToString(res_count) << std::endl;
	  lhs_payload.Reference(temp1);
  } else if (branch == 4) {
	  lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
	  ConstantVector::SetNull(lhs_payload, true);
  }

  // rhs
  if (branch == 1 || branch == 4) {
	  // TODO: apply sorting from combine and lhs
	  std::cout << "state.right_base:" << lineage[lsn].right_chunk_index << std::endl;
	  Vector temp2(LogicalType::BIGINT, (data_ptr_t)logIdx->sort.data());
	  temp2.Slice(SelectionVector(lineage[lsn].right->owned_data.get()), res_count);
	  std::cout << temp2.ToString(res_count) << std::endl;
	  rhs_payload.Reference(temp2);
  } else if (branch == 2 || branch == 3) {
	  rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
	  ConstantVector::SetNull(rhs_payload, true);
  }

  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, global_count);
  data_idx++;
  std::cout << insert_chunk.ToString() << std::endl;
  return res_count;
}

// Hash Join
// schema: [INTEGER lhs_index, INTEGER rhs_index, INTEGER out_index]
idx_t HashJoinLog::GetLineageAsChunk(DataChunk &insert_chunk,
                                     idx_t& global_count, idx_t& local_count,
                                     idx_t& data_idx,
                                     idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                     shared_ptr<LogIndex> logIdx) {
  
  if (data_idx >= output_index.size()) {
	  return 0;
  }

  idx_t lsn = output_index[data_idx].first;
  if (lsn == 0) { // something is wrong
	  return 0;
  }

  lsn -= 1;

  idx_t res_count = lineage_binary[lsn].count;
  data_ptr_t left_ptr = (data_ptr_t)lineage_binary[lsn].left.get();
  Vector lhs_payload(LogicalType::INTEGER);

  // Left side / probe side

  // check if all lhs is included

  if (left_ptr == nullptr || lineage_binary[lsn].branch == 2) {
    if (res_count == STANDARD_VECTOR_SIZE || lineage_binary[lsn].branch == 2) {
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
  
  Vector rhs_payload(LogicalType::INTEGER);
  data_ptr_t right_ptr;

  if (lineage_binary[lsn].branch == 1) {
	right_ptr = (data_ptr_t)lineage_binary[lsn].perfect_right.get();
	Vector temp(LogicalType::INTEGER, right_ptr);
	rhs_payload.Reference(temp);
  } else {
	data_ptr_t* right_build_ptr = lineage_binary[lsn].right.get();

	// Right side / build side
	if (right_build_ptr == nullptr) {
	  rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
	  ConstantVector::SetNull(rhs_payload, true);
	} else {
	  Vector temp(LogicalType::INTEGER, (data_ptr_t)logIdx->right_val_log[thid][lsn].get());
	  rhs_payload.Reference(temp);
	}
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
                               idx_t &cache_offset, idx_t &cache_size, bool &cache,
                               shared_ptr<LogIndex> logIdx) {

  if (global_count == 0) key_offset = 0;
	if (data_idx >= output_index.size()) {
		return 0;
	}

	idx_t lsn = output_index[data_idx].first;
	if (lsn == 0) { // something is wrong
		return 0;
	}

	lsn -= 1;

	idx_t scan_count = scan_log[lsn].count;
	if (current_key >= scan_count) {
		data_idx++;
    	key_offset += current_key;
		current_key = 0;
		return 0;
	}
	data_ptr_t* payload = scan_log[lsn].addchunk_lineage.get();
	data_ptr_t output_key = payload[current_key];
	// current scan , current offset into scan, current offset into groups of scan
	vector<idx_t>& la = logIdx->ha_hash_index[output_key];
	// read from offset_within_key to max(1024, la.size());
	idx_t end_offset = la.size() - offset_within_key;
	if (end_offset > STANDARD_VECTOR_SIZE) {
		end_offset = STANDARD_VECTOR_SIZE;
	}
	insert_chunk.SetCardinality(end_offset);
	data_ptr_t ptr = (data_ptr_t)(la.data() + offset_within_key);
	Vector in_index(LogicalType::BIGINT, ptr);
	// use in index to loop up hash_map_agg
	insert_chunk.data[0].Reference(in_index);
	insert_chunk.data[1].Reference(Value::INTEGER(current_key + key_offset)); // out_index

 	offset_within_key += end_offset;
	if (offset_within_key >= la.size()) {
		offset_within_key = 0;
		current_key++;
	}
	if (current_key >= scan_log[lsn].count) {
		cache = false;
    key_offset += current_key;
		current_key = 0;
		data_idx++;
	} else {
		cache = true;
	}

	return end_offset;
}


// Perfect Hash Agg
// schema: [INTEGER in_index, INTEGER out_index]
idx_t PHALog::GetLineageAsChunk(DataChunk &insert_chunk,
                                idx_t& global_count, idx_t& local_count,
                                idx_t& data_idx,
                                idx_t &cache_offset, idx_t &cache_size, bool &cache,
                                shared_ptr<LogIndex> logIdx) {

  if (global_count == 0) key_offset = 0;
	if (data_idx >= scan_lineage.size()) {
		return 0;
	}

	idx_t scan_count = scan_lineage[data_idx].count;
	if (current_key >= scan_count) {
		data_idx++;
    key_offset += current_key;
		current_key = 0;
		return 0;
	}
	//idx_t scan_count = scan_lineage[data_idx].count;
	uint32_t* payload = scan_lineage[data_idx].gather.get();
	uint32_t output_key = payload[current_key];
	// current scan , current offset into scan, current offset into groups of scan
	vector<idx_t>& la = logIdx->pha_hash_index[output_key];
	// read from offset_within_key to max(1024, la.size());
	idx_t end_offset = la.size() - offset_within_key;
	if (end_offset > STANDARD_VECTOR_SIZE) {
		end_offset = STANDARD_VECTOR_SIZE;
	}
	insert_chunk.SetCardinality(end_offset);
	data_ptr_t ptr = (data_ptr_t)(la.data() + offset_within_key);
	Vector in_index(LogicalType::BIGINT, ptr);
	// use in index to loop up hash_map_agg
	insert_chunk.data[0].Reference(in_index);
	insert_chunk.data[1].Reference(Value::INTEGER(current_key + key_offset)); // out_index

	global_count += end_offset;
	offset_within_key += end_offset;
	if (offset_within_key >= la.size()) {
		offset_within_key = 0;
		current_key++;
	}
	if (current_key >= scan_lineage[data_idx].count) {
		cache = false;
    key_offset += current_key;
		current_key = 0;
		data_idx++;
	} else {
		cache = true;
	}

	return end_offset;
}

} // namespace duckdb
#endif
