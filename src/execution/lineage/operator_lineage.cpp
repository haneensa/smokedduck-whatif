#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

idx_t OperatorLineage::Size() {
	idx_t size = 0;
/*	for (auto& log : log_per_thread) {
		size +=  log.second.GetLogSize(log.first);
	}*/

	return size;
}

idx_t Log::GetLatestLSN() {
	return 0;
}

idx_t Log::GetLogSizeBytes() {
	idx_t size_bytes = 0;
/*	for (idx_t i = 0; i < log->size(); i++) {
		for (const auto& lineage_data : log[i]) {
			size_bytes += lineage_data->data->Size();
		}
	}*/
	return size_bytes;
}
void OperatorLineage::InitLog(idx_t thread_id) {
  if (log_per_thread.find(thread_id) != log_per_thread.end()) {
    std::cout << "doublicate " << thread_id << std::endl;
    return;
  }
  thread_vec.push_back(thread_id);
  if (type ==  PhysicalOperatorType::FILTER) {
//    std::cout << "filter init log " << thread_id << std::endl;
	log_per_thread[thread_id] = make_shared<FilterLog>();
  } else if (type ==  PhysicalOperatorType::TABLE_SCAN) {
  //  std::cout << "scan init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<TableScanLog>();
  } else if (type ==  PhysicalOperatorType::LIMIT || type == PhysicalOperatorType::STREAMING_LIMIT) {
    //std::cout << "limit init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<LimitLog>();
  } else if (type ==  PhysicalOperatorType::ORDER_BY) {
    //std::cout << "init log orderby " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<OrderByLog>();
  } else if (type ==  PhysicalOperatorType::CROSS_PRODUCT) {
    //std::cout << "cross init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<CrossLog>();
  } else if (type ==  PhysicalOperatorType::PIECEWISE_MERGE_JOIN) {
    //std::cout << "merge init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<MergeLog>();
  } else if (type ==  PhysicalOperatorType::NESTED_LOOP_JOIN) {
    //std::cout << "nlj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<NLJLog>();
  } else if (type ==  PhysicalOperatorType::BLOCKWISE_NL_JOIN) {
    //std::cout << "bnlj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<BNLJLog>();
  } else if (type ==  PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
    //std::cout << "pha init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<PHALog>();
  } else if (type ==  PhysicalOperatorType::HASH_GROUP_BY) {
	  //std::cout << "ha init log " << thread_id << std::endl;
	  log_per_thread[thread_id] = make_shared<HALog>();
  } else if (type ==  PhysicalOperatorType::HASH_JOIN) {
    //std::cout << "hj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<HashJoinLog>();
  } else {
    log_per_thread[thread_id] = make_shared<Log>();
  }
}

// FilterLog
idx_t FilterLog::Size() {
  idx_t count = Count();
  idx_t size_bytes = count * sizeof(sel_t); 
  size_bytes += lineage.size() * sizeof(filter_artifact);

  return size_bytes;
}

idx_t FilterLog::Count() {
  idx_t count = 0;
  for (const auto& lineage_data : lineage) {
    count += lineage_data.count;
  }

  return count;
}

idx_t FilterLog::ChunksCount() {
  return lineage.size();
}
  
void FilterLog::BuildIndexes() {
  for (const auto& lineage_data : lineage) {
    if (lineage_data.sel != nullptr) {
      auto vec_ptr = lineage_data.sel.get();
      idx_t res_count = lineage_data.count;
      idx_t child_offset = lineage_data.child_offset;
      for (idx_t i = 0; i < res_count; i++) {
        *(vec_ptr + i) += child_offset;
      }
    }
  }
  processed = true;
}

// TableScanLog
idx_t TableScanLog::Size() {
  idx_t count = Count();
  idx_t size_bytes = count * sizeof(sel_t); 
  size_bytes += lineage.size() * sizeof(scan_artifact);

  return size_bytes;
}

idx_t TableScanLog::Count() {
  idx_t count = 0;
  for (const auto& lineage_data : lineage) {
    count += lineage_data.count;
  }

  return count;
}

idx_t TableScanLog::ChunksCount() {
  return lineage.size();
}
  
void TableScanLog::BuildIndexes() {
  for (const auto& lineage_data : lineage) {
    if (lineage_data.sel != nullptr) {
      auto vec_ptr = lineage_data.sel->owned_data.get();
      idx_t res_count = lineage_data.count;
      idx_t child_offset = lineage_data.start + lineage_data.vector_index;
      for (idx_t i = 0; i < res_count; i++) {
        *(vec_ptr + i) += child_offset;
      }
    }
  }
  processed = true;
}

// LimitLog
idx_t LimitLog::Size() {
  return lineage.size() * sizeof(limit_artifact);
}

idx_t LimitLog::Count() {
  idx_t count = 0;
  for (const auto& lineage_data : lineage) {
    count += lineage_data.end - lineage_data.start;
  }

  return count;
}

idx_t LimitLog::ChunksCount() {
  return lineage.size();
}
  
void LimitLog::BuildIndexes() {
}

// OrderByLog
idx_t OrderByLog::Size() {
  idx_t count = Count();
  idx_t size_bytes = count * sizeof(idx_t); 
  return size_bytes;
}

idx_t OrderByLog::Count() {
  idx_t count = 0;
  for (const auto& lineage_data : lineage) {
    count += lineage_data.size();
  }

  return count;
}

idx_t OrderByLog::ChunksCount() {
  return lineage.size();
}
  
void OrderByLog::BuildIndexes() {
}

// HashJoinLog
idx_t HashJoinLog::Size() {
  return 0;
}

idx_t HashJoinLog::Count() {
  return 0;
}

idx_t HashJoinLog::ChunksCount() {
  return lineage_binary.size();
}
  
void HashJoinLog::BuildIndexes() {
  idx_t count_so_far = 0;
  // if sel vector exists, create hash map: addr -> id ?
  for (idx_t i = 0; i < lineage_build.size(); i++) {
	idx_t res_count = lineage_build[i].added_count;
	auto payload = lineage_build[i].scatter.get();
	auto sel = lineage_build[i].sel;
	if (sel) {
		for (idx_t j = 0; j < res_count; j++) {
			hash_index[payload[j]] = sel->owned_data[j] + count_so_far;
		}
	} else {
		for (idx_t j = 0; j < res_count; j++) {
			hash_index[payload[j]] = j + count_so_far;
		}
	}

	count_so_far += res_count;
  }
}


// HashAggregateLog
idx_t HALog::Size() {
  return 0;
}

idx_t HALog::Count() {
  return 0;
}

idx_t HALog::ChunksCount() {
  return addchunk_log.size();
}

// TODO: an issue with multi-threading --  build could run on separate thread from scan
void HALog::BuildIndexes() {

  // TODO: detect if finalize exist

  // build side
  auto size = addchunk_log.size();
  idx_t count_so_far = 0;
  for (idx_t i=0; i < size; i++) {
	//if (sink_log[i].branch == 0) {
		idx_t res_count = addchunk_log[i].count;
		auto payload = addchunk_log[i].addchunk_lineage.get();
		for (idx_t j=0; j < res_count; ++j) {
			hash_index[payload[j]].push_back(j + count_so_far);
		}
		count_so_far += res_count;
	//}
  }
  processed = true;
}


// Perfect HashAggregateLog
idx_t PHALog::Size() {
  return 0;
}

idx_t PHALog::Count() {
  return 0;
}

idx_t PHALog::ChunksCount() {
  return 0;
}

void PHALog::BuildIndexes() {
  idx_t count_so_far = 0;
  for (idx_t i=0; i < build_lineage.size(); i++) {
	vector<uint32_t> &payload = build_lineage[i];
	for (idx_t i = 0; i < payload.size(); ++i) {
		auto val = i + count_so_far;
		hash_index[payload[i]].push_back(val);
	}
	count_so_far += payload.size();
  }
}


} // namespace duckdb
#endif
