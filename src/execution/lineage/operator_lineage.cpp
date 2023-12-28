#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

void OperatorLineage::PostProcess() {
	if (processed)	return;
	// 1. build indexes
	log_index = make_shared<LogIndex>();
	for (idx_t i : thread_vec) {
		std::cout << 'thread id ' << i <<std::endl;
		log_per_thread[i]->BuildIndexes(log_index);
	}

	// 2. adjust offsets for cheaper retrieval
	for (idx_t i : thread_vec) {
		std::cout << 'thread id ' << i <<std::endl;
		log_per_thread[i]->PostProcess(log_index);
	}

	processed = true;
}

void OperatorLineage::InitLog(idx_t thread_id, PhysicalOperator* op) {
  if (trace_lineage == false) return;

  if (log_per_thread.find(thread_id) != log_per_thread.end()) {
    std::cout << "doublicate " << thread_id << std::endl;
    return;
  }
  // TODO: add lock
  thread_vec.push_back(thread_id);
  if (type ==  PhysicalOperatorType::FILTER) {
//    std::cout << "filter init log " << thread_id << std::endl;
	log_per_thread[thread_id] = make_shared<FilterLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::TABLE_SCAN) {
  //  std::cout << "scan init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<TableScanLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::LIMIT || type == PhysicalOperatorType::STREAMING_LIMIT) {
    //std::cout << "limit init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<LimitLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::ORDER_BY) {
    //std::cout << "init log orderby " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<OrderByLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::CROSS_PRODUCT) {
    //std::cout << "cross init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<CrossLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::PIECEWISE_MERGE_JOIN) {
    //std::cout << "merge init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<MergeLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::NESTED_LOOP_JOIN) {
    //std::cout << "nlj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<NLJLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::BLOCKWISE_NL_JOIN) {
    //std::cout << "bnlj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<BNLJLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
    //std::cout << "pha init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<PHALog>(thread_id);
  } else if (type ==  PhysicalOperatorType::HASH_GROUP_BY) {
	  //std::cout << "ha init log " << thread_id << std::endl;
	  log_per_thread[thread_id] = make_shared<HALog>(thread_id);
  } else if (type ==  PhysicalOperatorType::HASH_JOIN) {
    //std::cout << "hj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<HashJoinLog>(thread_id);
  } else if (type ==  PhysicalOperatorType::DELIM_JOIN) {
	//std::cout << "hj init log " << thread_id << std::endl;
	auto distinct = (PhysicalOperator*)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get();
	distinct->lineage_op->InitLog(thread_id);
  } else {
    log_per_thread[thread_id] = make_shared<Log>(thread_id);
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

void FilterLog::BuildIndexes(shared_ptr<LogIndex> logIdx) {
}

void FilterLog::PostProcess(shared_ptr<LogIndex> logIdx) {
  if (processed) return;

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

void TableScanLog::BuildIndexes(shared_ptr<LogIndex> logIdx) {
}

void TableScanLog::PostProcess(shared_ptr<LogIndex> logIdx) {
  if (processed) return;

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
  
void LimitLog::BuildIndexes(shared_ptr<LogIndex> logIdx) {
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
  
void HashJoinLog::BuildIndexes(shared_ptr<LogIndex> logIdx) {
  idx_t count_so_far = 0;
  for (idx_t i = 0; i < lineage_build.size(); i++) {
    logIdx->arraySize += lineage_build[i].added_count;
  }

  logIdx->index_hj.resize(logIdx->arraySize);

  // if sel vector exists, create hash map: addr -> id ?
  for (idx_t i = 0; i < lineage_build.size(); i++) {
    idx_t res_count = lineage_build[i].added_count;
    data_ptr_t* payload = lineage_build[i].scatter.get();
    auto sel = lineage_build[i].sel;
    if (sel) {
      for (idx_t j = 0; j < res_count; j++) {
      std::uintptr_t addrValue = reinterpret_cast<std::uintptr_t>(payload[j]);
      idx_t hash = addrValue % logIdx->arraySize;
      logIdx->index_hj[hash].push_back({sel->owned_data[j]+count_so_far, payload[j]});
      }
    } else {
      for (idx_t j = 0; j < res_count; j++) {
        std::uintptr_t addrValue = reinterpret_cast<std::uintptr_t>(payload[j]);
        idx_t hash = addrValue % logIdx->arraySize;
        logIdx->index_hj[hash].push_back({j+count_so_far, payload[j]});
      }
    }

    count_so_far += res_count;
  }

  if (lineage_finalize.size() > 0) {
	for (idx_t i=0; i < lineage_finalize.back().added_count; i++) {
	  logIdx->perfect_hash_join_finalize[ lineage_finalize.back().sel->owned_data[i] ] = lineage_finalize.back().scatter[i];
	}
  }
}

void HashJoinLog::PostProcess(shared_ptr<LogIndex> logIdx) {
  if (processed) return;
  logIdx->right_val_log[thid].resize(GetLatestLSN()  + logIdx->right_val_log[thid].size() );
  for (idx_t i=0; i < output_index.size(); ++i) {
	idx_t lsn = output_index[i].first;
	if (lsn == 0) { // something is wrong
	  std::cout << "lsn == 0 for " << i <<  std::endl;
	  break;
	}
	lsn -= 1;
	idx_t child_offset = output_index[i].second;
	idx_t res_count = lineage_binary[lsn].count;
	if (lineage_binary[lsn].left != nullptr) {
	  auto vec_ptr = lineage_binary[lsn].left.get();
	  for (idx_t i = 0; i < res_count; i++) {
		*(vec_ptr + i) += child_offset;
	  }
	}

	if (lineage_binary[lsn].branch == 1) {
		auto vec_ptr = lineage_binary[lsn].perfect_right.get();
		for (idx_t i=0; i < res_count; i++) {
			auto idx = lineage_binary[lsn].perfect_right[i];
			std::uintptr_t scatter_idx = (std::uintptr_t)logIdx->perfect_hash_join_finalize[idx];
			idx_t hash = scatter_idx % logIdx->arraySize;
			for (auto k=0; k < logIdx->index_hj[hash].size(); ++k) {
				if (logIdx->index_hj[hash][k].second == (data_ptr_t)scatter_idx) {
					*(vec_ptr + i) = logIdx->index_hj[hash][k].first;
					std::cout << logIdx->index_hj[hash][k].first << std::endl;
				}
			}
		}
	} else {
		data_ptr_t* right_build_ptr = lineage_binary[lsn].right.get();
		if (logIdx->right_val_log[thid][lsn].get() == nullptr) {
			unique_ptr<sel_t[]>  right_val(new sel_t[res_count]);
			for (idx_t i=0; i < res_count; i++) {
				std::uintptr_t addrValue = reinterpret_cast<std::uintptr_t>(right_build_ptr[i]);
				idx_t hash = addrValue % logIdx->arraySize;
				for (auto k=0; k < logIdx->index_hj[hash].size(); ++k) {
				  if (logIdx->index_hj[hash][k].second == right_build_ptr[i]) {
					  right_val[i] = logIdx->index_hj[hash][k].first;
					  std::cout << thid << " " << res_count << " " << i << " " << (idx_t) right_val[i] << std::endl;
				  }
				}
			}
			logIdx->right_val_log[thid][lsn] = move(right_val);
		}
	}
  }
  processed = true;
}

// NLJ

void NLJLog::PostProcess(shared_ptr<LogIndex> logIdx) {
  if (processed) return;

  for (const auto& lineage_data : output_index) {
	idx_t lsn = lineage_data.first;
	idx_t child_offset = lineage_data.second;
	if (lsn == 0) { // something is wrong
		continue;
	}

	lsn -= 1;

	if (lineage[lsn].left != nullptr) {
		auto vec_ptr = lineage[lsn].left->owned_data.get();
		idx_t res_count = lineage[lsn].count;
		for (idx_t i = 0; i < res_count; i++) {
			*(vec_ptr + i) += child_offset;
		}
	}


	if (lineage[lsn].right != nullptr && lineage[lsn].current_row_index != 0) {
		auto vec_ptr = lineage[lsn].right->owned_data.get();
		idx_t res_count = lineage[lsn].count;
		for (idx_t i = 0; i < res_count; i++) {
			*(vec_ptr + i) += lineage[lsn].current_row_index;
		}
	}
  }
  processed = true;
}

// BNLJ

void BNLJLog::PostProcess(shared_ptr<LogIndex> logIdx) {
  if (processed) return;

  for (const auto& lineage_data : output_index) {
	idx_t lsn = lineage_data.first;
	idx_t child_offset = lineage_data.second;
	if (lsn == 0) { // something is wrong
		continue;
	}

	lsn -= 1;
	if (lineage[lsn].sel && lineage[lsn].branch_scanlhs == false &&  lineage[lsn].branch < 2) {
		auto vec_ptr = lineage[lsn].sel->owned_data.get();
		idx_t res_count = lineage[lsn].count;
		for (idx_t i = 0; i < res_count; i++) {
			*(vec_ptr + i) += child_offset;
		}
	}
  }
  processed = true;
}

// Merge Join

void MergeLog::BuildIndexes(shared_ptr<LogIndex> logIdx) {
  if (combine.empty()) return;

  logIdx->sort = move(combine.back());
}

void MergeLog::PostProcess(shared_ptr<LogIndex> logIdx) {
  if (processed) return;

  for (const auto& lineage_data : output_index) {
	idx_t lsn = lineage_data.first;
	idx_t child_offset = lineage_data.second;
	if (lsn == 0) { // something is wrong
		continue;
	}

	lsn -= 1;

	if (lineage[lsn].left) {
		auto vec_ptr =lineage[lsn].left->owned_data.get();
		idx_t res_count = lineage[lsn].count;
		for (idx_t i = 0; i < res_count; i++) {
			*(vec_ptr + i) += child_offset;
		}
	}

	if (lineage[lsn].right && (lineage[lsn].branch == 1 || lineage[lsn].branch == 4)) {
		auto vec_ptr =lineage[lsn].right->owned_data.get();
		idx_t res_count = lineage[lsn].count;
		for (idx_t i = 0; i < res_count; i++) {
			*(vec_ptr + i) +=  lineage[lsn].right_chunk_index;
		}
	}
  }
  processed = true;
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
void HALog::BuildIndexes(shared_ptr<LogIndex> logIdx) {
  // TODO: detect if finalize exist
  // build side
  for (auto g=0; g < grouping_set.size(); g++) {
	auto size = grouping_set[g].size();
	idx_t count_so_far = 0;
	for (idx_t i=0; i < size; i++) {
		//if (sink_log[i].branch == 0) {
		auto lsn = grouping_set[g][i];
		if (lsn == 0) {
			std::cout << "HALog::BuildIndexes: grouping_set lsn 0" << std::endl;
			return;
		}
		lsn -= 1;
		idx_t res_count = addchunk_log[lsn].count;
		auto payload = addchunk_log[lsn].addchunk_lineage.get();
		for (idx_t j=0; j < res_count; ++j) {
			logIdx->ha_hash_index[payload[j]].push_back(j + count_so_far);
		}
		count_so_far += res_count;
		//}
	}
  }
  // go over distinct_scan, distinct_sink
  // for each element in distinct sink, add it to HT. with value as distinct_ht[distinct_index[i]]
  if (grouping_set.empty() == false) return;
  for (auto g=0; g < distinct_index.size(); g++) {
	auto size = distinct_index[g].size();
	if (logIdx->distinct_count.find(g) == logIdx->distinct_count.end()) {
		logIdx->distinct_count[g] = 0;
	}
	idx_t count_so_far = logIdx->distinct_count[g];
	for (idx_t i=0; i < size; i++) {
		//if (sink_log[i].branch == 0) {
		auto lsn = distinct_index[g][i];
		if (lsn == 0) {
			std::cout << "HALog::BuildIndexes: distinct_index lsn 0" << std::endl;
			return;
		}
		lsn -= 1;
		idx_t res_count = addchunk_log[lsn].count;
		auto payload = addchunk_log[lsn].addchunk_lineage.get();
		for (idx_t j=0; j < res_count; ++j) {
			logIdx->ha_distinct_hash_index[payload[j]].push_back(j + count_so_far);
		}
		count_so_far += res_count;
		//}
	}
	logIdx->distinct_count[g] = count_so_far;
  }
}

void HALog::PostProcess(shared_ptr<LogIndex> logIdx) {
  if (grouping_set.empty() == false) return;

  // go over distinct_scan, distinct_sink
  // for each element in distinct sink, add it to HT. with value as distinct_ht[distinct_index[i]]
  for (auto g=0; g < distinct_scan.size(); g++) {
	auto size = distinct_scan[g].size();
	idx_t count_so_far = 0;
	for (idx_t i=0; i < size; i++) {
		//if (sink_log[i].branch == 0) {
		auto lsn = distinct_scan[g][i];
		auto sink_lsn = distinct_sink[g][i];
		if (lsn == 0 || sink_lsn == 0) {
			std::cout << "HALog::BuildIndexes: distinct_index lsn 0" << std::endl;
			return;
		}
		lsn -= 1;
		sink_lsn -= 1;
		idx_t res_count = scan_log[lsn].count;
		auto payload = scan_log[lsn].addchunk_lineage.get();
		auto sink_payload = addchunk_log[sink_lsn].addchunk_lineage.get();
		for (idx_t j=0; j < res_count; ++j) {
			logIdx->ha_hash_index[sink_payload[j]].insert(logIdx->ha_hash_index[sink_payload[j]].end(),
				                                          logIdx->ha_distinct_hash_index[payload[j]].begin(),
				                                          logIdx->ha_distinct_hash_index[payload[j]].end());
		}
		count_so_far += res_count;
		//}
	}
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

void PHALog::BuildIndexes(shared_ptr<LogIndex> logIdx) {
  idx_t count_so_far = 0;
  for (idx_t i=0; i < build_lineage.size(); i++) {
	vector<uint32_t> &payload = build_lineage[i];
	for (idx_t i = 0; i < payload.size(); ++i) {
		auto val = i + count_so_far;
		logIdx->pha_hash_index[payload[i]].push_back(val);
	}
	count_so_far += payload.size();
  }
}


} // namespace duckdb
#endif
