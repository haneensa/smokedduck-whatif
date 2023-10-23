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
  if (type ==  PhysicalOperatorType::FILTER) {
    std::cout << "filter init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<FilterLog>();
  } else if (type ==  PhysicalOperatorType::TABLE_SCAN) {
    std::cout << "scan init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<TableScanLog>();
  } else if (type ==  PhysicalOperatorType::LIMIT || type == PhysicalOperatorType::STREAMING_LIMIT) {
    std::cout << "limit init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<LimitLog>();
  } else if (type ==  PhysicalOperatorType::ORDER_BY) {
    std::cout << "init log orderby " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<OrderByLog>();
  } else if (type ==  PhysicalOperatorType::CROSS_PRODUCT) {
    std::cout << "cross init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<CrossLog>();
  } else if (type ==  PhysicalOperatorType::PIECEWISE_MERGE_JOIN) {
    std::cout << "merge init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<MergeLog>();
  } else if (type ==  PhysicalOperatorType::NESTED_LOOP_JOIN) {
    std::cout << "nlj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<NLJLog>();
  } else if (type ==  PhysicalOperatorType::BLOCKWISE_NL_JOIN) {
    std::cout << "bnlj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<BNLJLog>();
  } else if (type ==  PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
    std::cout << "pha init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<PHALog>();
  } else if (type ==  PhysicalOperatorType::HASH_JOIN) {
    std::cout << "hj init log " << thread_id << std::endl;
    log_per_thread[thread_id] = make_shared<HashJoinLog>();
  } else {
    log_per_thread[thread_id] = make_shared<Log>();
  }
}

// FilterLog
idx_t FilterLog::Size() {
  return 0;
}

idx_t FilterLog::Count() {
  return 0;
}

idx_t FilterLog::ChunksCount() {
  return 0;
}
  
void FilterLog::BuildIndexes() {
}

// TableScanLog
idx_t TableScanLog::Size() {
  return 0;
}

idx_t TableScanLog::Count() {
  return 0;
}

idx_t TableScanLog::ChunksCount() {
  return 0;
}
  
void TableScanLog::BuildIndexes() {
}

// LimitLog
idx_t LimitLog::Size() {
  return 0;
}

idx_t LimitLog::Count() {
  return 0;
}

idx_t LimitLog::ChunksCount() {
  return 0;
}
  
void LimitLog::BuildIndexes() {
}

// OrderByLog
idx_t OrderByLog::Size() {
  return 0;
}

idx_t OrderByLog::Count() {
  return 0;
}

idx_t OrderByLog::ChunksCount() {
  return 0;
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
  return 0;
}
  
void HashJoinLog::BuildIndexes() {
  idx_t size = lineage_build.size();
  start_base = 0;
  last_base = 0;
  idx_t count_so_far = 0;
  offset = 0;
  // TODO: this ignore selection vector if exists
  // if sel vector exists, create hash map: addr -> id ?
  if (size > 0) {
    auto payload = (uint64_t*)(lineage_build[0].scatter.get());
    idx_t res_count = lineage_build[0].added_count;
    start_base = payload[0];
    last_base = payload[res_count - 1];
    hm_range.emplace_back(start_base, last_base);
    hash_chunk_count.push_back(0);
    if (offset == 0 && res_count > 1) {
      offset = payload[1] - payload[0];
    }
    count_so_far += res_count;
  }

  for (auto i=1; i < size; ++i) {
    // build hash table with range -> acc
    // if x in range -> then use range.start and adjust the value using acc
    auto payload = (uint64_t*)(lineage_build[i].scatter.get());
    idx_t res_count = lineage_build[i].added_count;
    if (offset == 0) offset = payload[res_count - 1] - start_base;
    auto diff = (payload[res_count - 1] - start_base) / offset;
    if (diff + 1 !=  count_so_far + res_count - hash_chunk_count.back()) {
      // update the range and log the old one
      // range -> count
      // if value fall in this range, then remove the start / offset
      for (idx_t j = 0; j < res_count; ++j) {
        auto f = ((payload[j] - start_base) / offset);
        auto s = count_so_far + j - hash_chunk_count.back();
        if ( f !=  s) {
          if (j > 1) {
            hm_range.back().second = payload[j - 1]; // the previous one
          }
          hash_chunk_count.push_back(count_so_far + j);
          start_base = payload[j];
          last_base = payload[res_count - 1];
          hm_range.emplace_back(start_base, last_base);
          break;
        }
      }
    } else {
      hm_range.back().second = payload[res_count - 1];
    }
    count_so_far += res_count;
  }

  /*
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

     */
}


} // namespace duckdb
#endif
