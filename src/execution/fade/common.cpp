#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include <fstream>
#include <dlfcn.h>
#include <immintrin.h>
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include <thread>
#include <vector>

namespace duckdb {

void* Fade::compile(std::string code, int id) {
	// Write the loop code to a temporary file
	std::ofstream file("loop.cpp");
	file << code;
	file.close();
	const char* duckdb_lib_path = std::getenv("DUCKDB_LIB_PATH");
	if (duckdb_lib_path == nullptr) {
		// Handle error: environment variable not set
		std::cout << "DUCKDB_LIB_PATH undefined"<< std::endl;
		return nullptr;
	} else {
		std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
		std::string build_command = "g++ -O3 -std=c++2a -mavx512f -march=native -shared -fPIC loop.cpp -o loop.so -L" + std::string(duckdb_lib_path) + " -lduckdb";
		std::cout << duckdb_lib_path << " " << build_command.c_str() << std::endl;
		system(build_command.c_str());
		void *handle = dlopen("./loop.so", RTLD_LAZY);
		if (!handle) {
			std::cerr << "Cannot Open Library: " << dlerror() << std::endl;
		}
		return handle;
	}
}

std::unordered_map<std::string, float> Fade::parseWhatifString(EvalConfig& config) {
	std::unordered_map<std::string, float> result;
	std::istringstream iss(config.columns_spec_str);
	std::string token;

	while (std::getline(iss, token, '|')) {
		std::istringstream tokenStream(token);
		std::string table, prob;

		if (std::getline(tokenStream, table, ':')) {
			if (std::getline(tokenStream, prob)) {
				result[table] = std::stof(prob);
			}
		}
	}

	return result;
}

void Fade::FillFilterLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	vector<int>& lineage =  fade_data[op->id].lineage[0];
	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	idx_t offset = 0;

	idx_t row_count = lop->log_index->table_size;
	lineage.resize(row_count);

	do {
		cache_on = false;
		result.Reset();
		result.Destroy();
		op->lineage_op->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
		result.Flatten();
		if (result.size() == 0) continue;
		unsigned int * in_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
		for (idx_t i=0; i < result.size(); ++i) {
			idx_t iid = in_index[i];
			lineage[i+offset] = iid;
		}
		offset = result.size();
	} while (cache_on || result.size() > 0);
}

void Fade::FillJoinLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop, std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	vector<int>& lhs_lineage =  fade_data[op->id].lineage[0];
	vector<int>& rhs_lineage =  fade_data[op->id].lineage[1];
	int lhs_masks = fade_data[op->children[0]->id].n_masks;
	int rhs_masks = fade_data[op->children[1]->id].n_masks;
	idx_t row_count = lop->log_index->table_size;
	//if (lhs_masks > 0 && rhs_masks > 0) {
		lhs_lineage.resize(row_count);
		rhs_lineage.resize(row_count);
	/*} else if (lhs_masks > 0) {
		lhs_lineage.resize(row_count);
	} else {
		rhs_lineage.resize(row_count);
	}*/
	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	idx_t offset = 0;
	do {
		cache_on = false;
		result.Reset();
		result.Destroy();
		lop->GetLineageAsChunk(result, global_count, local_count, current_thread, log_id, cache_on);
		result.Flatten();
		if (result.size() == 0) continue;
		unsigned int * lhs_index = reinterpret_cast<unsigned int *>(result.data[0].GetData());
		unsigned int * rhs_index = reinterpret_cast<unsigned int *>(result.data[1].GetData());
	//	if (lhs_masks > 0 && rhs_masks > 0) {
			for (idx_t i=0; i < result.size(); ++i) {
				lhs_lineage[i+offset] = lhs_index[i];
				rhs_lineage[i+offset] = rhs_index[i];
			}
	/*	} else if (lhs_masks > 0) {
			for (idx_t i=0; i < result.size(); ++i) {
				lhs_lineage[i+offset] = lhs_index[i];
			}
		} else {
			for (idx_t i=0; i < result.size(); ++i) {
				rhs_lineage[i+offset] = rhs_index[i];
			}
		}*/
		offset += result.size();
	} while (cache_on || result.size() > 0);
}

std::vector<int> Fade::GetGBLineage(shared_ptr<OperatorLineage> lop, int row_count) {
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	bool cache_on = false;
	std::vector<int> lineage(row_count);
	do {
		cache_on = false;
		result.Reset();
		result.Destroy();
		lop->GetLineageAsChunk(result, global_count, local_count,
		                       current_thread, log_id, cache_on);
		result.Flatten();
		int64_t * in_index = reinterpret_cast<int64_t *>(result.data[0].GetData());
		int * out_index = reinterpret_cast<int *>(result.data[1].GetData());
		for (idx_t i=0; i < result.size(); ++i) {
			idx_t iid = in_index[i];
			idx_t oid = out_index[i];
			lineage[iid] = oid;
		}
	} while (cache_on || result.size() > 0);
	return lineage;
}

void Fade::PruneLineage(EvalConfig& config, PhysicalOperator* op,
                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                  vector<int>& out_order) {
	// top down.
	// lineage_data = self.prune_lineage(lineage_data, out_order, node_type)
	vector<int> new_order[2];

	if (out_order.empty() == false) {
		// if table scan and no filter push down, then use out_order aslineage
		// if hash group by: ..
		if (op->type == PhysicalOperatorType::TABLE_SCAN) {
			fade_data[op->id].lineage[0] = out_order;
		} else if (op->type == PhysicalOperatorType::FILTER) {
			vector<int> new_lineage(out_order.size());
			for (int i=0; i < out_order.size(); ++i) {
				new_lineage[i] = fade_data[op->id].lineage[0][out_order[i]];
			}
			fade_data[op->id].lineage[0] = std::move(new_lineage);
		} else if (op->type == PhysicalOperatorType::HASH_JOIN
		           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
		           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
		           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
		           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
			for (int side=0; side < 2; ++side) {
				vector<int> new_lineage(out_order.size());
				for (int i=0; i < out_order.size(); ++i) {
					new_lineage[i] = fade_data[op->id].lineage[side][out_order[i]];
				}
				// update lineage
				fade_data[op->id].lineage[side] = std::move(new_lineage);
			}
		}
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		for (int i=0; i < fade_data[op->id].lineage[0].size(); ++i) {
			new_order[0].push_back(i);
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {

		for (int side=0; side < 2; ++side) {
			vector<int>& lineage = fade_data[op->id].lineage[side];
			// 1. find how many unique elements in lhs_lineage, rhs_lineage
			set<int> lineage_unqiue_set(lineage.begin(), lineage.end());
			vector<int> lineage_unique(lineage_unqiue_set.begin(), lineage_unqiue_set.end());
			std::map<int, int> lineage_inverse_map;
			for (int i = 0; i < lineage_unique.size(); ++i) {
				lineage_inverse_map[lineage_unique[i]] = i;
			}
			vector<int>  lineage_inverse(lineage.size());
			for (int i = 0; i < lineage.size(); ++i) {
				lineage_inverse[i] = lineage_inverse_map[lineage[i]];
			}
			// update lineage
			fade_data[op->id].lineage[side] = std::move(lineage_inverse);
			new_order[side] = lineage_unique;
		}

	}


	for (idx_t i = 0; i < op->children.size(); i++) {
		Fade::PruneLineage(config, op->children[i].get(), fade_data, new_order[i]);
	}
}

void Fade::GetLineage(EvalConfig& config, PhysicalOperator* op,
                     std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                     std::unordered_map<std::string, float> columns_spec) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GetLineage(config, op->children[i].get(), fade_data, columns_spec);
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
	} else if (op->type == PhysicalOperatorType::FILTER) {
		FillFilterLineage(op, op->lineage_op, fade_data);
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		FillJoinLineage(op, op->lineage_op, fade_data);
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
	} else if (op->type == PhysicalOperatorType::PROJECTION) {
	}
}


template<class T1, class T2>
T2* Fade::GetInputVals(PhysicalOperator* op, shared_ptr<OperatorLineage> lop, idx_t col_idx) {
	idx_t chunk_count = op->children[0]->lineage_op->chunk_collection.ChunkCount();
	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	T2* input_values = new T2[row_count];

	idx_t offset = 0;
	for (idx_t chunk_idx=0; chunk_idx < chunk_count; ++chunk_idx) {
		DataChunk &collection_chunk = op->children[0]->lineage_op->chunk_collection.GetChunk(chunk_idx);
		T1* col = reinterpret_cast<T1*>(collection_chunk.data[col_idx].GetData());
		int count = collection_chunk.size();
		Vector new_vec(LogicalType::FLOAT, count);
		if (collection_chunk.data[col_idx].GetType().id() == LogicalTypeId::DECIMAL) {
			CastParameters parameters;
			uint8_t width = DecimalType::GetWidth(collection_chunk.data[col_idx].GetType());
			uint8_t scale = DecimalType::GetScale(collection_chunk.data[col_idx].GetType());
			switch (collection_chunk.data[col_idx].GetType().InternalType()) {
			case PhysicalType::INT16: {
				VectorCastHelpers::TemplatedDecimalCast<int16_t, float, TryCastFromDecimal>(
				    collection_chunk.data[col_idx], new_vec, count, parameters.error_message, width, scale);
				break;
			} case PhysicalType::INT32: {
				VectorCastHelpers::TemplatedDecimalCast<int32_t, float, TryCastFromDecimal>(
				    collection_chunk.data[col_idx], new_vec, count, parameters.error_message, width, scale);
				break;
			} case PhysicalType::INT64: {
				VectorCastHelpers::TemplatedDecimalCast<int64_t, float, TryCastFromDecimal>(
				    collection_chunk.data[col_idx], new_vec, count, parameters.error_message, width, scale);
				break;
			} case PhysicalType::INT128: {
				VectorCastHelpers::TemplatedDecimalCast<hugeint_t, float, TryCastFromDecimal>(
				    collection_chunk.data[col_idx], new_vec, count, parameters.error_message, width, scale);
				break;
			} default: {
				throw InternalException("Unimplemented internal type for decimal");
			}
			}
			col = reinterpret_cast<T1*>(new_vec.GetData());
		}
		for (idx_t i=0; i < collection_chunk.size(); ++i) {
			input_values[i+offset] = col[i]; // collection_chunk.data[col_idx].GetValue(i).GetValue<T2>();
		}
		offset +=  collection_chunk.size();
	}

	return input_values;
}

template <class T>
void Fade::PrintOutput(FadeDataPerNode& info, T* data_ptr) {
	for (int i=0; i < info.n_groups; i++) {
		for (int j=0; j < info.n_interventions; j++) {
			int index = i * info.n_interventions + j;
			std::cout << index << " G: " << i << " I: " << j << " -> " <<  data_ptr[index] << std::endl;
			break;
		}
	}
}


void Fade::ReleaseFade(EvalConfig& config, void* handle, PhysicalOperator* op,
                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                 std::unordered_map<std::string, float> columns_spec) {

	if (op->type != PhysicalOperatorType::PROJECTION &&
	    !(op->type == PhysicalOperatorType::FILTER && config.prune) &&
	    !(op->type == PhysicalOperatorType::TABLE_SCAN && config.prune)) {
		if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
			for (auto &pair : fade_data[op->id].alloc_vars) {
				if (!pair.second.empty()) {
					if (config.debug) {
						if (fade_data[op->id].alloc_vars_types[pair.first] == "int") {
							Fade::PrintOutput<int>(fade_data[op->id], (int*)pair.second[0]);
						} else if (fade_data[op->id].alloc_vars_types[pair.first] == "float") {
							Fade::PrintOutput<float>(fade_data[op->id], (float*)pair.second[0]);
						}
					}
					for (int t = 0; t < pair.second.size(); t++) {
						free(pair.second[t]);
						pair.second[t] = nullptr;
					}

				}
			}
			for (auto &pair : fade_data[op->id].input_data_map) {
				if (pair.second != nullptr) {
					free(pair.second);
					pair.second = nullptr;
				}
			}
		} else {
			if (fade_data[op->id].del_interventions != nullptr) {
				free(fade_data[op->id].del_interventions);
				fade_data[op->id].del_interventions = nullptr;
			}
		}
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		ReleaseFade(config, handle, op->children[i].get(), fade_data, columns_spec);
	}

}

} // namespace duckdb
#endif

