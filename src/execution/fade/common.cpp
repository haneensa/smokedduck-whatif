#ifdef LINEAGE
#include "duckdb/execution/fade/fade.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include <fstream>
#include <dlfcn.h>
#include <immintrin.h>
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include <thread>
#include <vector>

namespace duckdb {

string Fade::PrepareLineage(PhysicalOperator *op, bool prune, bool forward_lineage) {
	// timing vars
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;

	// 1. Post Process
	start_time = std::chrono::steady_clock::now();
	LineageManager::PostProcess(op);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double post_processing_time = time_span.count();

	// 2. retrieve lineage
	start_time = std::chrono::steady_clock::now();
	Fade::GetLineage(op);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double lineage_time = time_span.count();

	// 3 Prune
	double prune_time = 0;
	if (prune) {
		//int remove = PruneUtilization(op,0);
		//std::cout << "total remove " << remove << std::endl;
		start_time = std::chrono::steady_clock::now();
		vector<int> out_order;
		Fade::PruneLineage(op, out_order);
		end_time = std::chrono::steady_clock::now();
		time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
		prune_time = time_span.count();
	}

	double forward_lineage_time = 0;
	if (forward_lineage) { //if (config.n_intervention == 1 && config.incremental == true) {
		start_time = std::chrono::steady_clock::now();
		Fade::FillForwardLineage(op, prune);
		end_time = std::chrono::steady_clock::now();
		time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
		forward_lineage_time = time_span.count();
	}



	return "select " + to_string(post_processing_time) + " as post_processing_time, "
	       + to_string(lineage_time) + " as lineage_time, "
	       + to_string(prune_time) + " as prune_time, "
	       + to_string(forward_lineage_time) + " as forward_lineage_time";
}

void Fade::FillForwardLineage(PhysicalOperator* op, bool prune) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		FillForwardLineage(op->children[i].get(), prune);
	}

	if (prune == false && (op->type == PhysicalOperatorType::TABLE_SCAN || op->type == PhysicalOperatorType::FILTER)) {
		int forward_row_count = 0;
		if (op->type == PhysicalOperatorType::FILTER) {
			PhysicalOperator* cur_op = op->children[0].get();
			while (cur_op && cur_op->lineage_op && cur_op->lineage_op->backward_lineage[0].empty()) {
				cur_op = cur_op->children[0].get();
			}
			if (cur_op && cur_op->lineage_op && !cur_op->lineage_op->backward_lineage[0].empty()) {
        forward_row_count =  cur_op->lineage_op->backward_lineage[0].size();
        std::cout << "get lineage using bl for " << cur_op->id << " " << forward_row_count << std::endl;
      }
		} else {
      forward_row_count = op->lineage_op->log_index->table_size;
    }
		int row_count = op->lineage_op->backward_lineage[0].size();
		for (int oid=0; oid < row_count; ++oid) {
			int iid = op->lineage_op->backward_lineage[0][oid];
      if (iid >= forward_row_count) forward_row_count = iid+1;
    }

    //std::cout << forward_row_count << " " << op->lineage_op->log_index->table_size << " " << op->lineage_op->backward_lineage[0].size() << std::endl;

		op->lineage_op->forward_lineage[0].assign(forward_row_count, -1);
		for (int oid=0; oid < row_count; ++oid) {
			// key: iid, val: oid
			op->lineage_op->forward_lineage[0][ op->lineage_op->backward_lineage[0][oid] ] = oid;
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		int row_count = op->lineage_op->backward_lineage[0].size();
		// many to many mapping
		for (int side=0; side < 2; side++) {
			PhysicalOperator* cur_op = op->children[side].get();
			int forward_row_count =  0;
      while (cur_op && cur_op->lineage_op && cur_op->lineage_op->backward_lineage[0].empty()) {
				cur_op = cur_op->children[0].get();
			}
			if (cur_op && cur_op->lineage_op && !cur_op->lineage_op->backward_lineage[0].empty()) {
        forward_row_count =  cur_op->lineage_op->backward_lineage[0].size();
      }

			// iid -> oid
			for (int oid=0; oid < row_count; ++oid) {
				// should be list of oids intead of single value
				op->lineage_op->forward_lineage_list[side][ op->lineage_op->backward_lineage[side][oid]  ].push_back(oid);
			}
		}
	}
}

string Fade::get_header(EvalConfig config) {
	std::ostringstream oss;
	oss << R"(
#include <iostream>
#include <vector>
#include <unordered_map>
#include <immintrin.h>
#include <barrier>
#include <random>
#include <set>
)";

	if (config.use_duckdb) {
		oss << R"(
#include "duckdb.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
)";
	}

	if (config.num_worker > 1) {
		oss << "std::barrier sync_point(" + to_string(config.num_worker) + ");\n";
	}

	return oss.str();
}

string Fade::get_agg_alloc(int fid, string fn, string out_type) {
	std::ostringstream oss;
	if (fn == "sum") {
		oss << "\n";
		oss << out_type << "*__restrict__ out_" + to_string(fid) + " = (" + out_type +"*)alloc_vars[\"out_" + to_string(fid) << "\"][thread_id];\n";
	} else if (fn == "count") {
		oss << R"(
	int* __restrict__  out_count = (int*)alloc_vars["out_count"][thread_id];
)";
	}
	return oss.str();
}

string Fade::group_partitions(EvalConfig config, FadeDataPerNode& node_data) {
	std::ostringstream oss;
	if (config.num_worker > 1) {
		oss << "\tsync_point.arrive_and_wait();\n";
		oss << "\tconst int group_count = " << node_data.n_groups << ";\n";
		oss << R"(
	for (int jc = 0; jc < group_count; jc += 16) {
		if ((jc / 16) % num_threads == thread_id) {
)";
		for (auto &pair : node_data.alloc_vars) {
			oss << "{\n";
			int fid = node_data.alloc_vars_index[pair.first] ;
			string type_str = node_data.alloc_vars_types[pair.first];
			if (fid == -1) { // count
				oss <<  type_str +"* __restrict__ final_out = (" + type_str + "* __restrict__)alloc_vars[\"out_count\"][0];\n";
			} else {
				oss <<  type_str +"* __restrict__ final_out = (" + type_str + "* __restrict__)alloc_vars[\"out_"+to_string(fid)+"\"][0];\n";
			}
			oss << R"(
		    for (int j = jc; j < jc + 16 && j < group_count; ++j) {
		        for (int i = 1; i < num_threads; ++i) {
)";
			if (fid == -1) { // count
				oss <<  type_str +"* __restrict__ final_in = (" + type_str + "* __restrict__)alloc_vars[\"out_count\"][i];\n";
			} else {
				oss <<  type_str +"* __restrict__ final_in = (" + type_str + "* __restrict__)alloc_vars[\"out_"+to_string(fid)+"\"][i];\n";
			}

			oss << R"(
for (int k = 0; k < n_interventions; ++k) {
						int index = j * n_interventions + k;
)";
			oss << "final_out[index] += final_in[index];\n";
			oss << R"(
					}//(int k = 0; k < n_interventions; ++k)
				}//(int i = 1; i < num_threads; ++i)
			}//(int j = jc; j < jc + 16 && j < group_count; ++j)
    }
)";
		}
		oss << R"(
		}//if ((jc / 16) % num_threads == thread_id)
	}//for (int jc = 0; jc < group_count; jc += 16)
)";
	}

// iterate over annotations
// 
  return oss.str();

}

string Fade::get_agg_finalize(EvalConfig config, FadeDataPerNode& node_data) {
	std::ostringstream oss;

	if (config.n_intervention == 1) {
		if (config.use_duckdb) {
			oss << R"(
			}
			offset +=  collection_chunk.size();
		}
)";
		} else {
			oss << "\t }\n";
		}
	oss << Fade::group_partitions(config, node_data);
	oss << "\treturn 0;\n}\n";
  return oss.str();
	}
  
  //if (config.is_scalar && config.intervention_type != InterventionType::SCALE_UNIFORM) oss <<  "\n\t\t\t\t}//c\n"; // close inner loop
	if (config.is_scalar) oss <<  "\n\t\t\t\t}//c\n"; // close inner loop

	if (config.use_duckdb) {
		oss << R"(
			}//a.1
		}//a.2
		offset +=  collection_chunk.size();
}
)";
	} else {
		oss << R"(
		}//b.1
	} //b.2
)";
	}

	oss << Fade::group_partitions(config, node_data);
	oss << "\treturn 0;\n}\n";

	return oss.str();
}


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
		std::string build_command = "g++ -O3 -std=c++2a -mavx512f -march=native -shared -fPIC loop.cpp -o loop.so -L"
		                            + std::string(duckdb_lib_path) + " -lduckdb";
		system(build_command.c_str());
		void *handle = dlopen("./loop.so", RTLD_LAZY);
		if (!handle) {
			std::cerr << "Cannot Open Library: " << dlerror() << std::endl;
		}
		return handle;
	}
}

// table_name.col_name
std::unordered_map<std::string, std::vector<std::string>>  Fade::parseSpec(EvalConfig& config) {
	std::unordered_map<std::string, std::vector<std::string>> result;
  if (config.columns_spec_str.empty()) return result;

	std::istringstream iss(config.columns_spec_str);
	std::string token;

	while (std::getline(iss, token, '|')) {
		std::istringstream tokenStream(token);
		std::string table, column;
		if (std::getline(tokenStream, table, '.')) {
			if (std::getline(tokenStream, column)) {
				// Convert column name to uppercase (optional)
				for (char& c : column) {
					c = std::tolower(c);
				}
				// Add the table name and column to the dictionary
				result[table].push_back(column);
			}
		}
	}



	return result;
}

void Fade::FillFilterBackwardLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop) {
	bool cache_on = false;
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	idx_t offset = 0;

	idx_t row_count = lop->log_index->table_size;
	lop->backward_lineage[0].resize(row_count);

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
      //std::cout << op->id << " getlineage: " <<i << " " << offset << " " <<  i+offset << " -> " << iid << std::endl;
			lop->backward_lineage[0][i+offset] = iid;
		}
		offset += result.size();
	} while (cache_on || result.size() > 0);
}

void Fade::FillJoinBackwardLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop) {
	vector<int>& lhs_lineage =  lop->backward_lineage[0];
	vector<int>& rhs_lineage =  lop->backward_lineage[1];
	idx_t row_count = lop->log_index->table_size;
	lhs_lineage.resize(row_count);
	rhs_lineage.resize(row_count);
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
		for (idx_t i=0; i < result.size(); ++i) {
			lhs_lineage[i+offset] = lhs_index[i];
			rhs_lineage[i+offset] = rhs_index[i];
		}
		offset += result.size();
	} while (cache_on || result.size() > 0);
}

void Fade::FillGBForwardLineage(shared_ptr<OperatorLineage> lop, int row_count) {
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	bool cache_on = false;
	lop->forward_lineage[0].resize(row_count);
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
			lop->forward_lineage[0][iid] = oid;
		}
	} while (cache_on || result.size() > 0);
}


int Fade::PruneUtilization(PhysicalOperator* op, int M, int side=0) {
	int removed = 0;
	int original = 0;
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		original = op->lineage_op->backward_lineage[0].size();
		removed = (op->lineage_op->backward_lineage[0].size() - M);
		std::cout << op->id << " scan " << removed << " " << float(removed) / op->lineage_op->backward_lineage[0].size()
		          << " " << op->lineage_op->backward_lineage[0].size() / float(M) << " " << op->lineage_op->backward_lineage[0].size()
		          << " M " << M << std::endl;
	} else if (op->type == PhysicalOperatorType::FILTER) {
		original = op->lineage_op->backward_lineage[0].size();
		if (M == 0 || M > op->lineage_op->backward_lineage[0].size() ) {
			M = op->lineage_op->backward_lineage[0].size();
		} else {
			removed = (op->lineage_op->backward_lineage[0].size() - M);
		}
		std::cout <<  op->id << " filter " << removed << " " << float(removed)/ op->lineage_op->backward_lineage[0].size()
		          << " " << op->lineage_op->backward_lineage[0].size() / float(M) << " " << op->lineage_op->backward_lineage[0].size()
		          << " M " << M << std::endl;
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		original = op->lineage_op->backward_lineage[side].size();
		if (M == 0 || M > op->lineage_op->backward_lineage[side].size() ) {
			M = op->lineage_op->backward_lineage[side].size();
		} else {
			removed = (op->lineage_op->backward_lineage[side].size() - M);
		}
		std::cout <<  op->id << " join  " << removed << " " <<  float(removed)  / op->lineage_op->backward_lineage[side].size()
		          << " " << op->lineage_op->backward_lineage[side].size() / float(M) <<  " "
		          << op->lineage_op->backward_lineage[side].size() << " M: " << M << std::endl;
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		original = op->lineage_op->backward_lineage[0].size();
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		original += PruneUtilization(op->children[i].get(),  M, i);
	}

	return original;
}

void Fade::PruneLineage(PhysicalOperator* op, vector<int>& out_order) {
	vector<int> new_order[2];

	if (out_order.empty() == false) {
		// if table scan and no filter push down, then use out_order as lineage
		// if hash group by: ..
		if (op->type == PhysicalOperatorType::TABLE_SCAN) {
			// std::cout <<  op->id << " " << op->lineage_op->backward_lineage[0].size() / float(out_order.size())
			// << " scan prune previous output size M= " << op->lineage_op->backward_lineage[0].size()
			// << " new pruned: " << out_order.size() << std::endl;
			op->lineage_op->backward_lineage[0] = out_order;
		} else if (op->type == PhysicalOperatorType::FILTER) {
			vector<int> new_lineage(out_order.size());
			for (int i=0; i < out_order.size(); ++i) {
				new_lineage[i] = op->lineage_op->backward_lineage[0][out_order[i]];
			}
			//std::cout << op->id << " " << op->lineage_op->backward_lineage[0].size() / float(out_order.size())
			// << " filter prune previous output size M= " << op->lineage_op->backward_lineage[0].size()
			// << " new pruned: " << out_order.size() << std::endl;
			op->lineage_op->backward_lineage[0] = std::move(new_lineage);
		} else if (op->type == PhysicalOperatorType::HASH_JOIN
		           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
		           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
		           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
		           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
			for (int side=0; side < 2; ++side) {
				vector<int> new_lineage(out_order.size());
				for (int i=0; i < out_order.size(); ++i) {
					new_lineage[i] = op->lineage_op->backward_lineage[side][out_order[i]];
				}
				// std::cout << op->id << " " << op->lineage_op->backward_lineage[side].size() / float(out_order.size())
				// << " " << side << " join prune previous output size M= " << op->lineage_op->backward_lineage[side].size()
				// << " new pruned: " << out_order.size() << std::endl;
				// update lineage
				op->lineage_op->backward_lineage[side] = std::move(new_lineage);
			}
		}
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		for (int i=0; i < op->lineage_op->backward_lineage[0].size(); ++i) {
			new_order[0].push_back(i);
		}
		//std::cout << "filter push down M=" << new_order[0].size() << std::endl;
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {

		for (int side=0; side < 2; ++side) {
			// This creates out_order to pass to the child. e.g. output only references 0 and 3 from child [1, 1, 1, 3]
			// reindexes this lineage
			vector<int>& lineage = op->lineage_op->backward_lineage[side];
			vector<int>  lineage_inverse(lineage.size()); // [0, 0, 0, 1]
			vector<int>  lineage_unique; // [1, 3]

			std::map<int, int> new_order_map;
			for (int i = 0; i < lineage.size(); ++i) {
				if (new_order_map.find(lineage[i]) == new_order_map.end()) {
					new_order_map[lineage[i]] = new_order_map.size();
					lineage_unique.push_back(lineage[i]);
				}
				lineage_inverse[new_order_map[lineage[i]]];
			}

			//std::cout <<  op->lineage_op->backward_lineage[side].size()  / float(lineage_inverse.size())
			// << " " << side << " join push down join output M=" << op->lineage_op->backward_lineage[side].size()
			// << " unique= " << lineage_unique.size() << " inverse= " << lineage_inverse.size() << std::endl;
			// update lineage
			op->lineage_op->backward_lineage[side] = std::move(lineage_inverse);
			new_order[side] = lineage_unique;
		}

	} else if (op->type == PhysicalOperatorType::PROJECTION) {
    	new_order[0] = std::move(out_order);
  }

	for (idx_t i = 0; i < op->children.size(); i++) {
		Fade::PruneLineage(op->children[i].get(), new_order[i]);
	}
}

void Fade::GetLineage(PhysicalOperator* op) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GetLineage(op->children[i].get());
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN || op->type == PhysicalOperatorType::FILTER) {
    		// key: oid | val: iid
		if (op->type == PhysicalOperatorType::TABLE_SCAN && !dynamic_cast<PhysicalTableScan *>(op)->function.filter_pushdown) {
			idx_t row_count = op->lineage_op->log_index->table_size;
			op->lineage_op->backward_lineage[0].resize(row_count);
			for (int i=0; i < row_count; i++) {
				op->lineage_op->backward_lineage[0][i]=i;
			}
		} else {
			FillFilterBackwardLineage(op, op->lineage_op);
		}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		// key: oid | val: rhs,lhs
		FillJoinBackwardLineage(op, op->lineage_op);
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
		// key: iid, val: oid
		idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		FillGBForwardLineage(op->lineage_op, row_count);
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		// key: iid, val: oid
		idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		op->lineage_op->forward_lineage[0].resize(row_count);
		std::fill(op->lineage_op->forward_lineage[0].begin(), op->lineage_op->forward_lineage[0].end(), 0);
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
  std::cout << "Print Output for " << info.n_interventions << " interventions and " << info.n_groups << " groups." << std::endl;
	for (int i=0; i < info.n_groups; i++) {
		for (int j=0; j < info.n_interventions; j++) {
			int index = i * info.n_interventions + j;
			std::cout << " G: " << i << " I: " << j << " -> " <<  data_ptr[index] << std::endl;
		}
	}
}


void Fade::ReleaseFade(EvalConfig& config, void* handle, PhysicalOperator* op,
                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {

	if (op->type != PhysicalOperatorType::PROJECTION &&
	    !(op->type == PhysicalOperatorType::FILTER && config.prune) &&
	    !(op->type == PhysicalOperatorType::TABLE_SCAN && config.prune)) {
		if (op->type == PhysicalOperatorType::HASH_GROUP_BY
		    || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
		    || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
			for (auto &pair : fade_data[op->id].alloc_vars) {
				if (!pair.second.empty()) {
					if (config.debug) {
            std::cout << "Print out results for " << pair.first << std::endl;
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
		ReleaseFade(config, handle, op->children[i].get(), fade_data);
	}

}


template<class T>
void Fade::allocate_agg_output(string typ, int t, int n_groups, int n_interventions, string out_var, PhysicalOperator* op,
                         std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {
	fade_data[op->id].alloc_vars_types[out_var] =typ;
	fade_data[op->id].alloc_vars[out_var][t] = aligned_alloc(64, sizeof(T) * n_groups * n_interventions);
	if (fade_data[op->id].alloc_vars[out_var][t] == nullptr) {
		fade_data[op->id].alloc_vars[out_var][t] = malloc(sizeof(T) * n_groups * n_interventions);
	}
	memset(fade_data[op->id].alloc_vars[out_var][t], 0, sizeof(T) * n_groups * n_interventions);
}


// if nested, then take the output of the previous agg as input
void Fade::GroupByAlloc(EvalConfig& config, shared_ptr<OperatorLineage> lop,
                        std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
                        PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                        int keys_size, int n_groups) {
	const int n_interventions = fade_data[op->id].n_interventions;
	fade_data[op->id].n_groups = n_groups;
	idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
	bool include_count = false;

	// Populate the aggregate child vectors
	for (idx_t i=0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i]->Cast<BoundAggregateExpression>();
		vector<idx_t> aggregate_input_idx;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			aggregate_input_idx.push_back(bound_ref_expr.index);
		}
		string name = aggr.function.name;

		if (include_count == false && (name == "count" || name == "count_star")) {
			include_count = true;
			continue;
		} else if (name == "avg") {
			include_count = true;
		}

		if (name == "sum" || name == "sum_no_overflow" || name == "avg") {
			int col_idx = aggregate_input_idx[0]; //i + keys_size;
			string input_type = "float";
			string output_type = "float";
			if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::INTEGER ) {
				input_type = "int";
				output_type = "int";
			} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::FLOAT) {
				input_type = "float";
				output_type = "float";
			} else {
				input_type = "double";
				output_type = "float";
			}

			string out_var = "out_" + to_string(i); // new output
			string in_arr = "col_" + to_string(i);  // input arrays
			string in_val = "val_" + to_string(i);  // input values val = col_x[i]

			if (config.use_duckdb == false) {
				if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::INTEGER) {
					fade_data[op->id].input_data_map[i] = Fade::GetInputVals<int, int>(op, op->lineage_op, col_idx);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::FLOAT) {
					fade_data[op->id].input_data_map[i] = Fade::GetInputVals<float, float>(op, op->lineage_op, col_idx);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::DOUBLE) {
					fade_data[op->id].input_data_map[i] = Fade::GetInputVals<double, float>(op, op->lineage_op, col_idx);
				} else {
					fade_data[op->id].input_data_map[i] = Fade::GetInputVals<float, float>(op, op->lineage_op, col_idx);
				}
			}

			fade_data[op->id].alloc_vars[out_var].resize(config.num_worker);
			for (int t=0; t < config.num_worker; ++t) {
				if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::INTEGER) {
					allocate_agg_output<int>("int", t, n_groups, n_interventions, out_var, op, fade_data);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::FLOAT) {
					allocate_agg_output<float>("float", t, n_groups, n_interventions, out_var, op, fade_data);
				} else {
					allocate_agg_output<float>("float", t, n_groups, n_interventions, out_var, op, fade_data);
				}
			}
			fade_data[op->id].alloc_vars_index[out_var] = i;
		}
	}

	if (include_count == true) {
		string out_var = "out_count";
		fade_data[op->id].alloc_vars[out_var].resize(config.num_worker);
		for (int t=0; t < config.num_worker; ++t) {
			allocate_agg_output<int>("int", t, n_groups, n_interventions, out_var, op, fade_data);
		}
		fade_data[op->id].alloc_vars_index[out_var] = -1;
	}
}

template<class T>
pair<int*, int> local_factorize(shared_ptr<OperatorLineage> lop, idx_t col_idx) {
	std::unordered_map<T, int> dict;
	idx_t chunk_count = lop->chunk_collection.ChunkCount();
	idx_t row_count = lop->chunk_collection.Count();
	int* codes = new int[row_count];
  int count = 0;
	for (idx_t chunk_idx=0; chunk_idx < chunk_count; ++chunk_idx) {
		DataChunk &collection_chunk = lop->chunk_collection.GetChunk(chunk_idx);
		for (idx_t i=0; i < collection_chunk.size(); ++i) {
			T v = collection_chunk.GetValue(col_idx, i).GetValue<T>();
			if (dict.find(v) == dict.end()) {
				dict[v] = dict.size();
			}
			codes[count++] = dict[v];
		}
	}

	return make_pair(codes, dict.size());
}

std::pair<int*, int> create_codes(LogicalType& typ, shared_ptr<OperatorLineage> lop, int i) {
	if (typ == LogicalType::INTEGER) {
		return local_factorize<int>(lop, i);
	} else if (typ == LogicalType::BIGINT) {
		return local_factorize<int64_t>(lop, i);
	} else if (typ == LogicalType::VARCHAR) {
		return local_factorize<string>(lop, i);
	} else {
		return local_factorize<float>(lop, i);
	} 
	
	return {nullptr, 0};
}

std::pair<int*, int> augment(int row_count, std::pair<int*, int> new_codes, std::pair<int*, int> old_codes) {
  int factor = old_codes.second;
  for (int i=0; i < row_count; ++i) {
    new_codes.first[i] = new_codes.first[i]* factor + old_codes.first[i]; 
  }

  new_codes.second *= old_codes.second;
  return new_codes;
}

std::pair<int*, int> Fade::factorize(PhysicalOperator* op, shared_ptr<OperatorLineage> lop,
                                      std::unordered_map<std::string, std::vector<std::string>>& columns_spec) {
  std::vector<std::string> col_name_vec = columns_spec[lop->table_name];
	PhysicalTableScan * scan = dynamic_cast<PhysicalTableScan *>(op);
	std::pair<int*, int> fade_data = {nullptr, 0};
	std::pair<int*, int> res;
	idx_t row_count = lop->chunk_collection.Count();
  
	if (scan->function.projection_pushdown) {
		if (scan->function.filter_prune) {
			for (idx_t i = 0; i < scan->projection_ids.size(); i++) {
				const auto &column_id = scan->column_ids[scan->projection_ids[i]];
				if (column_id < scan->names.size() && std::find(col_name_vec.begin(), col_name_vec.end(), scan->names[column_id]) != col_name_vec.end()) {
					res = create_codes(scan->types[i], lop, i);
          if (fade_data.second > 0) {
            fade_data = augment(row_count, res, fade_data);
          } else {
            fade_data = res;
          }
				}
			}
		} else {
			for (idx_t i = 0; i < scan->column_ids.size(); i++) {
				const auto &column_id = scan->column_ids[i];
				if (column_id < scan->names.size() && std::find(col_name_vec.begin(), col_name_vec.end(), scan->names[column_id]) != col_name_vec.end()) {
					res = create_codes(scan->types[i], lop, i);
          if (fade_data.second > 0) {
            fade_data = augment(row_count, res, fade_data);
          } else {
            fade_data = res;
          }
				}
			}
		}
	} else {
		for (idx_t i=0; i < scan->names.size(); i++) {
				if (i < scan->names.size() && std::find(col_name_vec.begin(), col_name_vec.end(), scan->names[i]) != col_name_vec.end()) {
				  res = create_codes(scan->types[i], lop, i);
          if (fade_data.second > 0) {
            fade_data = augment(row_count, res, fade_data);
          } else {
            fade_data = res;
          }
			}
		}
	}
	

	return fade_data;
}

int* Fade::random_unique(shared_ptr<OperatorLineage> lop, idx_t distinct) {
	// Seed the random number generator
	std::random_device rd;
	std::mt19937 gen(rd());
	idx_t row_count = lop->log_index->table_size;
	int* codes = new int[row_count];

	// Generate random values
	std::uniform_int_distribution<int> distribution(0, distinct - 1);
  int count = 0;
	for (idx_t i = 0; i < row_count; ++i) {
		int random_value = distribution(gen);
		codes[count++] = random_value;
	}

	return codes;
}


void Fade::BindFunctions(EvalConfig& config, void* handle, PhysicalOperator* op,
                   std::unordered_map<idx_t, FadeDataPerNode>& fade_data) {

	for (idx_t i = 0; i < op->children.size(); i++) {
		BindFunctions(config, handle, op->children[i].get(), fade_data);
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		if (config.prune) return;
		//if (fade_data[op->id].n_masks > 0) {
		string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		fade_data[op->id].filter_fn = (int(*)(int, int*, void*, void*, std::set<int>&, std::set<int>&))dlsym(handle, fname.c_str());
		//	}
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		//if (fade_data[op->id].n_masks > 0) {
		string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
    if (config.n_intervention == 1 && config.incremental == true) {
      fade_data[op->id].join_fn_forward = (int(*)(int, std::unordered_map<int, std::vector<int>>&, 
              std::unordered_map<int, std::vector<int>>&, void*, void*, void*,
              std::set<int>&, std::set<int>&, std::set<int>&))dlsym(handle, fname.c_str());
    } else {
		  fade_data[op->id].join_fn = (int(*)(int, int*, int*, void*, void*, void*, std::set<int>&, std::set<int>&, std::set<int>&))dlsym(handle, fname.c_str());
    }
		//}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		string fname = "agg_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		if (config.use_duckdb && fade_data[op->id].has_agg_child == false) {
			fade_data[op->id].agg_duckdb_fn = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
			                                           ChunkCollection&, std::set<int>&))dlsym(handle, fname.c_str());
		} else {
			if (fade_data[op->id].has_agg_child) {
		  		fade_data[op->id].agg_fn_nested = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
				                                     std::unordered_map<std::string, vector<void*>>&, std::set<int>&))dlsym(handle, fname.c_str());
			} else {
		  		fade_data[op->id].agg_fn = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
				                              std::unordered_map<int, void*>&, std::set<int>&))dlsym(handle, fname.c_str());
			}
		}
	}
}


} // namespace duckdb
#endif

