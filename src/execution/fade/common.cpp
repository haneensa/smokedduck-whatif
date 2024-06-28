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
#include <thread>
#include <vector>

namespace duckdb {

int (*fade_random_fn)(int, int, float, int, void*, int, std::vector<__mmask16>&);

int fill_random_single(int thread_id, int row_count, float prob, int n_masks,
                       void* del_interventions_ptr, int rand_count,
                       std::vector<__mmask16>& base,
                       int start, int end) {
	int count = 0;
	int8_t* __restrict__ del_interventions = (int8_t* __restrict__)del_interventions_ptr;

	for (int i = start; i < end; ++i) {
		del_interventions[i] = (((double)rand() / RAND_MAX) < prob);
    // count += del_interventions[i];
	}

	// std::cout << thread_id << " random done-> start: "  << start << ", end: " << end <<  ", count: " << count << std::endl;
	return 0;
}

int fill_random_dense(int thread_id, int row_count, float prob, int n_masks,
                      void* del_interventions_ptr, int rand_count,
                      std::vector<__mmask16>& base,
                      int start, int end) {
	int count = 0;
	__mmask16* __restrict__ del_interventions = (__mmask16* __restrict__)del_interventions_ptr;

	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<int> dist(0, rand_count);
	base.resize(rand_count);

	for (int i = 0; i < rand_count; ++i) {
		base[i] = 0x0;
		for (int k = 0; k < 16; ++k) {
			if ((((double)rand() / RAND_MAX) < prob)) {
				base[i] |= (1 << k);
			}
		}
	}
	for (int i = start; i < end; ++i) {
		for (int j = 0; j < n_masks; ++j) {
			int r = dist(gen);
			del_interventions[i*n_masks+j] = base[r];
		}
	}

	// std::cout << thread_id << " random done-> start: "  << start << ", end: " << end <<  ", count: " << count << std::endl;
	return 0;
}


void Fade::GenRandomWhatifIntervention(int thread_id, EvalConfig& config, PhysicalOperator* op,
                                       std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                                       std::unordered_map<std::string, std::vector<std::string>>& spec,
                                       bool use_compiled=false) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		GenRandomWhatifIntervention(thread_id, config, op->children[i].get(), fade_data, spec, use_compiled);
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		if (spec.find(op->lineage_op->table_name) == spec.end() && !spec.empty()) {
			return;
		}

		pair<int, int> start_end_pair = Fade::get_start_end(fade_data[op->id]->rows, thread_id, config.num_worker);
		if (config.n_intervention == 1) {
			FadeNodeSingle* cur_node = dynamic_cast<FadeNodeSingle*>(fade_data[op->id].get());
			if (use_compiled) {
				fade_random_fn(thread_id, cur_node->rows, config.probability, 0, cur_node->base_single_del_interventions, global_rand_count, global_rand_base);
			} else {
				fill_random_single(thread_id, cur_node->rows, config.probability, 0,
				                   cur_node->base_single_del_interventions, global_rand_count,  global_rand_base,
				                   start_end_pair.first, start_end_pair.second);
			}
		} else {
			FadeNodeDense* cur_node = dynamic_cast<FadeNodeDense*>(fade_data[op->id].get());
			if (use_compiled) {
				fade_random_fn(thread_id, cur_node->rows, config.probability, cur_node->n_masks, cur_node->base_target_matrix, global_rand_count,  global_rand_base);
			} else {
				fill_random_dense(thread_id, cur_node->rows, config.probability, cur_node->n_masks,
				                  cur_node->base_target_matrix, global_rand_count,  global_rand_base,
				                  start_end_pair.first, start_end_pair.second);
			}
		}
	}
}

int PruneLineageCompile(duckdb::PhysicalOperator* op, std::vector<int>& lineage,
                        std::vector<int>& lineage_inverse, std::vector<int> &lineage_unique) {
	std::map<int, int> new_order_map;
	for (int i = 0; i < lineage.size(); ++i) {
		if (new_order_map.find(lineage[i]) == new_order_map.end()) {
			new_order_map[lineage[i]] = new_order_map.size();
			lineage_unique.push_back(lineage[i]);
			//std::cout << "add new lineage: " << new_order_map[lineage[i]] << " " << lineage[i] << std::endl;
		}
		lineage_inverse[i] = new_order_map[lineage[i]];
	}

	//std::cout <<  lineage.size()  << " " << lineage_unique.size() << " " << lineage_inverse.size() << " " <<  new_order_map.size() << std::endl;
	return 0;
}

int LineageReindex(std::vector<int>& out_order, std::vector<int>& new_lineage, std::vector<int>& old_lineage) {
	for (int i=0; i < new_lineage.size(); ++i) {
		new_lineage[i] = old_lineage[out_order[i]];
	}

	return 0;
}

string Fade::PrepareLineage(PhysicalOperator *op, bool prune, bool forward_lineage, bool use_gb_backward_lineage) {
	// timing vars
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;

	// 1. Post Process
	//std::cout << "start post process " << std::endl;
	start_time = std::chrono::steady_clock::now();
	LineageManager::PostProcess(op);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double post_processing_time = time_span.count();
	//std::cout << "end post process " << post_processing_time <<std::endl;

	// 2. retrieve lineage
	start_time = std::chrono::steady_clock::now();
	Fade::GetLineage(op, use_gb_backward_lineage);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double lineage_time = time_span.count();

  int lineage_count = LineageMemory(op);
  float lineage_size_mb = sizeof(int) * (lineage_count / (1024.0*1024.0));
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

  // std::cout << "after prune:" << std::endl;
  int lineage_count_prune = LineageMemory(op);
  float lineage_size_mb_prune = sizeof(int) * (lineage_count_prune / (1024.0*1024.0));


	return "select " + to_string(post_processing_time) + " as post_processing_time, "
	       + to_string(lineage_time) + " as lineage_time, "
	       + to_string(prune_time) + " as prune_time, "
	       + to_string(forward_lineage_time) + " as forward_lineage_time, "
         + to_string(lineage_count) + " as lineage_count, "
         + to_string(lineage_count_prune) + " as lineage_count_prune, "
         + to_string(lineage_size_mb) + " as lineage_size_mb, "
         + to_string(lineage_size_mb_prune) + " as lineage_size_mb_prune";
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
        // std::cout << "get lineage using bl for " << cur_op->id << " " << forward_row_count << std::endl;
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
)";


	if (config.num_worker > 1) {
		oss << "std::barrier sync_point(" + to_string(config.num_worker) + ");\n";
	}

	return oss.str();
}

string Fade::get_agg_alloc(EvalConfig &config, int fid, string fn, string out_type) {
	std::ostringstream oss;
	if (fn == "sum") {
		oss << "\n\t" << out_type << "*__restrict__ out_" + to_string(fid) + " = (" + out_type +"*)alloc_vars[\"out_" + to_string(fid) << "\"][thread_id];\n";
	} else if (fn == "count") {
		oss << R"(
	int* __restrict__  out_count = (int*)alloc_vars["out_count"][thread_id];
)";
	}
	return oss.str();
}

string Fade::group_partitions(EvalConfig config, int n_groups,
                              std::unordered_map<string, vector<void*>>& alloc_vars,
                              std::unordered_map<string, int>& alloc_vars_index,
                              std::unordered_map<string, string>& alloc_vars_types,
                              int num_worker) {
	std::ostringstream oss;
	if (num_worker > 1) {
		oss << "\tsync_point.arrive_and_wait();\n";
		oss << "\tconst int group_count = " << n_groups << ";\n";
		oss << R"(
	for (int jc = 0; jc < group_count; jc += 16) {
		if ((jc / 16) % num_threads == thread_id) {
)";
		for (auto &pair : alloc_vars) {
			oss << "{\n";
			int fid = alloc_vars_index[pair.first] ;
			string type_str = alloc_vars_types[pair.first];
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
std::unordered_map<std::string, std::vector<std::string>>  Fade::parseSpec(string& columns_spec_str) {
	std::unordered_map<std::string, std::vector<std::string>> result;
  if (columns_spec_str.empty()) return result;

	std::istringstream iss(columns_spec_str);
	std::string token;

	while (std::getline(iss, token, '|')) {
		std::istringstream tokenStream(token);
		std::string table, column;
		if (std::getline(tokenStream, table, '.')) {
			if (std::getline(tokenStream, column)) {
				// Convert column name to uppercase (optional)
				//for (char& c : column) {
				//	c = std::tolower(c);
				//}
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

void Fade::FillGBBackwardLineage(shared_ptr<OperatorLineage> lop, int row_count) {
	DataChunk result;
	idx_t global_count = 0;
	idx_t local_count = 0;
	idx_t current_thread = 0;
	idx_t log_id = 0;
	bool cache_on = false;
  lop->gb_backward_lineage.resize(row_count);

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
      lop->gb_backward_lineage[oid].push_back(iid);
		}
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
		//std::cout << op->id << " scan " << removed << " " << float(removed) / op->lineage_op->backward_lineage[0].size()
		//          << " " << op->lineage_op->backward_lineage[0].size() / float(M) << " " << op->lineage_op->backward_lineage[0].size()
		//          << " M " << M << std::endl;
	} else if (op->type == PhysicalOperatorType::FILTER) {
		original = op->lineage_op->backward_lineage[0].size();
		if (M == 0 || M > op->lineage_op->backward_lineage[0].size() ) {
			M = op->lineage_op->backward_lineage[0].size();
		} else {
			removed = (op->lineage_op->backward_lineage[0].size() - M);
		}
		//std::cout <<  op->id << " filter " << removed << " " << float(removed)/ op->lineage_op->backward_lineage[0].size()
		//          << " " << op->lineage_op->backward_lineage[0].size() / float(M) << " " << op->lineage_op->backward_lineage[0].size()
		//          << " M " << M << std::endl;
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
		//std::cout <<  op->id << " join  " << removed << " " <<  float(removed)  / op->lineage_op->backward_lineage[side].size()
		//          << " " << op->lineage_op->backward_lineage[side].size() / float(M) <<  " "
		//          << op->lineage_op->backward_lineage[side].size() << " M: " << M << std::endl;
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
			vector<int> new_lineage(out_order.size());
      vector<int>& old_lineage = op->lineage_op->backward_lineage[0];
      LineageReindex(out_order, new_lineage, old_lineage);
      //std::cout << op->id << " table scan new lineage: " << std::endl;
      //for (int i=0; i < out_order.size(); i++)
      //  std::cout << "\t -> " << i << " " << new_lineage[i];
      //std::cout << std::endl;
			op->lineage_op->backward_lineage[0] = std::move(new_lineage);
		} else if (op->type == PhysicalOperatorType::FILTER) {
			vector<int> new_lineage(out_order.size());
      vector<int>& old_lineage = op->lineage_op->backward_lineage[0];
      LineageReindex(out_order, new_lineage, old_lineage);
			/*std::cout << op->id << " " << op->lineage_op->backward_lineage[0].size() / float(out_order.size())
			 << " filter prune previous output size M= " << op->lineage_op->backward_lineage[0].size()
			 << " new pruned: " << out_order.size() << std::endl;
      std::cout << "filter new lineage: " << std::endl;
      for (int i=0; i < out_order.size(); i++)
        std::cout << "\t -> " << i << " " << new_lineage[i];
      std::cout << std::endl;*/
			op->lineage_op->backward_lineage[0] = std::move(new_lineage);
		} else if (op->type == PhysicalOperatorType::HASH_JOIN
		           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
		           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
		           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
		           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
			for (int side=0; side < 2; ++side) {
				vector<int> new_lineage(out_order.size());
        vector<int>& old_lineage = op->lineage_op->backward_lineage[side];
        LineageReindex(out_order, new_lineage, old_lineage);
				// std::cout << op->id << " " << op->lineage_op->backward_lineage[side].size() / float(out_order.size())
				// << " " << side << " join prune previous output size M= " << op->lineage_op->backward_lineage[side].size()
				// << " new pruned: " << out_order.size() << std::endl;
				// update lineage
				op->lineage_op->backward_lineage[side] = std::move(new_lineage);
			}
		}
	}

	if (op->type == PhysicalOperatorType::FILTER) {
		int lineage_size =  op->lineage_op->backward_lineage[0].size();
    vector<int> new_lineage(lineage_size);
		for (int i=0; i < lineage_size; ++i) {
			new_lineage[i] = i; // create 1:1 mapping
		}
    new_order[0] = std::move(op->lineage_op->backward_lineage[0]);
    op->lineage_op->backward_lineage[0] = std::move(new_lineage);
	//	std::cout << op->id << " filter push down new_order=" << new_order[0].size() << " old lineage: " << op->lineage_op->backward_lineage[0].size() << std::endl;
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {

		for (int side=0; side < 2; ++side) {
			// This creates out_order to pass to the child. e.g. output only references 0 and 3 from child [1, 1, 1, 3]
			// reindexes this lineage
			vector<int>& lineage = op->lineage_op->backward_lineage[side];
      std::vector<int>  lineage_inverse(lineage.size()); // [0, 0, 0, 1]
      PruneLineageCompile(op, lineage, lineage_inverse, new_order[side]);
		  //std::cout << op->id << " " << side << " join push down new_order=" << new_order[side].size() <<
      //  " old lineage: " << op->lineage_op->backward_lineage[side].size() <<
      // " inverse: " << lineage_inverse.size() <<  std::endl;
			op->lineage_op->backward_lineage[side] = std::move(lineage_inverse);
		}

	} else if (op->type == PhysicalOperatorType::PROJECTION) {
    	new_order[0] = std::move(out_order);
  }

	for (idx_t i = 0; i < op->children.size(); i++) {
		Fade::PruneLineage(op->children[i].get(), new_order[i]);
	}
}

void Fade::GetLineage(PhysicalOperator* op, bool use_gb_backward_lineage) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GetLineage(op->children[i].get(), use_gb_backward_lineage);
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
    if (use_gb_backward_lineage) {
		  idx_t row_count = op->lineage_op->chunk_collection.Count();
      FillGBBackwardLineage(op->lineage_op, row_count);
      //for (int i=0; i < row_count; i++)
      //  std::cout << "GB Backward Lineage Per Bucket: " << lop->gb_backward_lineage[i].size() << std::endl;
    } else {
		  idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		  FillGBForwardLineage(op->lineage_op, row_count);
    }
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		// key: iid, val: oid
		idx_t row_count = op->children[0]->lineage_op->chunk_collection.Count();
		op->lineage_op->forward_lineage[0].resize(row_count);
		std::fill(op->lineage_op->forward_lineage[0].begin(), op->lineage_op->forward_lineage[0].end(), 0);
	}
}

int Fade::LineageMemory(PhysicalOperator* op) {

  int total_size = 0;
  if (op->lineage_op) {
    int backward_size = op->lineage_op->backward_lineage[0].size();
    int forward_size = op->lineage_op->forward_lineage[0].size();

    // std::cout << op->id << " LineageMemory: " << backward_size << " " << forward_size << std::endl;
    total_size = backward_size + forward_size;
  }

  for (idx_t i = 0; i < op->children.size(); i++) {
		int desc_size = LineageMemory(op->children[i].get());
    total_size += desc_size;
	}
  return total_size;
}

void Fade::Clear(PhysicalOperator *op) {
  // massage the data to make it easier to query
  // for hash join, build hash table on the build side that map the address to id
  // for group by, build hash table on the unique groups
  if (op->lineage_op) {
	  op->lineage_op->Clear();
  }

  for (idx_t i = 0; i < op->children.size(); ++i) {
	  Clear(op->children[i].get());
  }
}


void Fade::GetCachedData(EvalConfig& config, PhysicalOperator* op,
                         std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                         std::unordered_map<std::string, std::vector<std::string>>& spec) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		GetCachedData(config, op->children[i].get(), fade_data, spec);
	}

	if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	    || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	    || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		// To support nested agg, check if any descendants is an agg
		if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
			PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
			auto &aggregates = gb->grouped_aggregate_data.aggregates;
			if (!fade_data[op->id]->has_agg_child) {
				fade_data[op->id]->GroupByGetCachedData(config, op->lineage_op, op, aggregates, gb->grouped_aggregate_data.groups.size());
			}
		} else if (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
			PhysicalPerfectHashAggregate * gb = dynamic_cast<PhysicalPerfectHashAggregate *>(op);
			auto &aggregates = gb->aggregates;
			if (!fade_data[op->id]->has_agg_child) {
				fade_data[op->id]->GroupByGetCachedData(config, op->lineage_op, op, aggregates, gb->groups.size());
			}
		} else {
			PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(op);
			auto &aggregates = gb->aggregates;
			if (!fade_data[op->id]->has_agg_child) {
				fade_data[op->id]->GroupByGetCachedData(config, op->lineage_op, op, aggregates, 0);
			}
		}
	}
}

pair<int, int> Fade::get_start_end(int row_count, int thread_id, int num_worker) {
	int batch_size = row_count / num_worker;
	if (row_count % num_worker > 0) batch_size++;
	int start = thread_id * batch_size;
	int end   = start + batch_size;
	if (end >= row_count)  end = row_count;
	return std::make_pair(start, end);
}

// two cases for scaling intervention: 1) scaling with selection vector (SCALE_RANDOM), 2) uniform scaling (SCALE_UNIFORM)
// uniform: only allocate memory for the aggregate results
// random: allocate single intervention/selection vector per table with 0s/1s with prob
void Fade::AllocSingle(EvalConfig& config, PhysicalOperator* op,
                 std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                 std::unordered_map<std::string, std::vector<std::string>>& spec) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		AllocSingle(config, op->children[i].get(), fade_data, spec);
	}

	unique_ptr<FadeNodeSingleCompile> node = make_uniq<FadeNodeSingleCompile>(op->id, 0, config.num_worker, 0, config.debug);
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		string table_name  = op->lineage_op->table_name;
		if (spec.find(table_name) == spec.end() && !spec.empty()) {
			if (config.debug) std::cout << "skip this scan " << table_name <<std::endl;
	    fade_data[op->id] = std::move(node);
			return;
		}

		node->n_interventions = 1;
		// TODO: read from file
		config.use_preprep_tm = false;
		if (!config.use_preprep_tm) {
			idx_t row_count = op->lineage_op->log_index->table_size;
			node->n_groups  = op->lineage_op->backward_lineage[0].size();
			idx_t max_row_count = op->lineage_op->backward_lineage[0][node->n_groups-1];
			if (config.debug) std::cout << "table scan -> " << node->n_groups << " " << max_row_count << " " << row_count << std::endl;
			if (config.prune) max_row_count = node->n_groups;
			node->rows = max_row_count;
			node->base_single_del_interventions = new int8_t[node->rows];
			if (node->n_groups != node->rows) {
			  node->single_del_interventions = new int8_t[node->n_groups];
			  node->gen = true;
			} else {
        node->single_del_interventions = node->base_single_del_interventions;
      }
		}
	} else if (op->type == PhysicalOperatorType::FILTER) {
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
		if (config.prune) {
			node->opid = fade_data[op->children[0]->id]->opid;
	    fade_data[op->id] = std::move(node);
			return;
		}

		if (node->n_interventions == 1) {
			node->n_groups = op->lineage_op->backward_lineage[0].size();
			node->rows = node->n_groups;
			node->single_del_interventions = new int8_t[node->n_groups];
			node->gen = true;
		}

	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		int lhs_n = fade_data[op->children[0]->id]->n_interventions;
		int rhs_n = fade_data[op->children[1]->id]->n_interventions;
		node->n_interventions = (lhs_n > 0) ? lhs_n : rhs_n;
		node->n_groups = op->lineage_op->backward_lineage[0].size();
		node->rows = node->n_groups;
		if (node->n_interventions == 1) {
			node->single_del_interventions = new int8_t[node->n_groups];
			node->gen = true;
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		node->gen = true;
		node->rows  = op->children[0]->lineage_op->chunk_collection.Count();
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
		node->GroupByAlloc(config.debug, op->type, op->lineage_op, op, config.aggid, config.groups);
	}  else if (op->type == PhysicalOperatorType::PROJECTION) {
		node->opid = fade_data[op->children[0]->id]->opid;
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
	}

	if (config.debug) {
		std::cout << "AllocSingle(" << op->id << ") -> " << ", n_interventions: " << node->n_interventions << std::endl;
	}

	fade_data[op->id] = std::move(node);
}


void Fade::AllocDense(EvalConfig& config, PhysicalOperator* op,
                std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                std::unordered_map<std::string, std::vector<std::string>>& spec) {
	for (idx_t i = 0; i < op->children.size(); ++i) {
		AllocDense(config, op->children[i].get(), fade_data, spec);
	}

  // TODO: check if compiled is false 
	unique_ptr<FadeNodeDenseCompile> node = make_uniq<FadeNodeDenseCompile>(op->id, 0, config.num_worker, 0, config.debug);
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		string table_name  = op->lineage_op->table_name;
		node->n_groups = op->lineage_op->backward_lineage[0].size();
		if (spec.find(table_name) == spec.end() && !spec.empty()) {
			if (config.debug) std::cout << "skip this scan " << table_name <<std::endl;
	    fade_data[op->id] = std::move(node);
			return;
		} else if (!spec.empty() &&  spec[table_name][0].substr(0, 3) == "npy") {
			string col_spec = spec[table_name][0];
			config.use_preprep_tm = node->read_dense_from_file(config.debug, col_spec, table_name);
			// load interventions
			// 1. iterate over base_target matrix and check that there are bits set
			if (config.use_preprep_tm && node->rows != node->n_groups) {
			  node->del_interventions =
				  (__mmask16 *)aligned_alloc(64, sizeof(__mmask16) * node->n_groups * node->n_masks);
			  if (config.debug)
					std::cout << node->rows << " alloc del_interventions " << node->n_groups << " " << node->n_masks << " "
					          << node->del_interventions << std::endl;
			  node->gen = true;
			}
		}
		if (!config.use_preprep_tm) {
			node->n_masks = std::ceil(config.n_intervention / config.mask_size);
			node->n_interventions = config.n_intervention;
			idx_t row_count = op->lineage_op->log_index->table_size;
			idx_t max_row_count = op->lineage_op->backward_lineage[0][node->n_groups-1];
			if (config.debug) std::cout << "table scan -> " << node->n_groups << " " << max_row_count << " " << row_count << std::endl;
			if (config.prune) max_row_count = node->n_groups;
			node->rows = max_row_count;
			node->base_target_matrix = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * node->rows * node->n_masks);
			if (node->n_groups != node->rows) {
			  node->del_interventions = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * node->n_groups * node->n_masks);
			  node->gen = true;
			  if (config.debug) std::cout << "alloc del_interventions " << node->n_groups << " " << node->n_masks << " " << node->del_interventions << std::endl;
			} else {
        node->del_interventions = node->base_target_matrix;
      }
		}
	} else if (op->type == PhysicalOperatorType::FILTER) {
		node->n_masks =  dynamic_cast<FadeNodeDense*>(fade_data[op->children[0]->id].get())->n_masks;
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
		node->n_groups = op->lineage_op->backward_lineage[0].size();
		node->rows = node->n_groups;

		if (config.prune) {
			node->opid = fade_data[op->children[0]->id]->opid;
	    fade_data[op->id] = std::move(node);
			return;
		}

		if (node->n_masks > 0) {
			node->del_interventions = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * node->n_groups * node->n_masks);
			node->gen = true;
		}

	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		int lhs_n = fade_data[op->children[0]->id]->n_interventions;
		int rhs_n = fade_data[op->children[1]->id]->n_interventions;
		node->n_interventions = (lhs_n > 0) ? lhs_n : rhs_n;
		node->n_masks = (lhs_n > 0) ? dynamic_cast<FadeNodeDense*>(fade_data[op->children[0]->id].get())->n_masks
		                            : dynamic_cast<FadeNodeDense*>(fade_data[op->children[1]->id].get())->n_masks;
		node->n_groups = op->lineage_op->backward_lineage[0].size();
		node->rows = node->n_groups;

		if (node->n_masks > 0) {
			node->del_interventions = (__mmask16*)aligned_alloc(64, sizeof(__mmask16) * node->n_groups * node->n_masks);
			node->gen = true;
		}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		node->gen = true;
		node->rows  = op->children[0]->lineage_op->chunk_collection.Count();
		node->n_masks = dynamic_cast<FadeNodeDense*>(fade_data[op->children[0]->id].get())->n_masks;
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
		node->GroupByAlloc(config.debug, op->type, op->lineage_op, op, config.aggid, config.groups);
	}  else if (op->type == PhysicalOperatorType::PROJECTION) {
		node->opid = fade_data[op->children[0]->id]->opid;
		node->n_masks  = dynamic_cast<FadeNodeDense*>(fade_data[op->children[0]->id].get())->n_masks;
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
	}

	if (config.debug) {
		std::cout << "AllocDense(" << op->id << ") -> n_masks:" << node->n_masks << ", n_interventions: " << node->n_interventions << std::endl;
	}

	fade_data[op->id] = std::move(node);
}

void Fade::GenSparseAndAlloc(EvalConfig& config, PhysicalOperator* op,
                             std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                             std::unordered_map<std::string, std::vector<std::string>>& columns_spec,
                             bool compile) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		GenSparseAndAlloc(config, op->children[i].get(), fade_data, columns_spec, compile);
	}

	unique_ptr<FadeSparseNode> node;
	if (compile) node = make_uniq<FadeNodeSparseCompile>(op->id, 0, config.num_worker, 0, config.debug);
	else node = make_uniq<FadeSparseNode>(op->id, 0, config.num_worker, 0, config.debug);

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		bool use_random = false;
		string table_name = op->lineage_op->table_name;
		if (columns_spec.find(table_name) == columns_spec.end()) { 
	    fade_data[op->id] = std::move(node);
      return;
    }
		int n_interventions = 0;
		int rows = 0;
		// iterate over all columns
		node->n_groups =  op->lineage_op->backward_lineage[0].size();
		for (auto& col_spec : columns_spec[table_name]) {
			std::ifstream rowsfile((table_name + "_" + col_spec + ".rows").c_str());
			if (rowsfile.is_open() && (rowsfile >> rows >> n_interventions) ) {
        config.specs_stack.push_back(table_name + "_" + col_spec);
			  if (config.debug)
          std::cout << "Annotations card for " << table_name << " " << col_spec << " has " <<
				  rows  << " with  " << n_interventions<< std::endl;
			  if (node->n_interventions > 0 && rows != node->rows) {
					std::cerr << "old rows doesn't match new rows" << std::endl;
					use_random = true;
					break;
			  }
			} else {
			  std::cerr << "Error opening metadata file or erros in # rows " <<
          rowsfile.is_open() << " " << n_interventions << " " << rows << " " << node->rows << std::endl;
			  use_random = true;
			  break;
			}

			node->rows = rows;
			rowsfile.close();
			use_random = node->read_annotations(n_interventions, rows, table_name, col_spec, config.debug);
		}
		if (use_random) {
			if (config.debug) std::cout << "generate random unique: " << config.n_intervention << std::endl;
			node->n_interventions = config.n_intervention;
			node->rows =  node->n_groups;
			node->base_annotations.reset(new int[node->n_groups]);
			Fade::random_unique(node->n_groups, node->base_annotations.get(), config.n_intervention);
		}

    if (node->rows > node->n_groups) {
			node->annotations.reset(new int[node->n_groups]);
    }
	} else if (op->type == PhysicalOperatorType::FILTER) {
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
		node->n_groups =  op->lineage_op->backward_lineage[0].size();
		node->rows =  node->n_groups;
		if (config.prune) node->opid = fade_data[op->children[0]->id]->opid;
		if (!config.prune && node->n_interventions > 1) {
      node->annotations.reset(new int[node->n_groups]);
    }
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		idx_t lhs_n =  fade_data[op->children[0]->id]->n_interventions;
		idx_t rhs_n =  fade_data[op->children[1]->id]->n_interventions;
    node->n_interventions = (lhs_n > 0) ? lhs_n : rhs_n;
		node->n_groups =  op->lineage_op->backward_lineage[0].size();
		node->rows =  node->n_groups;
		if (node->n_interventions > 1) node->annotations.reset(new int[node->n_groups]);
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY ||
	           op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY ||
	           op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		node->rows  = op->children[0]->lineage_op->chunk_collection.Count();
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
		node->GroupByAlloc(config.debug, op->type, op->lineage_op, op, config.aggid, config.groups);
		for (auto& out_var : node->alloc_vars_funcs) {
				string func = node->alloc_vars_funcs[out_var.first];
        if (func == "count") {
          config.groups_count.assign(node->n_groups, 0);
        }
        if (func == "avg" || func == "stddev" || func == "sum") {
          config.groups_sum.assign(node->n_groups, 0);
          if (func == "stddev") {
            config.groups_sum_2.assign(node->n_groups, 0);
          }
        }
    }
	}  else if (op->type == PhysicalOperatorType::PROJECTION) {
		node->n_interventions = fade_data[op->children[0]->id]->n_interventions;
		node->n_groups = fade_data[op->children[0]->id]->n_groups;
		node->rows = fade_data[op->children[0]->id]->rows;
		node->opid = fade_data[op->children[0]->id]->opid;
	}

	fade_data[op->id] = std::move(node);
}

void Fade::random_unique(int row_count, int* codes, idx_t distinct) {
	// Seed the random number generator
	std::random_device rd;
	std::mt19937 gen(rd());
	// Generate random values
	std::uniform_int_distribution<int> distribution(0, distinct - 1);
  	int count = 0;
	for (idx_t i = 0; i < row_count; ++i) {
		int random_value = distribution(gen);
		codes[count++] = random_value;
	}
}


void Fade::BindFunctions(EvalConfig& config, void* handle, PhysicalOperator* op,
                         std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data) {

	for (idx_t i = 0; i < op->children.size(); i++) {
		BindFunctions(config, handle, op->children[i].get(), fade_data);
	}

	FadeCompile* cur_node = dynamic_cast<FadeCompile*>(fade_data[op->id].get());

	if (op->type == PhysicalOperatorType::TABLE_SCAN || op->type == PhysicalOperatorType::FILTER) {
		string fname = "filter_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		cur_node->filter_fn = (int(*)(int, int*, void*, void*, const int, const int))dlsym(handle, fname.c_str());
	} else if (op->type == PhysicalOperatorType::HASH_JOIN
	           || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	           || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	           || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	           || op->type == PhysicalOperatorType::CROSS_PRODUCT) {
		//if (fade_data[op->id].n_masks > 0) {
		string fname = "join_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
		cur_node->join_fn = (int(*)(int, int*, int*, void*, void*, void*, const int, const int))dlsym(handle, fname.c_str());
		//}
	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	           || op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		string fname = "agg_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
    if ( dynamic_cast<FadeNode*>(fade_data[op->id].get())->has_agg_child) {
      cur_node->agg_fn_nested = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
                                         std::unordered_map<std::string, vector<void*>>&, const int, const int))dlsym(handle, fname.c_str());
    } else {
      cur_node->agg_fn = (int(*)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
          std::unordered_map<int, void*>&, const int, const int))dlsym(handle, fname.c_str());
        if (config.use_gb_backward_lineage) {
          string fname = "bw_agg_"+ to_string(op->id) + "_" + to_string(config.qid) + "_" + to_string(config.use_duckdb) +  "_" + to_string(config.is_scalar);
      cur_node->agg_fn_bw = (int(*)(int, std::vector<std::vector<int>>&, void*, std::unordered_map<std::string, vector<void*>>&,
          std::unordered_map<int, void*>&))dlsym(handle, fname.c_str());
        }
    }
	}
}

struct Compare {
	bool operator()(const std::pair<float, int>& a, const std::pair<float, int>& b) {
		return a.first == b.first ? a.second > b.second : a.first > b.first;
	}
};

std::vector<int> Fade::rank(PhysicalOperator* op, EvalConfig& config, std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data) {
	// get agg index
	//calculate minimize(sum(abs(output - new_agg_val[i])))
	std::vector<std::pair<float, int>> sums;

	for (auto &pair : fade_data[op->id]->alloc_vars) {
		if (!pair.second.empty()) {
			if (fade_data[op->id]->alloc_vars_types[pair.first] == "int") {
				int* data_ptr = (int*)pair.second[0];
				for (int j=0; j < fade_data[op->id]->n_interventions; j++) {
					float sum = 0;
					for (int i=0; i < fade_data[op->id]->n_groups; i++) {
						int index = i * fade_data[op->id]->n_interventions + j;
						sum +=  data_ptr[index];
					}
					sums.push_back(std::make_pair(sum, j));
				}
			} else {
				float* data_ptr = (float*)pair.second[0];
				for (int j=0; j < fade_data[op->id]->n_interventions; j++) {
					float sum = 0;
					for (int i=0; i < fade_data[op->id]->n_groups; i++) {
						int index = i * fade_data[op->id]->n_interventions + j;
						sum += data_ptr[index];
					}
					sums.push_back(std::make_pair(sum, j));
				}
			}
			break;
		}
	}


	std::priority_queue<std::pair<float, int>, std::vector<std::pair<float, int>>, Compare> pq;
	for (const auto& pair : sums) {
		pq.push(pair);
		if (pq.size() > config.topk) {
			pq.pop();
		}
	}

	vector<int> topk_vec;
	while (!pq.empty()) {
		int col = pq.top().second;
		float sum = pq.top().first;
		topk_vec.push_back(col);
		pq.pop();
	}

	return topk_vec;
}



} // namespace duckdb
#endif

