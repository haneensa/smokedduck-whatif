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

unique_ptr<FadeNode> global_fade_node;


template<class T1, class T2>
T2* Fade::GetInputVals(PhysicalOperator* op, idx_t col_idx) {
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
void FadeNode::PrintOutput(T* data_ptr) {
	for (int i=0; i < n_groups; i++) {
		for (int j=0; j < n_interventions; j++) {
			int index = i * n_interventions + j;
			std::cout << " G: " << i << " I: " << j << " -> " <<  data_ptr[index] << std::endl;
		}
	}
}

template<class T>
void FadeNode::allocate_agg_output(string typ, int t, int n_interventions, string out_var) {
	alloc_vars_types[out_var] =typ;
	alloc_vars[out_var][t] = aligned_alloc(64, sizeof(T) * n_groups * n_interventions);
	if (alloc_vars[out_var][t] == nullptr) {
		alloc_vars[out_var][t] = malloc(sizeof(T) * n_groups * n_interventions);
	}
	memset(alloc_vars[out_var][t], 0, sizeof(T) * n_groups * n_interventions);
}

void FadeNode::GroupByGetCachedData(EvalConfig& config, shared_ptr<OperatorLineage> lop,
                                PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                                int keys_size) {
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

		if (name == "sum" || name == "sum_no_overflow" || name == "avg") {
			int col_idx = aggregate_input_idx[0]; //i + keys_size;
			if (config.use_duckdb == false) {
				if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::INTEGER) {
					input_data_map[i] = Fade::GetInputVals<int, int>(op, col_idx);
        } else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::BIGINT) {
					input_data_map[i] = Fade::GetInputVals<int64_t, int>(op, col_idx);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::FLOAT) {
					input_data_map[i] = Fade::GetInputVals<float, float>(op,  col_idx);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::DOUBLE) {
					input_data_map[i] = Fade::GetInputVals<double, float>(op,  col_idx);
				} else {
					input_data_map[i] = Fade::GetInputVals<float, float>(op,  col_idx);
				}
			}

		}
	}
}

void FadeNode::LocalGroupByAlloc(bool debug,
                            shared_ptr<OperatorLineage> lop,
                            PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
                            int keys_size) {
	if (this->n_groups * this->n_interventions  > this->rows) this->num_worker = 1;

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
			alloc_vars[out_var].resize(this->num_worker);
			alloc_vars_funcs[out_var] = name;
			for (int t=0; t < this->num_worker; ++t) {
				if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::INTEGER ||
				    op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::BIGINT
				) {
					this->allocate_agg_output<int>("int", t, n_interventions, out_var);
				} else if (op->children[0]->lineage_op->chunk_collection.Types()[col_idx] == LogicalType::FLOAT) {
					this->allocate_agg_output<float>("float", t, n_interventions, out_var);
				} else {
					this->allocate_agg_output<float>("float", t, n_interventions, out_var);
				}
			}
			alloc_vars_index[out_var] = i;
		}
	}

	if (include_count == true) {
		string out_var = "out_count";
		alloc_vars_funcs[out_var] = "count";
		alloc_vars[out_var].resize(this->num_worker);
		for (int t=0; t < this->num_worker; ++t) {
			this->allocate_agg_output<int>("int", t, n_interventions, out_var);
		}
		alloc_vars_index[out_var] = -1;
	}
}

// if nested, then take the output of the previous agg as input
void FadeNode::GroupByAlloc(bool debug, PhysicalOperatorType typ,
                            shared_ptr<OperatorLineage> lop,
                            PhysicalOperator* op) {
	// To support nested agg, check if any descendants is an agg
	PhysicalOperator* cur_op = op->children[0].get();
	while (cur_op && !cur_op->children.empty() && !(cur_op->type == PhysicalOperatorType::HASH_GROUP_BY
	                                                || cur_op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	                                                || cur_op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE)) {
		cur_op = cur_op->children[0].get();
	}
	if (cur_op->type == PhysicalOperatorType::HASH_GROUP_BY
	    || cur_op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	    || cur_op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		this->has_agg_child = true;
		this->child_agg_id = cur_op->id;
	}

	if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(op);
		auto &aggregates = gb->grouped_aggregate_data.aggregates;
		this->n_groups = op->lineage_op->log_index->ha_hash_index.size();
		if (!this->has_agg_child) {
			this->LocalGroupByAlloc(debug, op->lineage_op, op, aggregates, gb->grouped_aggregate_data.groups.size());
		}
	} else if (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
		PhysicalPerfectHashAggregate * gb = dynamic_cast<PhysicalPerfectHashAggregate *>(op);
		auto &aggregates = gb->aggregates;
		this->n_groups = op->lineage_op->log_index->pha_hash_index.size();
		if (!this->has_agg_child) {
			this->LocalGroupByAlloc(debug, op->lineage_op, op, aggregates, gb->groups.size());
		}
	} else {
		PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(op);
		auto &aggregates = gb->aggregates;
		this->n_groups = 1;
		if (!this->has_agg_child) {
			this->LocalGroupByAlloc(debug, op->lineage_op, op, aggregates, 0);
		}
	}
}

void loca_debug_dense_matrix(int opid, int rows, int n_interventions, int n_masks, __mmask16* dense_matrix) {
	vector<int> nonzero_count(n_interventions);
	std::cout << opid << "-> n_masks: " << n_masks << " n_interventions: " << n_interventions << " rows: " << rows << std::endl;
	for (int r = 0; r < rows; r++) {
		for (int c = 0; c < n_masks; c++)
			for (int k = 0; k < 16; ++k) {
			  if (dense_matrix[r * n_masks + c] & (1 << k))
					nonzero_count[c * 16 + k]++;
			}
	}

	for (int c = 0; c < nonzero_count.size(); c++)
		std::cout << c << " " << nonzero_count[c] << std::endl;
}

void FadeNodeDense::debug_dense_matrix() {
	if (base_target_matrix) {
		loca_debug_dense_matrix(opid, rows, n_interventions, n_masks, base_target_matrix);
	} else {
		loca_debug_dense_matrix(opid, n_groups, n_interventions, n_masks, del_interventions);
	}
}

bool FadeNodeDense::read_dense_from_file(bool debug, string col_spec, string table_name) {
	FILE * fname = fopen((table_name + ".npy").c_str(), "r");
	if (fname == nullptr) {
		std::cerr << "Error: Unable to open file " << table_name << std::endl;
		return false;
	}

	std::stringstream ss(col_spec);
	string prefix, rows_str, cols_str;
	getline(ss, prefix, '_');
	getline(ss, rows_str, '_');
	getline(ss, cols_str, '_');

	int local_rows = std::stoi(rows_str);
	int local_cols = std::stoi(cols_str);

	uint8_t* temp  = (uint8_t*)aligned_alloc(64, sizeof(uint8_t) * local_rows * local_cols);
	size_t fbytes = fread(temp, sizeof(uint8_t), sizeof(uint8_t) * local_rows * local_cols,  fname);
	if ( fbytes != sizeof(uint8_t) * local_rows * local_cols) {
		std::cerr << "read failed " << std::endl;
		free(temp);
		return false;
	}

	if (debug) std::cout << "Use Pre generated interventions table_name: " <<
		    table_name <<  ", col_spec: " << col_spec  << ", rows: " <<
		    local_rows << ", cols: " << local_cols << std::endl;

	this->n_masks =  local_cols / 2;
	this->n_interventions = local_cols * 8;
	this->base_target_matrix = (__mmask16*)temp;
	this->rows = rows;

	if (debug) this->debug_dense_matrix( );

	fclose(fname);
	return true;
}

bool FadeSparseNode::read_annotations(int new_n_interventions, int rows, string& table_name, string& col_spec, bool debug) {
	FILE * fname = fopen((table_name + "_" + col_spec + ".npy").c_str(), "r");
	if (fname == nullptr) {
		std::cerr << "Error: Unable to open file." << std::endl;
		return true;
	}

	// read the first line to get cardinality
	unique_ptr<int[]> temp(new int[rows]);
	size_t fbytes = fread(temp.get(), sizeof(int), rows ,  fname);
	if ( fbytes != rows ) {
		std::cerr << "Error: Unable to open file." << std::endl;
		return true;
	}

	if (n_interventions > 0) {
		for (int i = 0 ; i < rows;  ++i) {
			base_annotations[i] = base_annotations[i] * new_n_interventions + temp[i];
		}
    n_interventions *= new_n_interventions;
	} else {
		base_annotations = std::move(temp);
    n_interventions = new_n_interventions;
	}

	return false;
}

} // namespace duckdb
#endif

