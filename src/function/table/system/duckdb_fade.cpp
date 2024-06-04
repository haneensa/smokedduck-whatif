#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/fade/fade.hpp"

namespace duckdb {

struct DuckDBFadeData : public GlobalTableFunctionState {
	DuckDBFadeData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBFadeBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

	if (global_fade_node == nullptr) return nullptr;

	// restriction: single agg at a time
	for (int i=0; i < global_fade_node->n_interventions; ++i) {
			names.emplace_back("i" + to_string(i));
		    // TODO: check the data type
			return_types.emplace_back(LogicalType::FLOAT);
	}

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBFadeInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBFadeData>();
	return std::move(result);
}

//! Create table to store executed queries with their IDs
//! Table name: queries_list
//! Schema: (INT query_id, varchar query)
void DuckDBFadeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBFadeData>();

	if (global_fade_node == nullptr || global_fade_node->alloc_vars.empty()) return;

	auto n_groups = global_fade_node->n_groups;
	auto n_interventions = global_fade_node->n_interventions;
	if (data.offset >= n_groups) {
		// finished returning values
		return;
	}
	idx_t count = 0;

	for (auto &pair : global_fade_node->alloc_vars) {

		// start returning values
		// either fill up the chunk or return all the remaining columns
		// TODO: figure out the best way to move data
		if (data.offset >= n_groups && count >= STANDARD_VECTOR_SIZE) {
			break;
		}
		idx_t col = 0;
    string name = global_fade_node->alloc_vars_funcs[pair.first];
		for (int i=0; i < global_fade_node->n_interventions; ++i) {
      // interventions X groups
			if (name == "count") {
				int* count_ptr = (int*)global_fade_node->alloc_vars["out_count"][0];
				int index = data.offset * n_interventions + i;
				output.SetValue(col++, count, Value::FLOAT( (int)count_ptr[index] ));
			} else if (name  == "sum" || name == "sum_no_overflow") {
		    void* data_ptr = pair.second[0];
				int index = data.offset * n_interventions + i;
				output.SetValue(col++, count,Value::FLOAT( ((float*)data_ptr)[index] ));
			} else if (name  == "avg") {
        // TODO: check data type
		    float* data_ptr = (float*)pair.second[0];
				int* count_ptr = (int*)global_fade_node->alloc_vars["out_count"][0];
				int index = data.offset * n_interventions + i;
        if (count_ptr[index] > 0) {
				  output.SetValue(col++, count,Value::FLOAT( data_ptr[index] / count_ptr[index] ));
        } else {
				  output.SetValue(col++, count,Value::FLOAT( 0 ));
        }
			}

		}
		count++;
		data.offset++;
	}


	output.SetCardinality(count);
}

void DuckDBFadeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_fade", {}, DuckDBFadeFunction, DuckDBFadeBind, DuckDBFadeInit));
}

} // namespace duckdb

