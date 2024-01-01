#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct DuckDBQueriesListData : public GlobalTableFunctionState {
	DuckDBQueriesListData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBQueriesListBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("size_bytes_max");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("size_bytes_min");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("nchunks");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("postprocess_time");
	return_types.emplace_back(LogicalType::FLOAT);

  names.emplace_back("plan");
	return_types.emplace_back(LogicalType::VARCHAR);


	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBQueriesListInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueriesListData>();
	return std::move(result);
}

static string JSONSanitize(const string &text) {
	string result;
	result.reserve(text.size());
	for (idx_t i = 0; i < text.size(); i++) {
		switch (text[i]) {
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		default:
			result += text[i];
			break;
		}
	}
	return result;
}

string PlanToString(PhysicalOperator *op) {
	string child_str;
	for (idx_t i = 0; i < op->children.size(); i++) {
		child_str += PlanToString(op->children[i].get());
		if (i != op->children.size() - 1) {
			child_str += ",";
		}
	}
	return "{\"name\": \"" + op->GetName() + "\",\"children\": [" + child_str + "],\"table\": \"" + op->lineage_op->table_name +  "\",\"extra\": \"" + JSONSanitize(op->ParamsToString())+ "\"}";
}

//! Create table to store executed queries with their IDs
//! Table name: queries_list
//! Schema: (INT query_id, varchar query)
void DuckDBQueriesListFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueriesListData>();
	auto query_to_id = context.client_data->lineage_manager->query_to_id;
	if (data.offset >= query_to_id.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
  std::vector<idx_t> stats(3, 0);
	while (data.offset < query_to_id.size() && count < STANDARD_VECTOR_SIZE) {
		string query = query_to_id[data.offset];
		idx_t col = 0;
		// query_id, INT
		output.SetValue(col++, count,Value::INTEGER(data.offset));
		// query, VARCHAR
		output.SetValue(col++, count, query);

    // size_byes_max
		output.SetValue(col++, count,Value::INTEGER(stats[0]));

    // size_bytes_min
		output.SetValue(col++, count,Value::INTEGER(stats[2]));

    // nchunks
		output.SetValue(col++, count,Value::INTEGER(stats[1]));

    // postprocess_time
    float postprocess_time = 0.0;//((float) end - start) / CLOCKS_PER_SEC;
		output.SetValue(col++, count,Value::FLOAT(postprocess_time));

    // plan, VARCHAR
		output.SetValue(col++, count, PlanToString(
		                                  context.client_data->lineage_manager->queryid_to_plan[data.offset].get()
		                                  ));

		count++;
		data.offset++;
	}
	output.SetCardinality(count);
}

void DuckDBQueriesListFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_queries_list", {}, DuckDBQueriesListFunction, DuckDBQueriesListBind, DuckDBQueriesListInit));
}

} // namespace duckdb
