//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/lineage_top.hpp
//
// Generic interface for lineage artifact data types
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
struct LogRecord;

class LineageData {
public:
	LineageData(idx_t count) : count(count), processed(false) {}

  //! Return number of entries in the artifact
	virtual idx_t Count() {
		return count;
	}
   
  //! Return a Vector container for artifact with 'type' as LogicalType
  //! and 'offset' added to every entry
	virtual Vector GetVecRef(LogicalType type, idx_t offset) {
		Vector vec(type, Process(offset));
		return vec;
	}
	
  //! Print out content of the artifact
  virtual void Debug() = 0;

  //! Return a pointer to the artifact with entries adjusted by 'offset'
	virtual data_ptr_t Process(idx_t offset) = 0;

	virtual void SetChild(shared_ptr<LogRecord> c) {
		child = move(c);
	}

	virtual shared_ptr<LogRecord> GetChild() {
		return child;
	}

  //! Return the size of artifacts in bytes
	virtual idx_t Size() = 0;

	virtual idx_t At(idx_t) = 0;

	virtual ~LineageData() {};

public:
	idx_t count;
	shared_ptr<LogRecord> child;
	bool processed;
};

} // namespace duckdb
#endif
