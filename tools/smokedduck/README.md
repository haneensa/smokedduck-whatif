## SmokedDuck
[SmokedDuck](https://github.com/haneensa/duckdb_lineage/tree/sd_release) is a fork of DuckDB instrumented to provide fine-grained (row) provenance also known as lineage.
It is designed to capture lineage with low overhead and query lineage. 
We're releasing this experimental prototype to support researchers and academics in using lineage to support provenance-powered use cases.

Please reach out to us if you're considering building something interesting with SmokedDuck or if you run into any issues. 
You can reach us quickly by creating a GitHub issue or emailing us (smokedduckdb@gmail.com).

If you run into performance issues, also please reach out to us. 
This is a reimplementation of our prior work that is in submission and focuses on functionality rather than performance.  
See the Roadmap section below for the current set of priorities.
We would love to prioritize any issues that are meaningful to folks attempting to use this project.


## DuckDB
DuckDB is a high-performance analytical database system. It is designed to be fast, reliable and easy to use. DuckDB provides a rich SQL dialect, with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs), and more. For more information on the goals of DuckDB, please refer to [the Why DuckDB page on the DuckDB website](https://duckdb.org/why_duckdb).

## Roadmap
- [ ] Support Window functions.
- [ ] Automatically create a ticket when a new major DuckDB version is released so we don't fall behind.
- [ ] Full testing over all operator code paths to ensure complete lineage coverage.
- [ ] Track captured lineage metadata so that we need to disable the optimizer during lineage querying.
- [ ] Automatically disable short-circuiting when capturing lineage.
- [ ] Port optimized lineage querying (using low cost lineage indexes) to this version of SmokedDuck.
- [ ] Create a Lineage Buffer Pool and explore compressed lineage to ensure we can stay within a user-specified budget of memory/disk.
- [ ] Support additional aggregation types including custom aggregation functions in KSemirings.

## Installation
We currently support running SmokedDuck in Python. If you would like to use another language, please contact us.
```shell
pip install smokedduck
```

## Minimal SmokedDuck Use Case (End-to-End Lineage)
```python
import smokedduck
import pandas as pd

# Creating connection
con = smokedduck.connect(':default:')

# Loading example data
p1 = pd.DataFrame({'a': [42, 43, 44, 45], 'b': ['a', 'b', 'a', 'b']})
p2 = pd.DataFrame({'b': ['a', 'a', 'c', 'b'], 'c': [4, 5, 6, 7]})
con.execute('create table t1 as (select * from p1)')
con.execute('create table t2 as (select * from p2)')

# Executing base query
con.execute('SELECT t1.b, sum(a + c) FROM t1 join (select b, avg(c) as c from t2 group by b) as t2 on t1.b = t2.b group by t1.b', capture_lineage='lineage').df()

# Printing lineage that was captured from base query
print(con.lineage().df())
```
Output
```text
   t1  t2  out_index
0   2   1          0
1   3   3          1
2   2   0          0
3   0   1          0
4   1   3          1
5   0   0          0
```

## Minimal SmokedDuck Use Case (Operator-Level Lineage)
```python
import smokedduck

con = smokedduck.connect()

con.execute("CREATE TABLE t1(i INTEGER);")
con.execute("INSERT INTO t1 SELECT i FROM range(0,4000) tbl(i);")

q = "select rowid, * from t1 where 2 > i  OR i > 3998"
out_df = con.execute(q, capture_lineage="lineage").df()
plan, qid = con.get_query_plan(q)
# {'name': 'PROJECTION_2', 'children': [{'name': 'FILTER_1', 'children': [{'name': 'SEQ_SCAN_0', 'children': [], 'table': 't1'}], 'table': ''}], 'table': ''}
lineage = con.get_operator_lineage('FILTER_1', qid)
print(lineage)
```
Output
```text
   in_index out_index
0   0   0
1   1   1
2   3999   2
```


## More Examples
- [Expanded Simple Example](https://github.com/haneensa/duckdb_lineage/blob/sd_release/smokedduck/test.py)
- [Crossfilter Example](https://github.com/haneensa/duckdb_lineage/blob/sd_release/smokedduck/crossfilter_test.py)
- [KSemimodule Example](https://github.com/haneensa/duckdb_lineage/blob/sd_release/smokedduck/ksemimodule_test.py)

## SmokedDuck Interface
The SmokedDuck object returned from `smokedduck.connect(...)` complies with the same interface as DuckDB's [DuckDBPyConnection](https://duckdb.org/docs/api/python/reference/#duckdb.DuckDBPyConnection) so check out it's interface for more details. We edit and expand upon the interface in the following ways:
### Edits
- `execute(self, query: str, capture_lineage: str = None, parameters: object = None, multiple_parameter_sets: bool = False) -> duckdb.DuckDBPyRelation`: changes the existing DuckDB `execute(...)` method with the ability to pass in a provenance model to the `capture_lineage` parameter as a string. See the Provenance Models section below to see the details. Also returns a DuckDBPyRelation instead of a DuckDBPyConnection.
### Extensions
- `lineage(self) -> duckdb.DuckDBPyConnection`: queries the last executed query with the `lineage` provenance model.
- `why(self) -> duckdb.DuckDBPyConnection`: queries the last executed query with the `why` provenance model.
- `polynomial(self) -> duckdb.DuckDBPyConnection`: queries the last executed query with the `polynomial` provenance model.
- `ksemimodule(self) -> duckdb.DuckDBPyConnection`: queries the last executed query with the `ksemimodule` provenance model.
- `backward(self, backward_ids: list, model: str = 'lineage') -> duckdb.DuckDBPyConnection`: executes a backward lineage query where the output indexes are filtered by the indexes in the passed `backward_ids` list. The provenance model defaults to `lineage` but can be overwritten.
- `forward(self, forward_table: str, forward_ids: list, model: str = 'lineage') -> duckdb.DuckDBPyConnection`: executes a forward lineage query where the input indexes from the `forward_table` are filtered by the indexes in the passed `forward_ids` list. The provenance model defaults to `lineage` but can be overwritten.

## Provenance Models
The following details how each provenance model affects capturing and querying:
| Provenance Model | Reference | Capture Mechanics | Querying Mechanics |
| --- | --- | --- | --- |
| `lineage` | [Cui et al. 2000](https://dl.acm.org/doi/pdf/10.1145/357775.357777) | Lineage captures the index of every tuple that contributes to each row in the output result set. | Lineage querying returns a column for each joined table in the base query. The cell value is the index of the row that contributed to the out_index. If multiple rows from a table contribute to an output such as when there's an aggregation, multiple rows are returned by the lineage query (similar to Perm). Self-joins lead to multiple columns that correspond to that table. |
| `why` | [Buneman et al. 2001](https://www.cis.upenn.edu/~sanjeev/papers/icdt01_data_provenance.pdf) | Why provenance captures the same data as Lineage and only differs in querying. | Why provenance produces a single row per out_index, creating a list of lists containing input indexes. Each internal list identifies one set of witnesses that contribute to the query. Base query aggregation results in multiple internal lists. |
| `polynomial` | [Green et al. 2007](https://web.cs.ucdavis.edu/~green/papers/pods07.pdf) | Polynomial (referring to Provenance Polynomials) captures the same data as Lineage and only differs in querying. | Polynomial returns how each out_index was created in the form of a polynomial. Items multiplied together must co-occur, and those added together can either occur in order for the out_index to exist. In general, joins produce multiplication and aggregations product addition. |
| `ksemimodule` | [Amsterdamer et al. 2011](https://arxiv.org/pdf/1101.1110.pdf) | K-Semimodule captures the same data as Lineage, and also captures all query intermediates that are materialized before an aggregation. | Querying K-Semimodule lineage results in a partial recalculation of a base query aggregate over whichever input or output tuples are selected. We're expanding the set of aggregate functions and complexity of queries that are supported. |

Note that all provenance models turn off DuckDB short-circuiting to ensure complete lineage is captured.
In particular, semi and anti joins are turned off within the optimizer, since otherwise not all input indexes that match the join condition would be considered.
Let us know if you're interested in a provenance capture mode that supports short-circuiting.

## SQL Reference
The [website](https://duckdb.org/docs/sql/introduction) contains a reference of functions and SQL constructs available in DuckDB.

## Development 
For development, DuckDB requires [CMake](https://cmake.org), Python3 and a `C++11` compliant compiler. Run `make` in the root directory to compile the sources. For development, use `make debug` to build a non-optimized debug version. You should run `make unit` and `make allunit` to verify that your version works properly after making changes. To test performance, you can run `BUILD_BENCHMARK=1 BUILD_TPCH=1 make` and then perform several standard benchmarks from the root directory by executing `./build/release/benchmark/benchmark_runner`. The detail of benchmarks is in our [Benchmark Guide](benchmark/README.md).

For SmokedDuck development, you can build SmokedDuck into a local virtualenv with the following commands:
```shell
# Create virtualenv
python -m venv venvPythonLineage
source venvPythonLineage/bin/activate

# Install other useful packages
pip install numpy
pip install pandas

# Build SmokedDuck locally
cd tools/pythonpkg
BUILD_LINEAGE=true python setup.py install

# Run SmokedDuck
cd ../..
python smokedduck/minimal_test.py
```
