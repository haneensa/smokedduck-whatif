<div align="center">
  <img src="https://duckdb.org/images/DuckDB_Logo_dl.png" height="50">
</div>
<p>&nbsp;</p>

<p align="center">
  <a href="https://github.com/duckdb/duckdb/actions">
    <img src="https://github.com/duckdb/duckdb/actions/workflows/Main.yml/badge.svg?branch=master" alt="Github Actions Badge">
  </a>
  <a href="https://app.codecov.io/gh/duckdb/duckdb">
    <img src="https://codecov.io/gh/duckdb/duckdb/branch/master/graph/badge.svg?token=FaxjcfFghN" alt="codecov"/>
  </a>
  <a href="https://discord.gg/tcvwpjfnZx">
    <img src="https://shields.io/discord/909674491309850675" alt="discord" />
  </a>
  <a href="https://github.com/duckdb/duckdb/releases/">
    <img src="https://img.shields.io/github/v/release/duckdb/duckdb?color=brightgreen&display_name=tag&logo=duckdb&logoColor=white" alt="Latest Release">
  </a>
</p>

## SmokedDuck
SmokedDuck is a fork of DuckDB instrumented to provide fine-grained (row) provenance also known as lineage. It is designed to capture lineage with low overhead and query lineage quickly. We're released this prototype to support researchers and academia in using lineage to support provenance-powered use cases.

Please reach out to us if you're considering building something interesting with SmokedDuck or if you run into any issues. You can reach us quickly by creating a GitHub issue or emailing us (Eugene Wu, ewu@cs.columbia.edu; Haneen Mohammed, ham2156@columbia.edu; Charlie Summers, cgs2161@columbia.edu).

## DuckDB
DuckDB is a high-performance analytical database system. It is designed to be fast, reliable and easy to use. DuckDB provides a rich SQL dialect, with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs), and more. For more information on the goals of DuckDB, please refer to [the Why DuckDB page on the DuckDB website](https://duckdb.org/why_duckdb).

## Installation
We currently support running SmokedDuck in Python. If you would like to use another language, please contact us.
```shell
pip install smokedduck
```

## Minimal SmokedDuck Use Case
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
0   0   3          0
1   0   1          0
2   2   2          1
3   0   0          0
```

## More Examples
- [Expanded Simple Example](smokedduck/test.py)
- [Crossfilter Example](smokedduck/crossfilter_test.py)
- [KSemimodule Example](smokedduck/ksemimodule_test.py)

## SmokedDuck Interface
The SmokedDuck object returned from `smokedduck.connect(...)` complies with the same interface as DuckDB's [DuckDBPyConnection](https://duckdb.org/docs/api/python/reference/#duckdb.DuckDBPyConnection) so check out it's interface for more details. We edit and expand upon the interface in the following ways:
### Edits
- `execute(self, query, capture_lineage=None, parameters=None, multiple_parameter_sets=None)`: changes the existing DuckDB `execute(...)` method with the ability to pass in a provenance model to the `capture_lineage` parameter as a string. See the Provenance Models section below to see the details.
### Extensions
- `lineage(self)`: queries the last executed query with the `lineage` provenance model.
- `why(self)`: queries the last executed query with the `why` provenance model.
- `polynomial(self)`: queries the last executed query with the `polynomial` provenance model.
- `ksemimodule(self)`: queries the last executed query with the `ksemimodule` provenance model.
- `backward(self, backward_ids, model='lineage')`: executes a backward lineage query where the output indexes are filtered by the indexes in the passed `backward_ids` list. The provenance model defaults to `lineage` but can be overwritten.
- `forward(self, forward_table, forward_ids, model='lineage')`: executes a forward lineage query where the input indexes from the `forward_table` are filtered by the indexes in the passed `forward_ids` list. The provenance model defaults to `lineage` but can be overwritten.

## Provenance Models
The following details how each provenance model affects capturing and querying:
| Provenance Model | Capture Mechanics | Querying Mechanics |
| --- | --- | --- |
| `lineage` | Lineage captures the index of every tuple that contributes to each row in the output result set. | Lineage querying returns a column for each joined table in the base query. The cell value is the index of the row that contributed to the out_index. If multiple rows from a table contribute to an output such as when there's an aggregation, multiple rows are returned by the lineage query (similar to Perm). Self-joins lead to multiple columns that correspond to that table.
| `why` | Why provenance captures the same data as Lineage and only differs in querying. | Why provenance produces a single row per out_index, creating a list of lists containing input indexes. Each internal list identifies one set of witnesses that contribute to the query. Base query aggregation results in multiple internal lists. |
| `polynomial` | Polynomial (referring to Provenance Polynomials) captures the same data as Lineage and only differs in querying. | Polynomial returns how each out_index was created in the form of a polynomial. Items multiplied together must co-occur, and those added together can either occur in order for the out_index to exist. In general, joins produce multiplication and aggregations product addition. |
| `ksemimodule` | K-Semimodule captures the same data as Lineage, and also captures all query intermediates that are materialized before an aggregation. | Querying K-Semimodule lineage results in a partial recalculation of a base query aggregate over whichever input or output tuples are selected. We're expanding the set of aggregate functions and complexity of queries that are supported. |

Note that all provenance models turn off DuckDB short-circuiting to ensure complete lineage is captured. In particular, semi and anti joins are turned off within the optimizer, since otherwise not all input indexes that match the join condition would be considered. Let us know if you're interested in a provenance capture mode that supports short-circuiting.

## Data Import
For CSV files and Parquet files, data import is as simple as referencing the file in the FROM clause:

```sql
SELECT * FROM 'myfile.csv';
SELECT * FROM 'myfile.parquet';
```

Refer to our [Data Import](https://duckdb.org/docs/data/overview) section for more information.

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
python setup.py install

# Run SmokedDuck
cd ../..
python smokedduck/minimal_test.py
```
