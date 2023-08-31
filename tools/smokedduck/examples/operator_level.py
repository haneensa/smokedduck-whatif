import smokedduck

def test_operator_level_lineage(con, plan: dict, qid: int):
    children = plan['children']
    lineage = con.get_operator_lineage(plan['name'], qid)
    print(plan["name"])
    print(lineage)
    for i in range(len(children)):
        test_operator_level_lineage(con, children[i], qid)

con = smokedduck.connect()

################### Filter
con.execute("CREATE TABLE t1(i INTEGER);")
con.execute("INSERT INTO t1 SELECT i FROM range(0,4000) tbl(i);")

q = "select rowid, * from t1 where 2 > i  OR i > 3998"
out_df = con.execute(q, capture_lineage="lineage").df()
print(out_df)
plan, qid = con.get_query_plan(q)
print(plan)
test_operator_level_lineage(con, plan, qid)

con.execute("drop table t1")

################### Block Nested Loop Join
con.execute("CREATE TABLE t1(i INTEGER, j INTEGER);")
con.execute("CREATE TABLE t2(i INTEGER);")

con.execute("INSERT INTO t1 SELECT i+1, i FROM range(0,10) tbl(i);")
con.execute("INSERT INTO t2 VALUES (1), (2), (1025);")

queries = [
"select t1.rowid, t2.rowid, t1.i, t1.j, t2.i from t1, t2 where t1.j == t2.i or t1.i == t2.i",
"select t1.rowid, t2.rowid, t1.i, t1.j from t1 left join t2 on t1.j == t2.i or t1.i == t2.i",
"select t1.rowid, t2.rowid, t1.i, t1.j, t2.i from t1 right join t2 on t1.j == t2.i or t1.i == t2.i",
"select t1.rowid, t2.rowid, t1.i, t1.j from t1 full outer join t2 on t1.j == t2.i or t1.i == t2.i",
"select t1.rowid, t1.i, t1.j from t1 semi join t2 on t1.j == t2.i or t1.i == t2.i"]

for q in queries:
    out_df = con.execute(q, capture_lineage="lineage").df()
    print(out_df)

    plan, qid = con.get_query_plan(q)
    test_operator_level_lineage(con, plan, qid)

con.execute("drop table t1")
con.execute("drop table t2")

#################### Nested Loop Join

con.execute("CREATE TABLE t1(i INTEGER);")
con.execute("CREATE TABLE t2(i INTEGER);")
con.execute("INSERT INTO t1 SELECT i FROM range(0,10) tbl(i);")
con.execute("INSERT INTO t1 VALUES (100);")
con.execute("INSERT INTO t2 VALUES (99), (100);")

queries = [
"select t1.rowid, t2.rowid, t1.i, t2.i from t1, t2 where  t1.i > t2.i and t1.i <> t2.i",
"select t1.rowid, t1.i from t1 semi join t2 on  (t1.i > t2.i)",
"select t1.rowid, t1.i from t1 anti join t2 on  (t1.i < t2.i)",
"select t1.rowid, t2.rowid, t1.i, t2.i from t1 right join t2  on t1.i > t2.i and t1.i <> t2.i",
"select t1.rowid, t2.rowid, t1.i, t2.i from t1 left join t2 on t1.i > t2.i and t1.i <> t2.i limit 10",
"select t1.rowid, t2.rowid, t1.i, t2.i from t1 full outer join t2 on t1.i > t2.i and t1.i <> t2.i"
]

for q in queries:
    out_df = con.execute(q, capture_lineage="lineage").df()
    print(out_df)

    plan, qid = con.get_query_plan(q)
    test_operator_level_lineage(con, plan, qid)

con.execute("drop table t1")
con.execute("drop table t2")

#################### Hash Join

con.execute("CREATE TABLE t1(i INTEGER, j INTEGER);")
con.execute("INSERT INTO t1 VALUES (3, 1),  (NULL, 2), (2, 3), (3, 4), (3, 8), (3, 7)")
con.execute("CREATE TABLE t2(i INTEGER, k VARCHAR(10));")
con.execute("INSERT INTO t2 VALUES (NULL, 'V'),   (2, 'B'), (3, 'C'), (1, 'D'), (3, 'A')")

queries = [
"select t1.rowid, t2.rowid, i, t1.j, t2.k from t1 left join t2 using(i) order by t1.rowid, t2.rowid",
"select t1.rowid, t2.rowid, i, t1.j, t2.k from t1 right join t2 using(i) order by t1.rowid, t2.rowid",
"select t1.rowid, t2.rowid, i, t1.j, t2.k from t1 full outer join t2 using(i)  order by t1.rowid, t2.rowid",
"select t1.rowid, * from t1 SEMI JOIN t2 ON t2.i = t1.i order by t1.rowid"
]

for q in queries:
    out_df = con.execute(q, capture_lineage="lineage").df()
    print(out_df)

    plan, qid = con.get_query_plan(q)
    test_operator_level_lineage(con, plan, qid)

con.execute("drop table t1")
con.execute("drop table t2")

con.execute("CREATE TABLE t1(i INTEGER, j INTEGER);")
con.execute("INSERT INTO t1 VALUES (3, 1),  (NULL, 2), (2, 3), (3, 4), (3, 8), (3, 7)")
con.execute("CREATE TABLE t2(i INTEGER, k VARCHAR(10));")
con.execute("INSERT INTO t2 VALUES (3, 'A'),   (2, 'B'), (3, 'C'), (1, 'D'), (3, 'A')")

queries = [
"select t1.rowid, t2.rowid, t1.j, t2.k from t1 inner join t2 on (t1.i = t2.i)"
]

for q in queries:
    out_df = con.execute(q, capture_lineage="lineage").df()
    print(out_df)

    plan, qid = con.get_query_plan(q)
    test_operator_level_lineage(con, plan, qid)

con.execute("drop table t1")
con.execute("drop table t2")

con.execute("CREATE TABLE t1(i INTEGER, j INTEGER);")
con.execute("INSERT INTO t1 VALUES (3, 1),  (NULL, 2), (2, 3), (3, 4), (3, 8), (3, 7)")
con.execute("CREATE TABLE t2(i INTEGER, k VARCHAR(10));")
con.execute("INSERT INTO t2 VALUES (NULL, 'V'),   (2, 'B'), (3, 'C'), (1, 'D'), (3, 'A')")

queries = [
"select t1.rowid, t2.rowid, t1.j, t2.k from t1 inner join t2 on (t1.i = t2.i)"
]

for q in queries:
    out_df = con.execute(q, capture_lineage="lineage").df()
    print(out_df)

    plan, qid = con.get_query_plan(q)
    test_operator_level_lineage(con, plan, qid)

con.execute("drop table t1")
con.execute("drop table t2")


##################### Hash Aggregate
con.execute("CREATE TABLE t1(i VARCHAR(10), j INTEGER);")
con.execute("INSERT INTO t1 VALUES ('H', 2),  ('O', 2), ('H', 4), ('M', 1), ('O', 1), ('H', 3), ('M', 2), ('O', 1)")
q = "select i, avg(j),  avg(distinct j) from t1 GROUP BY i"
out_df = con.execute(q, capture_lineage="lineage").df()
print(out_df)

plan, qid = con.get_query_plan(q)
test_operator_level_lineage(con, plan, qid)

con.execute("drop table t1")

##################### Perfect Hash Agg
con.execute("CREATE TABLE t1(i INTEGER, j INTEGER);")
con.execute("INSERT INTO t1 VALUES (1, 2),  (5, 2), (1, 4), (2, 1), (1, 3),(2, 2), (4, 1), (5, 1);")
q = "select t1.i, avg(t1.j) from t1 GROUP BY t1.i"

out_df = con.execute(q, capture_lineage="lineage").df()
print(out_df)

plan, qid = con.get_query_plan(q)
test_operator_level_lineage(con, plan, qid)

con.execute("drop table t1")

#################### Piecewise Merge Join
con.execute("CREATE TABLE t1(i INTEGER, j INTEGER);")
con.execute("INSERT INTO t1 VALUES (3, 1),  (NULL, 2), (2, 3), (3, 4), (3, 8), (3, 7)")
con.execute("INSERT INTO t1 SELECT i, i+1 FROM range(0,10) tbl(i);")

con.execute("CREATE TABLE t2(i INTEGER, k VARCHAR(10));")
con.execute("INSERT INTO t2 SELECT i, 'C' FROM range(0,10) tbl(i);")
con.execute("INSERT INTO t2 VALUES (3, 'A'),   (NULL, 'B'), (3, 'C'), (1, 'D'), (100, 'A')")

queries = [
"select t1.rowid, t2.rowid from t1 inner join t2 on (t1.i > t2.i)",
"select t1.rowid, t2.rowid from t1 right join t2 on (t1.i > t2.i)",
"select t1.rowid, t2.rowid from t1 left join t2 on (t1.i > t2.i)",
"select t1.rowid, t2.rowid from t1 full outer join t2 on (t1.i > t2.i)",
"select t1.rowid from t1 semi join t2 on (t1.i > t2.i)",
"select t2.rowid from t2 semi join t1 on (t1.i > t2.i)",
"select t1.rowid, t1.i, t1.j from t1 anti join t2 on (t1.i > t2.i)",
"select t2.rowid, t2.i, t2.k from t2 anti join t1 on (t1.i > t2.i)"
]

for q in queries:
    out_df = con.execute(q, capture_lineage="lineage").df()
    print(out_df)

    plan, qid = con.get_query_plan(q)
    test_operator_level_lineage(con, plan, qid)


con.execute("drop table t1")
con.execute("drop table t2")
