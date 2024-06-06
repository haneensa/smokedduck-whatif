import numpy as np
import time
import csv
import argparse
import smokedduck
import pandas as pd


from functools import wraps
from collections import *
from datetime import datetime
from flask import Flask, request, render_template, g, redirect, Response, jsonify
from flask_compress import Compress
from flask_cors import CORS, cross_origin



tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
print tmpl_dir
app = Flask(__name__, template_folder=tmpl_dir)
#CORS(Compress(app), supports_credentials=True)


def build_preflight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add('Access-Control-Allow-Headers', "*")
    response.headers.add('Access-Control-Allow-Methods', "*")
    return response
def build_actual_response(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response


def clear(c):
    tables = c.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            print("drop", row["name"])
            c.execute("DROP TABLE "+row["name"])
    c.execute("PRAGMA clear_lineage")


@app.route('/api/scorpion/', methods=['POST', 'GET'])
#@cross_origin(origins="*")
@returns_json
def scorpion():
  try:
    data =  json.loads(str(request.form['json']))
    requestid = request.form.get('requestid')
    print("running scorpion")
    results = scorpionutil.scorpion_run(g.db, data, requestid)
    return results
  except:
    return {}

  ret['results'] = results
  ret['top_k_results'] = top_k
  return ret



@app.route('/api/query/', methods=['POST', 'GET'])
@returns_json
def api_query():
  ret = { }
  jsonstr = request.args.get('json')
  if not jsonstr:
    print "query: no json string.  giving up"
    return ret

  args = json.loads(jsonstr)
  dbname = args.get('db')
  table = args.get('table')

  o, params = create_sql_obj(g.db, args)
  o.limit = 10000;
  query = str(o)
  print query
  print params

  if not dbname or not table or not query:
    print "query: no db/table/query.  giving up"
    return ret

  try:
    conn = g.db
    cur = conn.execute(query, [params])
    rows = cur.fetchall()
    cur.close()

    data = [dict(zip(cur.keys(), vals)) for vals in rows]
    ret['data'] = data
    ret['schema'] = get_schema(g.db, table)

  except Exception as e:
    traceback.print_exc()
    ret = {}

  print "%d points returned" % len(ret.get('data', []))
  return ret



con = smokedduck.connect('intel.db')
clear(con)



tables = con.execute("PRAGMA show_tables").fetchdf()

for t in tables["name"]:
    print(t)
    print(con.execute(f"pragma table_info('{t}')"))

parser = argparse.ArgumentParser()
parser.add_argument("--specs", help="|table.col", type=str, default="intel.moteid")
parser.add_argument("--sql", help="sql", type=str, default="select hrint, count() as count from intel group by hrint")
parser.add_argument("--aggid", help="agg_name", type=str, default=0)
args = parser.parse_args()

specs_tokens = args.specs.split('|')

cols = []
for token in specs_tokens:
    table_col = token.split('.')
    table = table_col[0]
    col = table_col[1]
    cols.append(col)

print(args.sql)

start = time.time()
out = con.execute(args.sql, capture_lineage='lineageAll').df()
end = time.time()
print(out)
query_timing = end - start
print(query_timing)
query_id = con.query_id

pp_timings = con.execute(f"pragma PrepareLineage({query_id}, false, false, false)").df()

q = f"pragma WhatIfSparse({query_id}, {args.aggid}, '{args.specs}', false);"
res = con.execute(q).fetchdf()
print(res)

q = f"select * from duckdb_fade() where g0 > 0;"
res = con.execute(q).fetchdf()
print(res)

q = f"pragma GetPredicate(0);"
res = con.execute(q).fetchdf()
print(res)

clear(con)

#cols_str = ",".join(cols)
#args.sql += ", " + cols_str
#args.sql += " order by " + cols_str
#
#sql = args.sql.lower().replace("select", "select " + cols_str + ",")
#print(sql)
#print(cols_str)
#
#out = con.execute(sql).fetchdf()
#print(out)



if __name__ == "__main__":


  import psycopg2
  DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
  print "registering type"
  psycopg2.extensions.register_type(DEC2FLOAT)


  app.run(host="localhost", port="8111", debug=True, threaded=True)


