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


    # get query
    # get good group ids
    # get bad groups ids
    results = scorpionutil.scorpion_run(g.db, data, requestid)
    return results
  except:
    return {}

  ret['results'] = results
  ret['top_k_results'] = top_k
  return ret


def runfade(sql, aggid, goodids, badids, query_id=None):
    con = smokedduck.connect('intel.db')
    clear(con)

    #parser = argparse.ArgumentParser()
    #parser.add_argument("--specs", help="|table.col", type=str, default="intel.moteid")
    #parser.add_argument("--sql", help="sql", type=str, default="select hrint, count() as count from intel group by hrint")
    #parser.add_argument("--aggid", help="agg_name", type=str, default=0)
    #args = parser.parse_args()
    
    intervened_attrs_list = [
        ["moteid"],
        ["voltage"],
        ["light"],
        ["moteid", "voltage"]
    ]

    if (query_id is None):
        start = time.time()
        out = con.execute(sql, capture_lineage='lineageAll').df()
        end = time.time()
        print(out)
        query_timing = end - start
        print(query_timing)
        query_id = con.query_id

    pp_timings = con.execute(f"pragma PrepareLineage({query_id}, false, false, false)").df()

    q = f"pragma WhatIfSparse({query_id}, {aggid}, '{args.specs}', false);"
    res = con.execute(q).fetchdf()
    print(res)

    goodids = list(map(str, goodids ))
    badids = list(map(str, badids ))
    avggood = f"({'+'.join(goodids)})/{len(goodids)}"
    avgbad = f"({'+'.join(badids)})/{len(badids)}"
    q = f"""select {avggood} as avggood, {avgbad} as avgbad 
    from duckdb_fade() order by {abs({avggood}-{avgbad})}"""
    res = con.execute(q).fetchdf()
    print(res)

    q = f"pragma GetPredicate(0);"
    res = con.execute(q).fetchdf()
    print(res)
    clear(con)

    

if __name__ == "__main__":


  import psycopg2
  DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
  print "registering type"
  psycopg2.extensions.register_type(DEC2FLOAT)


  app.run(host="localhost", port="8111", debug=True, threaded=True)


