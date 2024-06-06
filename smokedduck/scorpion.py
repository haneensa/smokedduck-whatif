import json
import os
import numpy as np
import time
import csv
import argparse
import pandas as pd


try:
    import smokedduck
except Exception as e:
    pass



from functools import wraps
from collections import *
from datetime import datetime
from flask import Flask, request, render_template, g, redirect, Response, jsonify
from flask_compress import Compress
from flask_cors import CORS, cross_origin



tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
print(tmpl_dir)
app = Flask(__name__, template_folder=tmpl_dir)
CORS(app, supports_credentials=True)


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
def scorpion():
  try:
    data =  json.loads(str(request.form['json']))
    requestid = request.form.get('requestid')
    print("running scorpion")

    sql = data['sql']
    badselection = data['badselection']
    goodselection = data['goodselection']
    badalias = next(iter(badselection))
    goodalias = next(iter(goodselection))
    badids = [d['id'] for d in badselection[badalias]]
    goodids = [d['id'] for d in goodselection[goodalias]]

    ret = runfade(sql, 1, goodids, badids)
    print(ret)

    return jsonify(ret)
  except Exception as e:
      print(e)
      print(sql)
      print(aggid)
      print(goodids)
      print(badids)
      results = [
          dict(score=0.1, clauses=["voltage < 0.1"]),
          dict(score=0.2, clauses=["moteid = 18"])
      ]
      return dict(
          status="final",
          results=results
      )




def runfade(sql, aggid, goodids, badids, query_id=None):


    con = smokedduck.connect('intel.db')
    clear(con)

    allids = goodids + badids
    goodids = list(map(str, goodids ))
    badids = list(map(str, badids ))
    avggood = f"({'+'.join(goodids)})/{len(goodids)}"
    avgbad = f"({'+'.join(badids)})/{len(badids)}"
    fade_q = f"""select {avggood} as avggood, {avgbad} as avgbad 
    from duckdb_fade() order by abs({avggood}-{avgbad})"""

    specs = [
        "readings.moteid",
        "readings.voltage",
        "readings.light",
        "readings.moteid|readings.voltage"
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
2
    q = f"pragma WhatIfSparse({query_id}, {aggid}, {allids}, '{specs[0]}', false);"
    res = con.execute(q).fetchdf()
    print(res)

    faderesults = con.execute(fade_q).fetchdf()

    results = []
    for i in range(10):
        g,b = faderesults['avggood'][i], faderesults['avgbad'][i]
        score = abs(g-b)
        q = f"pragma GetPredicate({i});"
        predicate = con.execute(q).fetchdf().iloc[0,0]
        clauses = [p.strip() for p in predicate.split("AND")]
        results.append(dict(score=score, clauses=clauses))


    clear(con)

    return dict(
        status="final",
        results=results
    )

    

if __name__ == "__main__":


  app.run(host="localhost", port="8111", debug=True, threaded=True)


