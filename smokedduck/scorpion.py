import pdb
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
    print("couldn't load smokedduck")
    pass




from functools import wraps
from collections import *
from datetime import datetime
from flask import Flask, request, render_template, g, redirect, Response, jsonify
from flask_compress import Compress
from flask_cors import CORS, cross_origin

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)


tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
print(tmpl_dir)
app = Flask(__name__, template_folder=tmpl_dir)
CORS(app)#, supports_credentials=True)


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
    if request.method == "GET":
      data =  json.loads(str(request.args['json']))
    else:
      data =  json.loads(str(request.form['json']))

    print(data)
    print("running scorpion")

    sql = data['sql']
    badselection = data['badselection']
    goodselection = data['goodselection']
    badalias = next(iter(badselection))
    goodalias = next(iter(goodselection))
    badids = [d['id'] for d in badselection[badalias]]
    goodids = [d['id'] for d in goodselection[goodalias]]
    print(sql)
    print(goodids)
    print(badids)

    with smokedduck.connect('intel.db') as con:
        ret = runscorpion(con, sql, 0, goodids, badids)
    print(ret)

  except Exception as e:
      print(e)
      ret = dict(
              status="final",
              results=[
          dict(score=0.1, clauses=["voltage < 0.1"]),
          dict(score=0.2, clauses=["moteid = 18"])
      ])

  response = json.dumps(ret, cls=NumpyEncoder)
  return response, 200, {'Content-Type': 'application/json'}



def runscorpion(con, sql, aggid, goodids, badids, query_id=None):
    clear(con)
    allids = goodids + badids

    if (query_id is None):
        start = time.time()
        out = con.execute(sql, capture_lineage='lineageAll').df()
        end = time.time()
        query_timing = end - start
        query_id = con.query_id
        goodvals = out.loc[goodids, 'agg'+str(aggid)]
        badvals = out.loc[badids, 'agg'+str(aggid)]
        print(out)
        print(out.loc[goodids])
        print(out.loc[badids])


    mg, mb = np.mean(goodvals), np.mean(badvals)
    goodids = [f"g{id}" for id in goodids]
    badids = [f"g{id}" for id in badids]

    ges = [f"abs({id}-{val})" for id, val in zip(goodids, goodvals)]
    maxgood = f"greatest({','.join(ges)})"

    print("badvals", badvals)
    bes = [f"coalesce(({val}-{id}),0)" for id, val in zip(badids, badvals)]
    maxbad = f"greatest({','.join(bes)})"
    minbad = f"least({','.join(bes)})"
    avgbad = f"({'+'.join(bes)})/{len(badids)}::float"
    fade_q = f"""WITH tmp AS (
        SELECT 
        pid,
        {avgbad} as avgbad, 
        {maxgood} as maxgood, {minbad} as minbad,{maxbad} as maxbad,
        {len(badids)} as nb  FROM duckdb_fade()
    )
    SELECT *, avgbad-maxgood as score
    FROM tmp
    ORDER BY score desc
    LIMIT 10"""

    specs = [
        "readings.moteid",
<<<<<<< HEAD
        "readings.voltage",
        "readings.light",
        "readings.moteid|readings.voltage",
        "readings.moteid|readings.light",
        "readings.voltage|readings.light",
        #"readings.moteid|readings.light|readings.voltage",
    ]


    con.execute(f"pragma PrepareLineage({query_id}, false, false, false)")
=======
        #"readings.voltage",
        #"readings.light",
        #"readings.moteid|readings.voltage",
        #"readings.moteid|readings.light",
        #"readings.voltage|readings.light",
        #"readings.moteid|readings.light|readings.voltage",
    ]

    use_gb_backward_lineage = 'true'
    con.execute(f"pragma PrepareLineage({query_id}, false, false, {use_gb_backward_lineage})")
>>>>>>> whatif-refactored
    results = []
    for spec in specs:
        results.extend(run_fade(con, query_id, aggid, allids, spec, fade_q))
    results.sort(key=lambda d: d['score'], reverse=True)
    print(results)
    return dict(
        status="final",
        results=results[:5]
    )

def run_fade(con, query_id, aggid, allids, spec, fade_q):
    print(fade_q)
    print(f"Run fade with spec {spec}")
    results = []
    try: 
        q = f"pragma WhatIfSparse({query_id}, {aggid}, {allids}, '{spec}', false);"
        print(q)
        con.execute(q).fetchdf()
        #print(con.execute("select * from duckdb_fade()").df())
        faderesults = con.execute(fade_q).fetchdf()
        print(faderesults)

        for i in range(10):
            score = float(faderesults['score'][i])
            pid = faderesults['pid'][i]
            q = f"pragma GetPredicate({pid});"
            predicate = con.execute(q).fetchdf().iloc[0,0]
            predicate = predicate.replace("_", ".")
            clauses = [p.strip() for p in predicate.split("AND")]
            results.append(dict(score=score, clauses=clauses))
        return results
    except Exception as e:
        print("exception")
        print(e)
        return []

    

if __name__ == "__main__":


  app.run(host="0.0.0.0", port="8111", debug=True, threaded=True)
