import duckdb
import re
import pandas as pd
import json
import os
import sys
import numpy as np

# Define a regular expression pattern to match filenames
pattern = r"(benchmark_|q)(\d+(?:\.\d+)?)"
#pattern = r"(benchmark|q)_?(\d+(?:\.\d+)?)_(no-pruned|pruned-lineage)?"
i = "0"

con = duckdb.connect()

def process_dbtoast():
    # no prune folder: build/toast
    # prune: build/toast_prune

    with open("build/toast_1.0.json", "r") as file:
        summary_prune= json.load(file)
    
    folder = "build/toast_may1"
    #pruned = ["", "_prune_probs_v2"]
    #pruned = ["_feb17_prune", "_feb13_no_prune"]
    #pruned = ["_mar19_prune"]
    #pruned = ["_april3_prune", "_april3_prune_scale", "_april3_no_prune"]
    pruned = ["_no_prune_DELETE", "_no_prune_SCALE", "_prune_DELETE", "_prune_SCALE"]
    data = []
    for prune_label in pruned:
        fname = f"{folder}{prune_label}/"
        for filename in os.listdir(fname):
            print(filename)

            with open(fname + filename, 'r') as file:
                for line in file:
                    line = line.strip()
                    print(line)
                    if "del_time" in line: 
                        del_time = int(line.split(' ')[1])/1000.0
                    elif "total_time" in line:
                        total_time = int(line.split(' ')[1])/1000.0
                    
                print(del_time, total_time)
                # parse filename
                tokens = filename[:-4].split("_")
                print(tokens)
                q = int(tokens[2])
                sf = float(tokens[3])
                p = float(tokens[4])
                print(summary_prune[str(q)])
                is_pruned = "prune"
                if "no_prune" in prune_label:
                    is_pruned = "no-prune"

                do_scale = False
                if "SCALE" in prune_label:
                    do_scale = True
                # TODO: detect scaling

                setup_time = summary_prune[str(q)]["prune_table"]*1000
                prune_time = summary_prune[str(q)]["prune"]*1000
                if is_pruned == "no-prune":
                    prune_bool = False
                    setup_time = 0
                    prune_time = 0
                else:
                    prune_bool = True
                    setup_time = summary_prune[str(q)]["prune_table"]*1000
                    prune_time = summary_prune[str(q)]["prune"]*1000
                itype = "DELETE"
                if do_scale:
                    itype = "SCALE"
                data.append({"eval_time": del_time, "sf":float(sf), "qid": q, "itype": itype, "prob": p, "incremental": True,
                    "use_duckdb": None, "is_scalar": None, "prune": prune_bool, "num_threads": -1,
                    "distinct": 1, "batch": 1, "post_time": 0, "gen_time": setup_time, "prep_time": total_time-del_time,
                    "compile_time": 0, "prune_time": prune_time})
                print(data[-1])
    return data

if __name__ == "__main__":
    dbt_data = process_dbtoast()
    # dbt pruned vs dbt full
    df_dbt = pd.DataFrame(dbt_data)
    print(df_dbt)
    df_dbt.to_csv("dbtoast_may1.csv", index=False)
