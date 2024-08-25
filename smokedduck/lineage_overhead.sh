#!/bin/bash

sf_values=("1" "5" "10")
models=("lineage" "lineageAll" "ksemimodule")
workload=("tpch") # "nsf" "flights")
# add prob, itype, incremental
#lineage_csv="fade_data/lineage_overhead_all_april30_v3.csv"
lineage_csv="fade_data/lineage_overhead_all_tpch.csv"
touch ${lineage_csv}
echo qid,sf,model,workload,query_timing,lineage_timing > ${lineage_csv}
for w in "${workload[@]}"
do
  if [ "$w" = "nsf" ]; then
    rm build/db/nsf_data_keyed.db
    python3 ../nsf/parse_data.py
  fi
  for m in "${models[@]}"
  do
    if [ "$w" = "tpch" ]; then
      for sf in "${sf_values[@]}"
      do
      python3 smokedduck/eval_query.py --sf ${sf} --csv ${lineage_csv} --model ${m} --workload ${w}
      done
    elif [ "$w" = "nsf" ]; then
      python3 smokedduck/eval_query.py --sf 0 --csv ${lineage_csv} --model ${m} --workload ${w}
    else 
      python3 smokedduck/eval_query.py --sf 0 --csv ${lineage_csv} --model ${m} --workload ${w}
    fi
  done
done
