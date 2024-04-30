#!/bin/bash

export DUCKDB_LIB_PATH=/ProvEnhance/third_party/smokedduck-whatif/build/release/src

query_nums=("1" "3" "5" "7" "9" "10" "12")
sf_values=("1" "5" "10")
distinct=("2048") # "1024" "2048" "2560")
threads_num=("1" "2" "4" "8")
csv="cube_april27.csv"
touch ${csv}
# add prob, itype, incremental
echo sf,qid,num_threads,distinct,timing > ${csv}

for sf in "${sf_values[@]}"
do
  rm db.out
  python3 smokedduck/prep_db.py --sf ${sf} --gen_distinct True
  for n in "${distinct[@]}"
  do
    for thread in "${threads_num[@]}"
    do
      for query_num in "${query_nums[@]}"
      do
        python3 smokedduck/test_whatif_cube.py  --sf ${sf} --csv ${csv} --i ${query_num} --t ${thread} --interventions ${n}
      done
    done
  done
done
