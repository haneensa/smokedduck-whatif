#!/bin/bash

export DUCKDB_LIB_PATH=/ProvEnhance/third_party/smokedduck-whatif/build/release/src

query_nums=("1" "3" "5" "7" "9" "10" "12")
sf_values=("1") # "5" "10")  # "0.2" "0.4") # "5.0" "10.0") # (# "3.0" "4.0")
distinct=("1024") #"512") # "1024" "2048" "2560")
threads_num=("2") # "2" "4" "8")
binary=("true" "false")
#binary=("false" # "false")
csv="dense_test.csv"
debug="false" #"true"
touch ${csv}
echo sf,qid,use_duckdb,is_scalar,num_threads,distinct,batch,post_time,gen_time,prep_time,compile_time,eval_time > ${csv}

for sf in "${sf_values[@]}"
do
  rm db.out
  python3 smokedduck/prep_db.py --sf ${sf}
  for n in "${distinct[@]}"
  do
    for is_scalar in "${binary[@]}"
    do
      # todo: add conditional, if n=1 and is_scalar is false then skip
      for thread in "${threads_num[@]}"
      do
        # for use_duckdb in "${binary[@]}" do
        for prune in "${binary[@]}"
        do
            use_duckdb="true"
            for query_num in "${query_nums[@]}"
            do
              python3 smokedduck/test_whatif.py  --prune ${prune} --sf ${sf} --csv ${csv} --i ${query_num} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --interventions ${n}
            done # query_num
        # done # use_duckdb
        done # prune
      done # is_scalar
    done # thread
  done # n
done # sf
