#!/bin/bash

query_nums=("1" "3" "5" "7" "9" "10" "12")
sf_values=("1" "5") # # "5" "10")  # "0.2" "0.4") # "5.0" "10.0") # (# "3.0" "4.0")
distinct=("1024") #"64" "256" "512" "1024" "2048")
threads_num=("1" "2" "4" "8")
binary=("true" "false")
csv="dense_feb7.csv"
debug="true"
touch ${csv}
echo sf,qid,use_duckdb,is_scalar,num_threads,distinct,batch,post_time,gen_time,prep_time,compile_time,eval_time > ${csv}

for sf in "${sf_values[@]}"
do
  rm db.out
  python3 smokedduck/prep_db.py --sf ${sf}
  for n in "${distinct[@]}"
  do
    for thread in "${threads_num[@]}"
    do
      for is_scalar in "${binary[@]}"
      do
        # for use_duckdb in "${binary[@]}" do
            use_duckdb="true"
            for query_num in "${query_nums[@]}"
            do
              python3 smokedduck/test_whatif.py  --sf ${sf} --csv ${csv} --i ${query_num} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug}
            done # query_num
        # done # use_duckdb
      done # is_scalar
    done # thread
  done # n
done # sf
