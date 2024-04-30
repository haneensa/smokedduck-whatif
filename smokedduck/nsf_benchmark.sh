#!/bin/bash

export DUCKDB_LIB_PATH=/ProvEnhance/third_party/smokedduck-whatif/build/release/src

sf_values=("1")
threads_num=("8") # "2" "4" "8")
is_scalar_binary=("true") # "false") 
#duckdb_binary=("false" "true")
prune_binary=("false") #"true") #"true") # "false")
csv="nsf_test.csv"
debug="false"
mat=0
batch="4"
use_duckdb="false"
touch ${csv}
# add prob, itype, incremental
echo sf,qid,itype,prob,incremental,use_duckdb,is_scalar,prune,num_threads,distinct,batch,post_time,gen_time,prep_time,compile_time,eval_time,prune_time,lineage_time,ksemimodule_timing,spec,lineage_count,lineage_count_prune,lineage_size_mb,lineage_size_mb_prune,use_gb_backward_lineage,code_gen_time,data_time > ${csv}

capture_lineage_overhead="false"
if [ "$capture_lineage_overhead" = "true" ]; then
  lineage_csv="fade_data/lineage_overhead.csv"
  touch ${lineage_csv}
  echo qid,sf,query_timing,lineage_timing > ${lineage_csv}
  for sf in "${sf_values[@]}"
  do
    python3 smokedduck/eval_query.py --sf ${sf} --csv ${lineage_csv}
  done
fi

for sf in "${sf_values[@]}"
do
  #rm build/db/nsf_data_keyed.db
  #python3 ../nsf/parse_data.py
  for is_scalar in "${is_scalar_binary[@]}"
  do
    for thread in "${threads_num[@]}"
    do
      for prune in "${prune_binary[@]}"
      do
          python3 smokedduck/nsf_whatif.py  --batch ${batch} --prune ${prune} --sf ${sf} --csv ${csv} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --mat ${mat}
      done # prune
    done # is_scalar
  done # thread
done # sf
