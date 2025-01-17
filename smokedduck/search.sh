#!/bin/bash

# deletion, dense, sparse

export DUCKDB_LIB_PATH=/ProvEnhance/third_party/smokedduck-whatif/build/release/src

# forward vs backward as we vary threads

# Q1. chunked vs not : forward lineage, vary threads, vary n, scalar vs vec, single agg vs many (evaluated n at a time)
query_nums=("1" "3" "5" "7" "9"  "10" "12")
sf_values=("1") #"5" "10") #"1" "5" "10") # "10")  # "0.2" "0.4") # "5.0" "10.0") # (# "3.0" "4.0")
distinct=("0") # "64" "256" "512" "1024" "2048")
threads_num=("1" "2" "4" "8")
prune_binary=("true"  "false")
# TODO: need to pad interventions to the next power of 2
is_scalar_binary=("true") #  "false") 
csv="test.csv" 
debug="false"
itype="SEARCH"
spec='lineitem.l_shipmode|lineitem.l_returnflag'
batch_list=("4") 
use_duckdb="false"
use_gb_bw_lineage_list=("false")
use_attrs="--use_attrs ${spec}"
touch ${csv}
echo sf,qid,itype,prob,incremental,use_duckdb,is_scalar,prune,num_threads,distinct,batch,post_time,gen_time,prep_time,compile_time,eval_time,prune_time,lineage_time,ksemimodule_timing,spec,lineage_count,lineage_count_prune,lineage_size_mb,lineage_size_mb_prune,use_gb_backward_lineage,code_gen_time,data_time > ${csv}
prob="0.1"

for sf in "${sf_values[@]}"
do
  echo "start"
  rm db.out
  python3 smokedduck/prep_db.py --sf ${sf} ${gen} ${use_attrs}
  for n in "${distinct[@]}"
  do
    for is_scalar in "${is_scalar_binary[@]}"
    do
      if [ "$is_scalar" = "false" ] && [ "$n" -eq 1 ]; then
        echo "skip because SIMD is set and n_interventions == 1"
        continue
      fi
        
      
      for thread in "${threads_num[@]}"
      do
        # for use_duckdb in "${binary[@]}" do
        for prune in "${prune_binary[@]}"
        do
            for query_num in "${query_nums[@]}"
            do
              for batch in "${batch_list[@]}"
              do
                for use_gb_bw_lineage in "${use_gb_bw_lineage_list[@]}"
                do
                  python3 smokedduck/test_whatif.py  --spec ${spec} --batch ${batch} --prune ${prune} --sf ${sf} --csv ${csv} --i ${query_num} --use-gb-bw-lineage ${use_gb_bw_lineage} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --interventions ${n} --itype ${itype} --prob ${prob} --incremental "false"
                  if [ "$is_scalar" = "false" ]; then
                    echo "skip because itype==SEARCH is set and SIMD is set"
                    continue
                  fi
                  python3 smokedduck/test_whatif.py  --spec ${spec} --batch ${batch} --prune ${prune} --sf ${sf} --csv ${csv} --i ${query_num} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --interventions ${n} --itype ${itype} --prob ${prob} --incremental "true"
                done
              done # batch
            done # query_num
        # done # use_duckdb
        done # prune
      done # is_scalar
    done # thread
  done # n
done # s
