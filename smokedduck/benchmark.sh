#!/bin/bash

# Single Scaling: distinct=1, is_scalar=true, itype=scale_random
# Batched Scaling: distinct > 1, itype=scale_random
# Scaling & Deletion: ?

export DUCKDB_LIB_PATH=/ProvEnhance/third_party/smokedduck-whatif/build/release/src

query_nums=("1" "3" "5" "7" "9"  "10" "12")
sf_values=("5" "10")  # "0.2" "0.4") # "5.0" "10.0") # (# "3.0" "4.0")
# ADD 64, 256
distinct=("1" "64" "256" "512" "1024" "2048" "2560")
threads_num=("8") #"1" "2" "4" "8")
prune_binary=("true") # "false")
is_scalar_binary=("false") #"true" "false")
csv="dense_delete_sf5_10_april10.csv"
debug="false"
#itype_list=("SCALE_RANDOM") # evaluate scaling some tuples of an attribute
itype_list=("DENSE_DELETE")
#itype_list=("SEARCH")
# if search then include incremental or not
prob_list=("0.1") #"0.001" "0.002" "0.005" "0.01" "0.02" "0.05" "0.1" "0.2" "0.3" "0.4" "0.5")
spec="lineitem.i"
spec='""'
batch="4"
use_duckdb="true"
touch ${csv}
# add prob, itype, incremental
echo sf,qid,itype,prob,incremental,use_duckdb,is_scalar,prune,num_threads,distinct,batch,post_time,gen_time,prep_time,compile_time,eval_time,prune_time,lineage_time,ksemimodule_timing,spec > ${csv}

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
  echo "start"
  rm db.out
  python3 smokedduck/prep_db.py --sf ${sf}
  for itype in "${itype_list[@]}"
  do
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
          #if [ "$itype" = "SEARCH" ] && [ "$thread" -ne 1 ]; then
          #  echo "skip because itype==SEARCH is set and n_threads > 1"
          #  continue
          #fi
          
          # for use_duckdb in "${binary[@]}" do
          for prune in "${prune_binary[@]}"
          do
              for query_num in "${query_nums[@]}"
              do
                for prob in "${prob_list[@]}"
                do
                  #if [ "$prob" != "0.1" ] && [ "$n" -ne 1 ]; then
                  #  echo "skip because n <> 1 and prob <> 0.1"
                  #  continue
                  # fi
                  
                  python3 smokedduck/test_whatif.py  --spec ${spec} --batch ${batch} --prune ${prune} --sf ${sf} --csv ${csv} --i ${query_num} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --interventions ${n} --itype ${itype} --prob ${prob} --incremental "false"
                  if [ "$itype" = "SEARCH" ] || { [ "$thread" -eq 1 ] && [ "$n" -eq 1 ] && [ "$use_duckdb" = "false" ]; } ; then
                    if [ "$itype" = "SEARCH" ] && [ "$is_scalar" = "false" ]; then
                      echo "skip because itype==SEARCH is set and SIMD is set"
                      continue
                    fi

                    #python3 smokedduck/test_whatif.py  --spec ${spec} --batch ${batch} --prune ${prune} --sf ${sf} --csv ${csv} --i ${query_num} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --interventions ${n} --itype ${itype} --prob ${prob} --incremental "true"
                  fi
                done
              done # query_num
          # done # use_duckdb
          done # prune
        done # is_scalar
      done # thread
    done # n
  done # itype
done # sf
