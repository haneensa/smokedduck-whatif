#!/bin/bash

# deletion, dense, sparse

export DUCKDB_LIB_PATH=/ProvEnhance/third_party/smokedduck-whatif/build/release/src

# forward vs backward as we vary threads

# Q1. chunked vs not : forward lineage, vary threads, vary n, scalar vs vec, single agg vs many (evaluated n at a time)
query_nums=("19") #"6" "8" "19" "14") #"1" "3" "5" "7" "9"  "10" "12" "6" "8" "19") # "14")
sf_values=("10" "5" "1") # "5" "10") # "5" "10") #"1" "5" "10") # "10")  # "0.2" "0.4") # "5.0" "10.0") # (# "3.0" "4.0")
# ADD 64, 256
distinct=("1" "64" "256" "512" "1024" "2048")
threads_num=("1"  "2" "4" "8")
prune_binary=("true") #  "false")
is_scalar_binary=("true"  "false") 
csv="fade_simd_test.csv" 
csv="fade_simd_test_scalarJoinFilter_sf5.csv" 
csv="fade_scalar_extra.csv"
csv="fade_all_a21.csv"
csv="fade_all_a22_scalarJoinFilter.csv"
csv="fade_all_a25_spec_19_test.csv"
debug="false"
itype_list=("DENSE_SPEC" "SEARCH") #"DENSE_DELETE") # "DENSE_SPEC") # "SCALE_RANDOM") #"SCALE_RANDOM" "DENSE_DELETE" "SEARCH" "DENSE_SPEC")
spec='""'
batch_list=("4") 
use_duckdb="false"
use_gb_bw_lineage_list=("false")
iters=2
touch ${csv}
echo iter,sf,qid,itype,prob,incremental,use_duckdb,is_scalar,prune,num_threads,distinct,batch,post_time,gen_time,prep_time,compile_time,eval_time,prune_time,lineage_time,ksemimodule_timing,spec,lineage_count,lineage_count_prune,lineage_size_mb,lineage_size_mb_prune,use_gb_backward_lineage,code_gen_time,data_time > ${csv}

for sf in "${sf_values[@]}"
do
  echo "start"
  rm db.out
  python3 smokedduck/prep_db.py --sf ${sf} ${gen}
  for itype in "${itype_list[@]}"
  do
    if [ "$itype" = "SEARCH" ] ; then
      spec='lineitem.i'
    elif [ "$itype" = "DENSE_SPEC" ] ; then
      itype="DENSE_DELETE"
      spec='lineitem.i'
    else
      spec='""'
    fi
    for n in "${distinct[@]}"
    do
      if [ "$n" -eq 1 ] && [ "$itype" = "DENSE_DELETE" ] && [ "$spec" = '""' ] ; then
        prob_list=("0.001" "0.002" "0.005" "0.01" "0.02" "0.05" "0.1" "0.2" "0.3" "0.4" "0.5")
      else
        prob_list=("0.1")
      fi
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
                    for prob in "${prob_list[@]}"
                    do
                      for iter in $(seq 1 $iters)
                      do
                        python3 smokedduck/test_whatif.py  --iter ${iter} --spec ${spec} --batch ${batch} --prune ${prune} --sf ${sf} --csv ${csv} --i ${query_num} --use-gb-bw-lineage ${use_gb_bw_lineage} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --interventions ${n} --itype ${itype} --prob ${prob} --incremental "false"
                        if [ "$itype" = "SEARCH" ] ; then
                          if [ "$itype" = "SEARCH" ] && [ "$is_scalar" = "false" ]; then
                            echo "skip because itype==SEARCH is set and SIMD is set"
                            continue
                          fi
                          python3 smokedduck/test_whatif.py  --iter ${iter} --spec ${spec} --batch ${batch} --prune ${prune} --sf ${sf} --csv ${csv} --i ${query_num} --use-duckdb ${use_duckdb} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --interventions ${n} --itype ${itype} --prob ${prob} --incremental "true"
                        fi
                      done # iter
                    done # prob
                  done
                done # batch
              done # query_num
          # done # use_duckdb
          done # prune
        done # is_scalar
      done # thread
    done # n
  done # itype
done # s
