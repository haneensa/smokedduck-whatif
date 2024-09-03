#!/bin/bash

# Single Scaling: distinct=1, is_scalar=true, itype=scale_random
# Batched Scaling: distinct > 1, itype=scale_random

export DUCKDB_LIB_PATH=/ProvEnhance/third_party/smokedduck-whatif/build/release/src

# forward vs backward as we vary threads

# Q1. chunked vs not : forward lineage, vary threads, vary n, scalar vs vec, single agg vs many (evaluated n at a time)
groups=(8 16 32 64 1000 10000) #2 4 8 10 100 1000)
cards=(10000000) #100000 1000000 10000000)
distinct=(1024) #64 256 512 1024 2048)
alpha_list=(1 0)
aggs=(1) # 2 4 8)
threads_num=(1 2 4 8)
prune="true"
is_scalar_binary=("true" "false")
csv="forward_vs_backward_jul11_iter.csv"
debug="false"
itype="DENSE_DELETE"
prob_list=("0.1") #"0.001"  "0.002" "0.005" "0.01" "0.02" "0.05" "0.1" "0.2" "0.3" "0.4" "0.5")
spec='""'
batch_list=(1)
use_gb_bw_lineage_list=("true" "false")
touch ${csv}
# add prob, itype, incremental
echo group,card,alpha,naggs,itype,prob,incremental,use_duckdb,is_scalar,prune,num_threads,distinct,batch,post_time,gen_time,prep_time,compile_time,eval_time,prune_time,lineage_time,ksemimodule_timing,spec,lineage_count,lineage_count_prune,lineage_size_mb,lineage_size_mb_prune,use_gb_backward_lineage > ${csv}

for c in "${cards[@]}"
do
  for g in "${groups[@]}"
  do
    for a in "${alpha_list[@]}"
    do
      for naggs in "${aggs[@]}"
      do
        echo "start"
        rm db_${g}_${c}_${a}.0_${naggs}.out
        python3 smokedduck/prep_micro.py --group ${g} --card ${c} --alpha ${a} --naggs ${naggs}

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
              for batch in "${batch_list[@]}"
              do
                  for prob in "${prob_list[@]}"
                  do
                    for use_gb_bw_lineage in "${use_gb_bw_lineage_list[@]}"
                    do
                      python3 smokedduck/test_whatif_micro.py --batch ${batch} --prune ${prune} --csv ${csv}  --use-gb-bw-lineage ${use_gb_bw_lineage} --t ${thread} --is-scalar ${is_scalar} --debug ${debug} --interventions ${n} --itype ${itype} --prob ${prob} --group ${g} --card ${c} --alpha ${a} --naggs ${naggs}
                    done
                  done
              done
            done
          done
        done

      done
    done
  done
done
