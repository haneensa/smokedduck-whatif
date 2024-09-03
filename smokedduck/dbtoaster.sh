#!/bin/bash
#root=build/toast_no_prune_scale_delete_april30 #delete_lineitem
prefix=build/toast_may1
#rm -dr ${prefix}*
query_nums=("01" "03" "05" "07" "09" "10" "12")
sf="1.0"
prob_list=("0.001"  "0.002" "0.005" "0.01" "0.02" "0.05" "0.1" "0.2" "0.3" "0.4" "0.5")
# prepare for db
rm -f build/db/*
rm -fr build/data/*
python3.9 -m prov_eval.prepare --scale-factor ${sf}  --database-out build/db/tpch_scale_${sf}.db --query-db "TPCH"
itypes=("SCALE" "DELETE")
prune_binary=("false") #"true"  "false") # "true")

for prune in ${prune_binary[@]}
do
  if [ "$prune" == "true" ]; then
  python3.9 -m scripts.runner.dbt_why --scale-factor ${sf}
  prefix2=${prefix}_prune
  else
  prefix2=${prefix}_no_prune
  python3.9 -m scripts.runner.tpch_regular --scale-factor ${sf}
  fi
  for itype in ${itypes[@]}
  do
    root=${prefix2}_${itype}
    rm -rf ${root}
    mkdir ${root}
    if [ "$itype" == "DELETE" ]; then
      scale_lineitem="false"
      delete_lineitem="false"
      do_delete="true"
    else
      scale_lineitem="true"
      delete_lineitem="true"
      do_delete="false"
    fi

    for q in "${query_nums[@]}"
    do
      # generate base_sql + driver_defines
      if [ "$prune" == "true" ]; then
        echo "prune folder"
        data_out="build/data/tpch_scale_${sf}/dbt_tpch_${q}/"
      else
        data_out="build/data/regular/dbt/tpch_scale_${sf}/"
      fi
      python3 -m baselines.dbtoaster.loader --data-out ${data_out} --dbt-query-file queries/tpch/dbtoaster/tpch_${q}.sql --define-out build/query/dbt_q${q}.args --query-db TPCH --query-file queries/tpch/tpch_${q}.sql --query-name q1 --query-out build/query/dbt_q${q}.sql  --scale-factor ${sf}
      for prob in "${prob_list[@]}"
      do
        echo ${q} ${prob}
        # run dbtoaster-backend
        #PS: you need to install sbt first
        cd third_party/dbtoaster-backend; time_taken=$(time (sbt "toast -o ../../build/src/dbt_q${q}.cpp -l cpp --batch --preload --del -O3 ../../build/query/dbt_q${q}.sql"))
        cd ../..;
        
        # run
        time g++ -Ddel_prob=${prob} -Ddelete_lineitem=${delete_lineitem} -Dscale_lineitem=${scale_lineitem} -Ddo_delete=${do_delete} -std=c++14 -O3 -march=native baselines/dbtoaster/driver/main.cpp -o build/exe/tpch_q${q} -I third_party/dbtoaster-backend/ddbtoaster/srccpp/lib -I baselines/dbtoaster/driver -include build/src/dbt_q${q}.cpp @build/query/dbt_q${q}.args
        build/exe/tpch_q${q} --preload --save-timings --timings-out ${root}/experiment_one_${q}_${sf}_${prob}.out -b 10000000 --seed 2
      done # prob
    done # q
  done #itype
done # prune

