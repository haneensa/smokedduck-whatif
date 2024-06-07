rm intel.db
python3 smokedduck/intel_prep.py  --specs "readings.voltage|readings.moteid|readings.light"
python3 smokedduck/scorpion_local.py
#python3 smokedduck/whatif_demo.py --specs "intel.voltage|intel.moteid|intel.light" --sql "select hr, count(), sum(temp), avg(temp), stddev(temp) from intel group by hr" --aggid 0  --groups 20 10 1 30
#python3 smokedduck/whatif_demo.py --specs "intel.moteid" --sql "select hr, count(), sum(temp), avg(temp), stddev(temp) from intel group by hr" --groups 0
