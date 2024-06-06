rm intel.db
python3 smokedduck/intel_prep.py --specs "intel.voltage|intel.moteid|intel.light"
python3 smokedduck/whatif_demo.py --specs "intel.voltage|intel.moteid|intel.light" --sql "select hr, count(), sum(temp), avg(temp), stddev(temp) from intel group by hr" --aggid 0 --groupid 335
#python3 smokedduck/whatif_demo.py --specs "intel.moteid" --sql "select hr, count(), sum(temp), avg(temp), stddev(temp) from intel group by hr" --aggid 0
