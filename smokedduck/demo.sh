rm intel.db
python3 smokedduck/intel_prep.py --specs "intel.moteid|intel.temp|intel.voltage"
python3 smokedduck/whatif_demo.py --specs "intel.moteid" --sql "select hr, count(), sum(temp), avg(temp), stddev(temp) from intel where hr <'2004-02-28 01:00:00' group by hr" --aggid 3 --groupid 0
#python3 smokedduck/whatif_demo.py --specs "intel.moteid" --sql "select hr, count(), sum(temp), avg(temp), stddev(temp) from intel group by hr" --aggid 0
