rm intel.db
python3 smokedduck/intel_prep.py  --specs "readings.voltage|readings.moteid|readings.light"
python3 smokedduck/scorpion_local.py
