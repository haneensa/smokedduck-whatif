#!/bin/sh

SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

echo $SCRIPTPATH

rm -rf	$SCRIPTPATH/.eggs \
		$SCRIPTPATH/.pytest_cache \
		$SCRIPTPATH/build \
		$SCRIPTPATH/dist \
		$SCRIPTPATH/smokedduck.egg-info 
		$SCRIPTPATH/wheelhouse

python3 -m pip uninstall smokedduck --yes
