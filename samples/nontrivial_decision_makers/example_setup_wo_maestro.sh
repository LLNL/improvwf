#!/bin/bash
################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

# Clean directories if they already exist
if [ -d ../../sample_output ]; then
  read -p "../../sample_output folder already exists--good to delete it? [yn] " candelete
  echo ""
    if [ "$candelete" = "y" ]; then
      rm -rf ../../sample_output
    else
      echo "Not deleting folder, quitting"
      exit 0
    fi
fi

# ### Set up and fill directories ###
mkdir ../../sample_output/
mkdir ../../sample_output/remote_master
cp ../sample_worker/lulesh_sample_history.yaml ../../sample_output/remote_master/global_history.yaml

# For each worker:
workerid=0
upperlim=$((${1:-0}))
while [ $workerid -le $upperlim ]
do
    # Make the source directory on the master
    mkdir ../../sample_output/remote_master/worker_${workerid}_source/
    mkdir ../../sample_output/remote_master/worker_${workerid}_source/decision_study_files
    mkdir ../../sample_output/remote_master/worker_${workerid}_source/studies

    # Copy over the decision-making files
    cp random_forest_eps_greedy.py ../../sample_output/remote_master/worker_${workerid}_source/decision_study_files/random_forest_eps_greedy.py
    cp decision_maker.yaml ../../sample_output/remote_master/worker_${workerid}_source/decision_study.yaml
    cp ../sample_worker/lulesh_menu.yaml ../../sample_output/remote_master/worker_${workerid}_source/menu.yaml
    cp ../sample_worker/studies/*.yaml ../../sample_output/remote_master/worker_${workerid}_source/studies/

    ((workerid++))
done

echo "Release versions of random_forest_eps_greedy.py have input values"
echo "configured for currin.py, not lulesh, and will consequently make"
echo "random selections; modify lines 99-102 as required."

# ### Invoke improv run ###
cd ../../sample_output/
workerid=0
while [ $workerid -le $upperlim ]
do
    mkdir worker_${workerid}_file_system/
    improv run worker_${workerid}_file_system remote_master/worker_${workerid}_source -H remote_master/global_history.yaml -s 5 -m="-s 1" &
    ((workerid++))
done
