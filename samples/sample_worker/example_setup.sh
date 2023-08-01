#!/bin/bash
################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

# ### Example setup ###

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

mkdir ../../sample_output/
mkdir ../../sample_output/remote_master
cp lulesh_sample_history.yaml ../../sample_output/remote_master/global_history.yaml

# For each worker:
workerid=0
upperlim=$((${1:-0}))
while [ $workerid -le $upperlim ]  # If the workerid <= the argument, or 1 if no argument:
do
    mkdir ../../sample_output/worker_${workerid}_file_system/

    # Make the source directory on the master
    mkdir ../../sample_output/remote_master/worker_${workerid}_source/
    mkdir ../../sample_output/remote_master/worker_${workerid}_source/decision_study_files
    mkdir ../../sample_output/remote_master/worker_${workerid}_source/studies

    # Copy the worker's files in to the worker_${workerid}_source/ directory. improv run will pull them from here.
    cp pop_in_random_order.py ../../sample_output/remote_master/worker_${workerid}_source/decision_study_files/pop_in_random_order.py
    cp decision_maker.yaml ../../sample_output/remote_master/worker_${workerid}_source/decision_study.yaml
    cp lulesh_menu.yaml ../../sample_output/remote_master/worker_${workerid}_source/menu.yaml
    cp studies/*.yaml ../../sample_output/remote_master/worker_${workerid}_source/studies/

    ((workerid++))
done



# ### Launch the workers via improv run ###

# # Go to the root directory of each improv daemon's file system: #
# cd ../../sample_output/worker_0_file_system/
# improv run ./ ../remote_master/worker_0_source -H ../remote_master/global_history.yaml -s 5 -m="-s 1" &

# # OR INSTEAD: #
# cd ../../sample_output/
# improv run worker_0_file_system remote_master/worker_0_source -H remote_master/global_history.yaml -s 5 -m="-s 1" &

# # Or to launch all of the workers: #
cd ../../sample_output/
workerid=0
while [ $workerid -le $upperlim ]
do
    improv run worker_${workerid}_file_system remote_master/worker_${workerid}_source -H remote_master/global_history.yaml -s 5 -m="-s 1" &
    sleep 5
    ((workerid++))
done
