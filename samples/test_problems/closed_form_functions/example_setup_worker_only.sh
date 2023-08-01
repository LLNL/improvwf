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

mkdir ../../../sample_output/
mkdir ../../../sample_output/remote_master
cp sample_history.yaml ../../../sample_output/remote_master/global_history.yaml

# For each worker:
workerid=0
upperlim=$((${3:-0}))
#nparameters=$((${4:-0}))

while [ $workerid -le $upperlim ]  # If the workerid <= the argument, or 1 if no argument:
do
    mkdir ../../../sample_output/worker_${workerid}_file_system/

    # Make the source directory on the master
    mkdir ../../../sample_output/remote_master/worker_${workerid}_source/
    mkdir ../../../sample_output/remote_master/worker_${workerid}_source/decision_study_files
    mkdir ../../../sample_output/remote_master/worker_${workerid}_source/studies
    mkdir ../../../sample_output/remote_master/worker_${workerid}_source/studies/study_files

    # Copy over the necessary files
    cp ${2:-'../../nontrivial_decision_makers/random_forest_eps_greedy.py'} ../../../sample_output/remote_master/worker_${workerid}_source/decision_study_files/python_decision_maker.py
    cp ../../sample_worker/decision_maker.yaml ../../../sample_output/remote_master/worker_${workerid}_source/decision_study.yaml
    cp closed_form_function_menu.yaml ../../../sample_output/remote_master/worker_${workerid}_source/menu.yaml
    cp closed_form_function_query.yaml ../../../sample_output/remote_master/worker_${workerid}_source/studies/
    cp ${1:-'currin.py'} ../../../sample_output/remote_master/worker_${workerid}_source/studies/study_files/closed_form_function.py

    # Edit the experimental study
    param_sedstring="s/\\$\((X)\)/\\$\(X0\) \\$\(X1\)/"   # It turns out to be really important to choose "" or '' appropriately here.
    if [[ $OSTYPE == darwin* ]]; then
        sed -i "" "${param_sedstring}" ../../../sample_output/remote_master/worker_${workerid}_source/studies/closed_form_function_query.yaml
    else
        sed -i "${param_sedstring}" ../../../sample_output/remote_master/worker_${workerid}_source/studies/closed_form_function_query.yaml
    fi

    # Edit the decision-making study
    #sed -i "" "s/pop_in_random_order.py/${2:-pop_in_random_order.py}/" ../../../sample_output/remote_master/worker_${workerid}_source/decision_study.yaml
    if [[ $OSTYPE == darwin* ]]; then
        sed -i "" "s/pop_in_random_order.py/python_decision_maker.py/" ../../../sample_output/remote_master/worker_${workerid}_source/decision_study.yaml
    else
        sed -i "s/pop_in_random_order.py/python_decision_maker.py/" ../../../sample_output/remote_master/worker_${workerid}_source/decision_study.yaml
    fi

    ((workerid++))
done



# Relative paths work now;
# the worker's file system root is specified relative to
# the current directory, as are the global history file, inbox,
# outbox, and decision-maker. Absolute paths work for all cases.
# Any of the examples below can be used for demonstration purposes.

# # Go to the root directory of the improv daemon's file system:
# cd ../../../sample_output/worker_0_file_system/
# improv run ./ ../remote_master/worker_0_source -H ../remote_master/global_history.yaml -s 1 -m="-s 1" &

# # OR INSTEAD:
cd ../../../sample_output/
improv run worker_0_file_system remote_master/worker_0_source -H remote_master/global_history.yaml -s 1 -m="-s 1" &
