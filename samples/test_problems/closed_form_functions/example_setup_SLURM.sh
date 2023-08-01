#!/bin/bash
################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

# ### Input handling ###
filename=hierarchical_slurm.yaml

# Get machine-specific SLURM settings
hostname=${3:-'hostname'}
bank=${4:-'bank_name'}
queue=${5:-'queue_name'}

# ### Make directories and prepare files ###

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

mkdir ../../../sample_output
mkdir ../../../sample_output/remote_master

cp ${filename} ../../../sample_output/remote_master/master.yaml
cp closed_form_function_menu.yaml ../../../sample_output/remote_master/input_menu.yaml
cp ../../sample_worker/decision_maker.yaml ../../../sample_output/remote_master/decision_study.yaml
cp closed_form_function_query.yaml ../../../sample_output/remote_master/   # Not in a studies sub-directory
cp ${1:-'currin.py'} ../../../sample_output/remote_master/closed_form_function.py
cp ${2:-'../../nontrivial_decision_makers/random_forest_eps_greedy.py'} ../../../sample_output/remote_master/python_decision_maker.py
cp sample_history.yaml ../../../sample_output/remote_master/global_history.yaml



# ### Edit the experimental study, decision-making, and master studies ###
# At present, hard-code the parameters for the Currin (2D) case:
# Note that conditional switching on ${1:-'currin.py'} could be used to set this to a variety of choices.
param_sedstring="s/\\$\((X)\)/\\$\(X0\) \\$\(X1\)/"   # It turns out to be really important to choose "" or '' appropriately here.
if [[ $OSTYPE == darwin* ]]; then
    # Edit the experimental study
    sed -i "" "${param_sedstring}" ../../../sample_output/remote_master/closed_form_function_query.yaml
    # Edit the decision-making study
    sed -i "" "s/pop_in_random_order.py/python_decision_maker.py/" ../../../sample_output/remote_master/decision_study.yaml
    # Edit the master study:
    sed -i "" -E "s/queue:[ a-zA-Z0-9\._]+$/queue: ${queue}/" ../../../sample_output/remote_master/master.yaml
    sed -i "" -E "s/bank:[ a-zA-Z0-9\._]+$/bank: ${bank}/" ../../../sample_output/remote_master/master.yaml
    sed -i "" -E "s/host:[ a-zA-Z0-9\._]+$/host: ${hostname}/" ../../../sample_output/remote_master/master.yaml
else
    sed -i "${param_sedstring}" ../../../sample_output/remote_master/closed_form_function_query.yaml

    sed -i "s/pop_in_random_order.py/python_decision_maker.py/" ../../../sample_output/remote_master/decision_study.yaml

    sed -i -E "s/queue:[ a-zA-Z0-9\._]+$/queue: ${queue}/"  ../../../sample_output/remote_master/master.yaml
    sed -i -E "s/bank:[ a-zA-Z0-9\._]+$/bank: ${bank}/" ../../../sample_output/remote_master/master.yaml
    sed -i -E "s/host:[ a-zA-Z0-9\._]+$/host: ${hostname}/" ../../../sample_output/remote_master/master.yaml
fi



# ### Launch the hierarchical daemons ###
cd ../../../sample_output/remote_master
maestro -d 1 run master.yaml -s 1 # Invoke maestro on the hierarchical_template with a 1-second sleeptime and all logging enabled
