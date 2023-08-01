#!/bin/bash
################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################


# ### Prepare directories and files for improv invocation ###
# This is really 'setting up' the example reproducibly

# Prepare the root directory for the example
# rm -rf ../../../sample_output
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
mkdir ../../../sample_output/remote_master/studies

# Copy over the source files:
cp sample_history.yaml ../../../sample_output/remote_master/global_history.yaml
cp closed_form_function_menu.yaml ../../../sample_output/remote_master/input_menu.yaml
cp ../../sample_worker/decision_maker.yaml ../../../sample_output/remote_master/decision_study.yaml
cp closed_form_function_query.yaml ../../../sample_output/remote_master/studies/
cp ${2:-'../../nontrivial_decision_makers/random_forest_eps_greedy.py'} ../../../sample_output/remote_master/python_decision_maker.py
cp ${1:-'currin.py'} ../../../sample_output/remote_master/closed_form_function.py

# At present, hard-code the parameters for the Currin (2D) case:
# Note that conditional switching on ${1:-'currin.py'} could be used to set this to a variety of choices.
param_sedstring="s/\\$\((X)\)/\\$\(X0\) \\$\(X1\)/"   # It turns out to be really important to choose "" or '' appropriately here.
if [[ $OSTYPE == darwin* ]]; then
    sed -i "" "${param_sedstring}" ../../../sample_output/remote_master/studies/closed_form_function_query.yaml
else
    sed -i "${param_sedstring}" ../../../sample_output/remote_master/studies/closed_form_function_query.yaml
fi

# Edit the decision-making study
if [[ $OSTYPE == darwin* ]]; then
    sed -i "" "s/pop_in_random_order.py/python_decision_maker.py/" ../../../sample_output/remote_master/decision_study.yaml
else
    sed -i "s/pop_in_random_order.py/python_decision_maker.py/" ../../../sample_output/remote_master/decision_study.yaml
fi

# ### Invoke Improv prepare and run ###
# Given a fresh directory with the necessary files, these are the steps
# which would be executed by a user.
cd ../../../sample_output/remote_master

# Improv prepare:
improv prepare decision_study.yaml input_menu.yaml ./studies -Sf="closed_form_function.py" -i 0 -o . -Df="python_decision_maker.py"

# Improv run:
cd ../  # Now in sample_output
mkdir worker_0_file_system
improv run worker_0_file_system remote_master/source -H remote_master/global_history.yaml -s 1 -m="-s 1" &
