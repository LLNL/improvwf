#!/bin/bash
################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

# An example of how improv prepare and run can be used locally without
# requiring a top-level maestro.

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

# Prepare the "master" directory; no master maestro will be created, but it will contain the
# files at invocation time.
mkdir ../../sample_output
mkdir ../../sample_output/remote_master

# Provide the worker's menu, decision study, decision-maker routine, studies, and global history file.
cp ../sample_worker/lulesh_menu.yaml ../../sample_output/remote_master
cp ../sample_worker/decision_maker.yaml ../../sample_output/remote_master
cp ../sample_worker/pop_in_random_order.py ../../sample_output/remote_master
cp -R ../sample_worker/studies ../../sample_output/remote_master  # Syntax is important on this one
cp ../sample_worker/lulesh_sample_history.yaml ../../sample_output/remote_master/global_history.yaml

# Prepare the worker's source directory
cd ../../sample_output/remote_master
improv prepare decision_maker.yaml lulesh_menu.yaml ./studies  -i 0 -o . -Df="pop_in_random_order.py"

# Make the worker's private directory and direct it to pull from its source directory
mkdir ../worker_0_file_system
cd ../
improv run worker_0_file_system remote_master/source -H remote_master/global_history.yaml -s 5 -m="-s 1" &
