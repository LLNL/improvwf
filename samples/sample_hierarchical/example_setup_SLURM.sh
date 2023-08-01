#!/bin/bash
################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################


# Get machine-specific SLURM settings
hostname=${1:-'hostname'}
bank=${2:-'bank_name'}
queue=${3:-'queue_name'}

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

# ### Set up the files and directories:
mkdir ../../sample_output
mkdir ../../sample_output/remote_master
cp hierarchical_template.yaml ../../sample_output/remote_master/master.yaml
cp lulesh_small.yaml ../../sample_output/remote_master
cp lulesh_medium.yaml ../../sample_output/remote_master
cp lulesh_large.yaml ../../sample_output/remote_master
cp ../sample_worker/decision_maker.yaml ../../sample_output/remote_master
cp -R ../sample_worker/studies ../../sample_output/remote_master  # Syntax is important on this one
cp ../sample_worker/pop_in_random_order.py ../../sample_output/remote_master
cp ../sample_worker/lulesh_sample_history.yaml ../../sample_output/remote_master/global_history.yaml

if [[ $OSTYPE == darwin* ]]; then
    # # Edit the master study:
    # First, put in the queue, bank, and host information
    sed -i "" -E "s/queue:[ a-zA-Z0-9\._]+$/queue: ${queue}/" ../../sample_output/remote_master/master.yaml
    sed -i "" -E "s/bank:[ a-zA-Z0-9\._]+$/bank: ${bank}/" ../../sample_output/remote_master/master.yaml
    sed -i "" -E "s/host:[ a-zA-Z0-9\._]+$/host: ${hostname}/" ../../sample_output/remote_master/master.yaml
    # Second, uncomment the batch, nodes, and walltime information in the spec;
    sed -i "" "s/^#batch:/batch:/" ../../sample_output/remote_master/master.yaml
    sed -i "" "s/^#    type:/    type:/" ../../sample_output/remote_master/master.yaml
    sed -i "" "s/^#    queue:/    queue:/" ../../sample_output/remote_master/master.yaml
    sed -i "" "s/^#    bank:/    bank:/" ../../sample_output/remote_master/master.yaml
    sed -i "" "s/^#    host:/    host:/" ../../sample_output/remote_master/master.yaml
    sed -i "" "s/^#    nodes:/    nodes:/" ../../sample_output/remote_master/master.yaml
    sed -i "" "s/^#      nodes :/      nodes :/" ../../sample_output/remote_master/master.yaml
    sed -i "" "s/^#      walltime :/      walltime :/" ../../sample_output/remote_master/master.yaml

else
    sed -i -E "s/queue:[ a-zA-Z0-9\._]+$/queue: ${queue}/"  ../../sample_output/remote_master/master.yaml
    sed -i -E "s/bank:[ a-zA-Z0-9\._]+$/bank: ${bank}/" ../../sample_output/remote_master/master.yaml
    sed -i -E "s/host:[ a-zA-Z0-9\._]+$/host: ${hostname}/" ../../sample_output/remote_master/master.yaml
    sed -i "s/^#batch:/batch:/" ../../sample_output/remote_master/master.yaml
    sed -i "s/^#    type:/    type:/" ../../sample_output/remote_master/master.yaml
    sed -i "s/^#    queue:/    queue:/" ../../sample_output/remote_master/master.yaml
    sed -i "s/^#    bank:/    bank:/" ../../sample_output/remote_master/master.yaml
    sed -i "s/^#    host:/    host:/" ../../sample_output/remote_master/master.yaml
    sed -i "s/^#    nodes:/    nodes:/" ../../sample_output/remote_master/master.yaml
    sed -i "s/^#      nodes :/      nodes :/" ../../sample_output/remote_master/master.yaml
    sed -i "s/^#      walltime :/      walltime :/" ../../sample_output/remote_master/master.yaml
fi

# ### Invoke the spec to launch the hierarchical setup.
# # Commenting the following commands out will let you inspect the standard files improv expects before improv prepare.
cd ../../sample_output/remote_master
# # After invoking maestro run, maestro prompts asks if you would like to launch the study; answering "n" will allow you
# # to inspect the master maestro's own setup. In that event, you will need to re-invoke maestro, as below.
maestro -d 1 run master.yaml -s 1 # Invoke maestro on the hierarchical_template with a 1-second sleeptime
