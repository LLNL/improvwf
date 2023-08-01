#!/bin/bash
################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

# An example of how maestro run can be used to run a hierarchical-
# structured improv study.

# ### Setup ###
# Create the files the user is intended to supply: the master study, menus,
# decision study, decision-maker routine, experimental studies, and base history file.

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

# # Make directories
# Remember to clean up the old ../../sample_output directory
if [[ -e '../../sample_output' ]]; then
  echo '../../sample_output must not exist! Please delete it.'
  exit 1
fi

mkdir ../../sample_output
mkdir ../../sample_output/remote_master

# # Copy files
# Master study: (includes database_url value and improv run invocation with -b flag)
cp hierarchical_template_database.yaml ../../sample_output/remote_master/master.yaml

# Menus:
cp ../sample_hierarchical/lulesh_small.yaml ../../sample_output/remote_master
cp ../sample_hierarchical/lulesh_medium.yaml ../../sample_output/remote_master
cp ../sample_hierarchical/lulesh_large.yaml ../../sample_output/remote_master
# Decision study and decision-maker routine:
cp ../sample_worker/decision_maker.yaml ../../sample_output/remote_master
cp ../sample_worker/pop_in_random_order.py ../../sample_output/remote_master
# Edit these
if [[ $OSTYPE == darwin* ]]; then
  sed -i "" -E 's/-y \$\(IMPROV_HISTORY\)/-b \$\(IMPROV_DATABASE\)/' ../../sample_output/remote_master/decision_maker.yaml
  improv_root_sedstring=$(echo $(greadlink -f ../../) | sed 's/\//\\\//g')
  sed -i "" -E "s/IMPROV_GITREP_ROOT_FILL_IN/ ${improv_root_sedstring}/" ../../sample_output/remote_master/master.yaml
  sed -i "" -E 's/(database_url: +)0.0.0.0/\1$(SPECROOT)\/sqlite_db.sqlite/' ../../sample_output/remote_master/master.yaml
else
  sed -i -E 's/-y \$\(IMPROV_HISTORY\)/-b \$\(IMPROV_DATABASE\)/' ../../sample_output/remote_master/decision_maker.yaml
  improv_root_sedstring=$(echo $(readlink -f ../../) | sed 's/\//\\\//g')
  sed -i -E "s/IMPROV_GITREP_ROOT_FILL_IN/ ${improv_root_sedstring}/" ../../sample_output/remote_master/master.yaml
  sed -i -E 's/(database_url: +)0.0.0.0/\1$(SPECROOT)\/sqlite_db.sqlite/' ../../sample_output/remote_master/master.yaml
fi

# Studies:
cp -R ../sample_worker/studies ../../sample_output/remote_master  # Syntax is important on this one
# Base history:
cp ../sample_worker/lulesh_sample_history.yaml ../../sample_output/remote_master/global_history.yaml


# ### Launch the whole study via the master study ###
cd ../../sample_output/remote_master
# maestro run master.yaml -s 1 # Invoke maestro on the master study with a 1-second sleeptime

# When complete, you can dump from the database to yaml for inspection:
# python <path>/db_and_yaml.py dump -b remote_master/sqlite_db.sqlite -H ./dumpcheck.yaml
# And clean up after yourself by removing the sample_output directory.
