################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################
"""A utility module for working with the wrapper style workflow."""
import yaml
from improvwf.utils import yaml_safe_load_with_lock


def fill_parameters(args):
    """"Takes a maestro parameters.yaml file and outputs a global.parameters block.

        Example:

            INPUT: parameters.yaml:

                ? ANTIGEN_CHAINS_IN_STRUCT.1.ANTIGEN_FASTA_HASH.1.ANTIGEN_FASTA_PATH.1.MASTER_ANTIGEN_FASTA_HASH.1.MASTER_ANTIGEN_FASTA_PATH.1.STRUCTURE_HASH.1.STRUCTURE_PATH.1
                : labels: !!python/object/apply:collections.OrderedDict
                - - - $(ANTIGEN_CHAINS_IN_STRUCT.label)
                    - ANTIGEN_CHAINS_IN_STRUCT.1
                    - - $(ANTIGEN_FASTA_HASH.label)
                    - ANTIGEN_FASTA_HASH.1
                    - - $(ANTIGEN_FASTA_PATH.label)
                    - ANTIGEN_FASTA_PATH.1
                    - - $(MASTER_ANTIGEN_FASTA_HASH.label)
                    - MASTER_ANTIGEN_FASTA_HASH.1
                    - - $(MASTER_ANTIGEN_FASTA_PATH.label)
                    - MASTER_ANTIGEN_FASTA_PATH.1
                    - - $(STRUCTURE_HASH.label)
                    - STRUCTURE_HASH.1
                    - - $(STRUCTURE_PATH.label)
                    - STRUCTURE_PATH.1
                params:
                    $(ANTIGEN_CHAINS_IN_STRUCT): A
                    $(ANTIGEN_FASTA_HASH):
                    - 975e07240d0a1274b0745aacbe09b8b1
                    - MD5
                    $(ANTIGEN_FASTA_PATH): /home/example_user/worker_1_0_file_system/improv/decision_maker/decision_study_files/mutants_request__786177.fasta
                    $(MASTER_ANTIGEN_FASTA_HASH):
                    - bb28711c0539f208d37d615e5504a6fb
                    - MD5
                    $(MASTER_ANTIGEN_FASTA_PATH): /home/example_user/remote_master/study_files/RBD_FAB.fasta
                    $(STRUCTURE_HASH):
                    - a2d2a120f8da30616ba2e281c7143aa5
                    - MD5_ALL
                    $(STRUCTURE_PATH): /home/example_user/remote_master/study_files/RBD_FABminimized.pdb.final.pdb

            OUTPUT: generated_parameters.yaml:

                global.parameters:
                    ANTIGEN_CHAINS_IN_STRUCT:
                        values: [A]
                        label: [ANTIGEN_CHAINS_IN_STRUCT.1]
                    ANTIGEN_FASTA_HASH:
                        values:
                        - ['975e07240d0a1274b0745aacbe09b8b1', 'MD5']
                        label: [ANTIGEN_FASTA_HASH.1]
                    ANTIGEN_FASTA_PATH:
                        values: [/home/example_user/worker_1_0_file_system/improv/decision_maker/decision_study_files/mutants_request__786177.fasta]
                        label: [ANTIGEN_FASTA_PATH.1]
                    MASTER_ANTIGEN_FASTA_HASH:
                        values:
                        - ['bb28711c0539f208d37d615e5504a6fb', 'MD5']
                        label: [MASTER_ANTIGEN_FASTA_HASH.1]
                    MASTER_ANTIGEN_FASTA_PATH:
                        values: [/home/example_user/remote_master/study_files/RBD_FAB.fasta]
                        label: [MASTER_ANTIGEN_FASTA_PATH.1]
                    STRUCTURE_HASH:
                        values:
                        - ['a2d2a120f8da30616ba2e281c7143aa5', 'MD5_ALL']
                        label: [STRUCTURE_HASH.1]
                    STRUCTURE_PATH:
                        values: [/home/example_user/remote_master/study_files/RBD_FABminimized.pdb.final.pdb]
                        label: [STRUCTURE_PATH.1]
        """

    labels = []
    vals = {}
    data = {}

    # Open the file (invoked from within the wrapper inner study - the file is 1 dir above).
    with open('../meta/parameters.yaml', 'r') as f:
        data = yaml.load(f, Loader=yaml.Loader)

    for key, values in data.items():
            # Gather the labels of each parameter.
            for i in range(len(list(values.items())[0][1].items())):
                label = list(values.items())[0][1]
                label = list(label.items())[i][1]
                labels.append(label)

            # Gather the list of values and their names.
            for i in range(len(list(values.items())[0][1].items())):
                val = list(values.items())[1][1]
                val = list(val.items())[i][1]
                name = list(values.items())[1][1]
                name = list(name.items())[i][0]

                # Strip the '$()' out of the names so they can be used as a key.
                name = name.replace('$(', '').replace(')', '')
                if type(val) is list:
                    val = (
                        """
        - [{}]
        """.format(str(val).replace('[', '').replace(']', '')).rstrip()
                    )
                else:
                    val = '[' + str(val) + ']'
                vals[name] = val

    triplets = []
    triplets.append("global.parameters:")

    # Generate the triplets for the global.parameters these are:
    # NAME:
    #   values:
    #   label:
    for i in range(len(vals)):
        triplet = (
            """
    {}:
        values: {}
        label: [{}]
    """.format(labels[i].replace('.1', ''), vals[labels[i].replace('.1', '')], labels[i])).rstrip()

        triplets.append(triplet)

    global_parameters_block = ''.join(triplets)

    print(global_parameters_block)

    with open("generated_parameters.yaml", "a") as f:
        f.truncate(0)
        f.write(global_parameters_block)
