# Improv Dynamic Workflows (ImprovWF)

## Summary

A Python package for dynamic workflows, supplementing Maestro
([MaestroWF](https://github.com/LLNL/maestrowf)).

A workflow is a series of dependent computations which are executed to
yield an experimental result, much like sheet music describes a musical
performance. _Dynamic_ workflows are like improvisational jazz, in which
the musicians create unique, situation-driven, collaborative variations
on themes. Dynamic workflows can potentially yield better results, save
computation, and/or save human decision-making effort versus pre-specified
or manual workflows, as they need not run the exhaustive set of studies
which might be required in a pre-specified set, and they make their own
experimental design decisions at run time. If you have an existing
Maestro study where you would like to save computation and/or human time,
Improv is for you.


## Core capabilities
Improv provides the following core capabilities for dynamic decision-making:

- A worker daemon (invoked by improv run) that alternates between
    running experiments via standard Maestro studies and making
    decisions via specialized, decision-making Maestro studies.

- History tracking: The improv daemon records in-progress and finished
    studies run via Improv in a global history file (shared between
    simultaneously executed daemons) and a local (private) history file.
    The status of each such study is included, along with the results of
    finished studies.

- An automatic setup routine (invoked by improv prepare) that
    creates standardized files and folders in support of the
    distributed workers.

In addition, Improv provides aids for getting set up with your particular workflow:

- Templates for hierarchical and distributed workflows with local
    decision-making, using Improv's prepare and run functions with
    shared history to enable distributed decision-making toward a
    common goal.  These include a master daemon, implemented via a
    Maestro study, that executes the setup and submission processes.

- Templates for decision-maker studies (.yaml) and functions (python),
    including modular models and decision rules. Together, the model
    and decision rule are used to make a decision about what maestro
    study to run next, and with what parameters. The decision-maker
    studies read the shared history, call the model and decision rule
    to select from the available studies and parameters given the
    history information, create the resulting requested Maestro study
    specifications (.yaml files) and place these study requests in the
    inbox of Improv's run daemon.


## Background: Maestro

Improv adds to the capabilities of the core Maestro system, so it is important to
understand how Maestro works. In short:

- A specification, typically provided in the form of a .yaml file,
    gives a series of job steps to be executed.  Each step has a "cmd"
    field, which gives a series of command-line steps to be executed
    as a shell script.

- Maestro parses and expands (i.e., for multiple values
    of parameters) this specification into an "ExecutionGraph." The
    ExecutionGraph must be a static, directed, acyclic graph of job step
    dependencies, and is stored as a Python pickle file.

- Maestro creates a daemon, called the "conductor," which oversees the
    execution of the ExecutionGraph, periodically waking, checking the
    status of each job step, launching those that are ready,
    performing some other maintenance operations, and sleeping again.
    When all steps of the study are resolved, the conductor is shut
    down.


## Improv: Extending Maestro

Improv works to extend the Maestro process. First, the Improv prepare
and run sub-commands can be used with a "master" maestro to set up and
run distributed Improv daemons. The distributed improv run daemons then
launch a series of Maestro studies and store the results, where these
studies are selected online via decision-making Maestro studies.

Relative to the base Maestro setup, Improv provides the critical
"plumbing" necessary to make online decisions about what computations
to perform next; Maestro has no intrinsic capabilities to alter its own
ExecutionGraph (and consequently what subsequent study steps it will
perform) after ExecutionGraph construction.


## Basic Usage

Improv is designed for the following hierarchical setup. Note that an
improv run daemon could also be run as a solo experimenter, either
locally or on an allocation, rather than operating in a distributed
ensemble of multiple improv daemons. The hierarchical procedure is as
follows.

A light-weight, master Maestro process is invoked (e.g., on a login node)
by:
```
    maestro run <improv_master_study.yaml>
```
In pseudocode, the master study .yaml file describes a "study" of the
following form:
```
    for p in parameter_sets:
        improv prepare p
    for p in parameter_sets:
        improv run p
        # The resulting daemons may be launched on heterogeneous allocations
        # by, e.g., SLURM.
```
Upon making their way through the queue of the scheduler (if
applicable) the spawned improv run daemons run this pseudocode:
```
    last_was_decision = False
    stop = False
    while not stop:
        log results of concluded studies to history
        for s in studies_in_inbox:
            maestro run <s>
        if any studies launched:
            last_was_decision=False

        if no studies running and not last_was_decision:
            last_was_decision=True
            maestro run <decision study>
            # The decision study will place studies
            # in the inbox, or else terminate the daemon
            # by declining to do so.
        elif no studies running:
            stop = True
```
where the maestro run invocations from within improv run are _local_ to the allocation.

By providing templates for the master maestro's study and the decision-making
studies, an interface for history read/writes, and the improv prepare and run
code, Improv enables a user who has an existing, parameterized maestro study
to select and execute various parameterizations online.  To do so, the user
must convert their existing Maestro study/studies into a menu (listing the
possible studies and parameter values which are allowed for each) and
parameter-less template studies. Further, the user must modify a supplied
decision-study template to create a decision-making .yaml study that reads the
history and submits new studies, and modify a template master study to set up
for the improv run daemons.


## Installation

In order to install Improv, run the following command

```
pip install improvwf
```

or, from within the improv directory,

```
pip install .
```

You can also install Improv within a virtual environment. Virtual environments
(virtualenvs) can be activated and deactivated with the following commands
(note that '(venv)' indicates that the virtual environment is active):
```
    # Activate:
    $ source <path_to_activate>
    # Deactivate:
    (venv)$ deactivate
```
Activate your virtualenv and install using `pip` as specified above.

Installation time varies based on internet speed and dependency caching/availability,
but typically takes 1-2 minutes on a typical desktop computer. 


## Quick Start & Demo

Change directory to the `improvwf/samples` directory. Within are several
demonstrations of the improv code. Each will write files to the `improvwf/sample_output`
directory; _this will need to be deleted between runs for clean examples._
If run again without cleaning (run `cleanup_sample_outputs.sh`), the sample will prompt you for permission to clean.

Change directory to `improvwf/samples/sample_hierarchical/` and run `sh example_setup.sh`.
This will launch a local, hierarchical Improv run, with a central Maestro conductor
launching and monitoring several Improv daemons, each of which conducts its own set of
experiments, reading and writing to the global history file accordingly; note that the
"live" `global_history.yaml` is maintained in the master conductor's history-initialize
directory, not in that conductor's root directory.

The specifications for each of the components of this hierarchical study can be inspected.
They are:

- the setup script `example_setup.sh`
- the master study template `hierarchical_template.yaml`
- the menus `lulesh_small`, `_medium`, and `_large.yaml`
- the studies `../sample_worker/studies/`
- the simple decision-making study `../sample_worker/decision_maker.yaml`
- the Python script `../sample_worker/pop_in_random_order.py`

Further examples, including with a more interesting model and decision-maker, are within the  `<improv_root>/samples directory`; see especially
`samples/test_problems/closed_form_functions/` and the examples therein.

The basic demo will take around 5 minutes to complete on a typical desktop computer.
Its status can be checked using maestro:
```
    $ cd improvwf/sample_output/remote_master/maestro_path
    $ cd <generated folder within, hierarchical_sample1_<sometimestamp> >
    $ maestro status .
```

Once complete, the `sample_output` directory will be populated with results. Within should be a number of worker filesystems, each containing
the outputs of a series of LULESH runs:
```
    # Any of the worker filesystems can be accessed the same way 
    $ cd  worker_1_file_system/workspace/
    # View a list of runs handled by this worker
    $ ls
    # View the results of one of the runs
    $ cat <generated folder within, lulesh_sample_<type>_<timestamp> >/results.yaml
```
Each .yaml should contain a property value corresponding to the `type`.

The dataset for this run consists of demo parameters from [LLNL's LULESH](https://github.com/LLNL/LULESH) project.
These are specified in the menus and master study template.


## Writing your own studies
### Experimental studies
Improv (and Maestro) are constructed with the following scenario in mind; first, that
the user has a series of inter-connected experimental steps which must be executed to produce
an experimental "result"; second, that these steps are described by one or more "scripts",
which could be written in any language, but can be invoked from the command line by one means
or another.

The first step is to convert such a script into a series of command line steps which constitute a
Maestro study. The LULESH examples in the Maestro package are nice demonstrations of a somewhat
complex case of this, demonstrating how Maestro studies can include such handy features as fetching
git dependencies, compiling executables, and handing results from step to step within the workflow.
A thoroughly commented example is given in Maestro's `samples/documentation/lulesh_sample1_unix_commented.yaml`.

In the simplest Improv scenario, it is enough to select the set of parameters that are fed into a Maestro study;
Improv's LULESH demos take this route. Improv supports the next elaboration, into a multi-study system, that
selects not only which parameters should be used, but what type of study should be run; the improv LULESH demos

In more complex scenarios, a Maestro study might be broken up into multiple components, some or
all of which might have adaptive flow control, implemented in an Improv decision-making study.
One possibility is to have a decision-maker answering the question of "is this simulated result
sufficiently precise or accurate?" If not, Improv could launch additional experimental studies which would
contribute to refining the "final" result.  This is particularly suitable for Monte-Carlo-like
settings where additional calculations amount to "sampling" and "sufficiently precise or accurate"
can be expressed in terms of a concept like "is this set of inputs likely to be interesting/optimal?"
Improv's contribution in this setting would be to reduce computational load by more narrowly targeting
computation to inputs which are likely to be fruitful.

### Decision-makers:
Decision-makers are also implemented as Maestro-compatible studies. They are edited (within the initialization
in improv run) to incorporate the file paths they need (the inbox, the location of any files required for
decision-making, the history, etc.), the names of which are all prefixed in the yaml study's environment section
("env") with "IMPROV_". A template decision-making study is provided in `samples/sample_worker/decision_maker.yaml`,
and a template Python routine to actually select and write the new study is provided in
`samples/sample_worker/template_decisionmaking_routine.py`. The study is relatively simple; it simply provides
the interface to Maestro that is needed for study execution. Much more elaborate decision-making studies are possible.

The Python decision-making script is just one option for the core of the decision-making study, but one that
is well-provided for by Improv's utility routines (`improvwf/utils.py`), which handle reading the history
and menu, composing the selected study from the study template and the selected parameters, and writing
the study to a yaml file in the inbox.  See the template for examples.

### Database support:
The global_history.yaml mechanism does not scale; an LC/HPC-compatible database is need to provide scalibility. Improv
includes MySQL compatible database support through the open-source [Sina library](https://github.com/LLNL/Sina).

`db_and_yaml.py` is a command-line tool to load/dump YAML-based studies to/from the MySQL database. It is located at:
`<improv_root>/improvwf/db_interface/db_and_yaml.py`

### Antibody and Antigen Sequencing:
There is Antibody/Antigen sequence specific functionality in parts of Improv.  This functionality relies on a
study history format similar to the sample history file `<improvwf_root>/samples/sample_antibody/sample_FAB_record.yaml`.

A sqlite database (file) can be created from this history record using the `db_and_yaml.py` 'load' command:
```
python ./improvwf/db_interface/db_and_yaml.py load -b ./sqlite_db.sqlite -H ./samples/sample_antibody/sample_FAB_record.yaml
```

### Unit Tests:
The <improv_root>/tests directory contains pytest-compatible unit tests that exercise the database code and
antibody/antigen functionality. With pytest installed, run these tests from <improv_root>:
```
pytest -s ./tests/
```

## System Requirements
# Software Dependencies
Improv requires Python 3 (>=3.9 recommended) and access to pip. Further dependencies (and versions if not newest)
are detailed in, and acquired through, the setup.py file; running the install command (`pip install .`) will fetch
them automatically.

# Operating Systems
Linux: Improv has been written and tested primarily on TOSS3 and TOSS4, based on RHEL (Red Hat Enterprise Linux).
Most recent testing has been done on 8.9.

macOS: Tested on macOS Monterey (12.7). in case of issues installing mysql on macOS, you can comment that dependency out of setup.py,
as it is not required for basic improv usage (though it is recommended for running at scale).

# Hardware Requirements
No non-standard hardware is required to run Improv. It can be run at demo scales on a typical desktop computer. 


## Authors and Contributors
- Thomas Desautels, LLNL
- Rebecca Haluska, LLNL
- John Goforth, LLNL
- Denis Vashchenko, LLNL


## License

Improv is distributed under the MIT license. Contributions must be made under this license. 

More information can be found in the `LICENSE` and `NOTICE` files.

LLNL-CODE-853340
