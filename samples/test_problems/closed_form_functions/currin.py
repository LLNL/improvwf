################################################################################
# Copyright (c) 2018-2023, Lawrence Livermore National Security, LLC
# See the top-level LICENSE and NOTICE files for details.
#
# SPDX-License-Identifier: MIT
################################################################################

"""An implementation of the Currin closed-form, multi-fidelity test function."""
import math
import yaml
import sys
import os


# Currin function (MF version) and cost function:
def currinfunc(x, z):
    """
    Compute the Currin function, given x and z

    Copied from Kandasamy 2017 (ArXiv), with the exception that z is now
    flipped; z = 0 is the nominal problem, and z in (0, 1] is a lower-fidelity
    surrogate.
    :param x: 2-element list of numbers, both in [0, 1]; the spatial
        location of the query
    :param z: 1-element list of numbers or a single float, in [0, 1]; the
        fidelity, with 0 being the "true" but more expensive value and 1
        being the lowest-fidelity value.
    :return: a float, giving the Currin function's value.
    """
    if not isinstance(z, list):
        z = [z]

    # leadterm = 1.0 - 0.1 * (1.0 - z) * math.exp(-1.0 /(2.0 * x[1]))
    leadterm = 1.0 - (0.1 * z[0] * math.exp(-1.0 / (2.0 * x[1])))
    numerator   = (2300.0 * x[0]**3 + 1900.0 * x[0]**2 + 2092.0 * x[0] + 60.0)
    denominator = ( 100.0 * x[0]**3 +  500.0 * x[0]**2 +    4.0 * x[0] + 20.0)
    return leadterm * numerator / denominator


def currincost(x, z):
    """
    Compute the cost of the Currin function evaluation requested.
    :param x: 2-element list of numbers, both in [0, 1]; the spatial
        location of the query
    :param z: 1-element list of numbers or a single float, in [0, 1]; the
        fidelity, with 0 being the "true" but more expensive value and 1
        being the lowest-fidelity value.
    :return: a float, giving the Currin function's cost.
    """
    if not isinstance(z, list):
        z = [z]
    return 1.0 - z[0]  # flipped from Kandasamy 2017 to match above


def main(x=None, z=None, path=None):
    """
    Evaluate the currin function and its cost, and write the values to the
    specified .yaml file
    :param x: 2-element list of numbers in [0, 1]; the spatial
        locations of the query
    :param z: 1-element list of numbers or a single number in [0, 1];
        the fidelity, with 0 being the "true" but more expensive value and 1
        being the lowest-fidelity value.
    :param path: string giving path to desired output yaml file; defaults to
        <current directory>/results.yaml.
    """
    if path is None:
        path = os.path.join(os.getcwd(), "results.yaml")

    cost = currincost(x, z)
    value = currinfunc(x, z)

    with open(path, "w") as f:
        yaml.safe_dump({"value": value, "cost": cost}, f)


if __name__ == "__main__":
    # print(sys.argv)
    argdict = {"x": sys.argv[1:-2], "z": sys.argv[-2], "path": sys.argv[-1]}

    # Process args to make sure they are numbers and not strings:
    if isinstance(argdict["x"], str):
        argdict["x"] = [float(xi_str) for xi_str in argdict["x"].split(" ")]
    elif isinstance(argdict["x"], list):
        argdict["x"] = [float(xi_str) for xi_str in argdict["x"]]

    if isinstance(argdict["z"], str):
        argdict["z"] = [float(zi_str) for zi_str in argdict["z"].split(" ")]

    # print(argdict)

    main(**argdict)
