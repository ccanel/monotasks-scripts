"""
This script parses event logs (that have already been copied to the local file system) to create
graphs of disk throughput during the write and read phases of a disk experiment.

The type of disk experiment that this file assumes the event logs correspond to is:
  org.apache.spark.monotasks.tests.disk.DiskExperiment

The directory structure in which the event log files are stored should be:

  log root directory      # input parameter
   |
   | - machine1           # machine name
   |   |
   |   | - 1              # experiment name
   |   |   |
   |   |   | - monotasks  # program name
   |   |   |   |
   |   |   |   | - machine1_1_monotasks_event_log
   |   |   |
   |   |   | - spark
   |   |       |
   |   |       | - machine1_1_spark_event_log
   |   |
   |   | - 2
   |   |   ...
   |   ...
   |
   | - machine2
   |   ...
   ...

* There can be any number of machines, named in any fashion.
* There can be any number of experiments, named in any fashion. Typically, the names of the
  experiment directories should be the number of concurrent tasks in that experiment.
* The "monotasks" program folder should only appear in one of the experiment folders, since it was
  only tested once.
"""

import collections
import os
import subprocess
import sys

import parse_event_logs

USAGE = "python build_concurrent_tasks_graphs_2.py " + \
        "<data root dir> " + \
        "<output dir> " + \
        "<base plot file for phase JCTs> " + \
        "<base plot file for total JCT> " + \
        "<base plot file for phase disk throughputs>" + \
        "<base plot file for phase disk utilizations>"

def main(argv):
  if (len(argv) != 6):
    print USAGE
    exit

  root_dir = argv[0]
  output_dir = argv[1]
  base_plot_filename_for_jcts = argv[2]
  base_plot_filename_for_total_jct = argv[3]
  base_plot_filename_for_throughputs = argv[4]
  base_plot_filename_for_utilizations = argv[5]

  event_logs = find_event_logs(root_dir)
  if (event_logs is None):
    print "Log directory structure is not properly formed!"
    return

  for (machine_name, results) in format_dictionary(event_logs).iteritems():
    (monotasks_results, spark_results_list) = results

    # Create graphs for each phase.
    for phase in {"write", "read"}:
      filter_function = filter_write_phase if (phase == "write") else filter_read_phase
      for _, spark_results in spark_results_list:
        parse_event_logs.Analyzer(spark_results, filter_function).output_utilizations(spark_results)

      parse_event_logs.Analyzer(
        monotasks_results, filter_function).output_utilizations(monotasks_results)

      generate_phase_jcts_graph(
        machine_name,
        phase,
        monotasks_results,
        spark_results_list,
        base_plot_filename_for_jcts,
        output_dir)
      generate_phase_throughputs_graph(
        machine_name,
        phase,
        monotasks_results,
        spark_results_list,
        base_plot_filename_for_throughputs,
        output_dir)
      generate_phase_utilizations_graph(
        machine_name,
        phase,
        monotasks_results,
        spark_results_list,
        base_plot_filename_for_utilizations,
        output_dir)

    # Create graphs of the total JCTs.
    for _, spark_results in spark_results_list:
      parse_event_logs.Analyzer(spark_results, lambda x: x).output_utilizations(spark_results)

    parse_event_logs.Analyzer(monotasks_results, lambda x: x).output_utilizations(monotasks_results)

    generate_total_jct_graph(
      machine_name,
      monotasks_results,
      spark_results_list,
      base_plot_filename_for_total_jct,
      output_dir)

  print "Done"


"""
Navigates the filesystem hierarchy described in the main file comment and creates the following
dictionary:
  { machine name -> ( monotasks_results , { experiment name -> spark_results } ) }

Returns None if the directory structure is not properly formed.
"""
def find_event_logs(root_dir):
  master_experiments = None
  machines = dict()

  for machine_name in os.listdir(root_dir):
    # For ever machine:
    machine_dir = os.path.join(root_dir, machine_name)
    monotasks_results = None

    if (os.path.isdir(machine_dir)):
      experiments = dict()

      for experiment_name in os.listdir(machine_dir):
        # For every experiment:
        experiment_dir = os.path.join(machine_dir, experiment_name)

        if (os.path.isdir(experiment_dir)):
          spark_results = None

          for program_name in os.listdir(experiment_dir):
            # For every program:
            program_dir = os.path.join(experiment_dir, program_name)

            if (os.path.isdir(program_dir)):
              results_path = os.path.join(
                program_dir, "%s_%s_%s_event_log" % (machine_name, experiment_name, program_name))

              if (program_name == "spark"):
                spark_results = results_path
              elif (program_name == "monotasks"):
                if (monotasks_results is None):
                  monotasks_results = results_path
                else:
                  print "Found additional results for monotasks ... ignoring them."
              else:
                print "Unrecognized program: %s" % program_name
                return None

          # Verify that each experiment directory contains results for Spark.
          if (spark_results is None):
            print "No spark results found for experiment %s" % experiment_name
            return None

          experiments[experiment_name] = spark_results

      # Verify that each machine directory contains the correct experiment directories.
      experiment_names = experiments.keys()
      if (master_experiments is None):
        master_experiments = experiment_names
      elif (collections.Counter(experiment_names) != collections.Counter(master_experiments)):
        print "Experiments are wrong! expected: %s found: %s " % \
          (master_experiments, experiment_names)
        return None

      machines[machine_name] = (monotasks_results, experiments)

  return machines


"""
Transforms the event_logs dictionary (whose format is described in the comment for the function
find_event_logs()) into this format:
  { machine name -> ( monotasks_results, List(spark_results for experiment 1, ...) ) }
"""
def format_dictionary(event_logs):
  machines = dict()

  for (machine_name, results) in event_logs.iteritems():
    (monotasks_results, experiments) = results
    spark_results_list = []

    # Loop over the experiments in sorted order.
    for experiment in sorted(map(lambda x: int(x), experiments.keys())):
      spark_results_list.append((experiment, experiments[str(experiment)]))

    machines[machine_name] = (monotasks_results, spark_results_list)

  return machines


"""
It is assumed that the proper event log analysis results files have already been generated.
"""
def generate_phase_jcts_graph(
    machine_name,
    phase,
    monotasks_results,
    spark_results_list,
    base_plot_filename,
    output_dir):
  if (phase not in {"write", "read"}):
    print "Unknown phase: %s" % phase
    exit

  # Construct the data file.
  prefix = "%s_%s-phase" % (machine_name, phase)
  jcts_data_filename = os.path.join(output_dir, "%s_jcts.data" % prefix)
  jcts_data_file = open(jcts_data_filename, 'w')

  i = 1

  # First, parse the Spark results.
  for (experiment, spark_results) in spark_results_list:
    spark_jcts_filename = "%s_jcts" % spark_results
    jcts_data_file.write(build_data_line(spark_jcts_filename, experiment, i, 1000))
    i += 1

  # Then, add the Monotasks results.
  monotasks_jcts_filename = "%s_jcts" % monotasks_results
  jcts_data_file.write(build_data_line(monotasks_jcts_filename, 0, i, 1000))
  jcts_data_file.close()

  # Construct the plot files.
  plot_filename = os.path.join(output_dir, "plot_%s_jcts.gp" % prefix)
  plot_file = open(plot_filename, 'w')
  graph_filename = os.path.join(output_dir, "%s_jcts.pdf" % prefix)

  y_max = 75 if (phase == "write") else 300

  for line in open(base_plot_filename, 'r'):
     new_line = line.replace("__JCT_DATA_FILENAME__", jcts_data_filename)
     new_line = new_line.replace("__OUTPUT_FILENAME__", graph_filename)
     new_line = new_line.replace("__Y_MAX__", str(y_max))
     plot_file.write(new_line)

  plot_file.close()

  # Generate the graph.
  subprocess.check_call("gnuplot %s" % plot_filename, shell=True)
  print "Generated graph of JCTs for machine: %s and phase: %s" % (machine_name, phase)


"""
It is assumed that the proper event log analysis results files have already been generated.
"""
def generate_total_jct_graph(
    machine_name,
    monotasks_results,
    spark_results_list,
    base_plot_filename,
    output_dir):
  # Construct the data files.
  prefix = "%s_%s" % (machine_name, "total")
  spark_total_jcts_data_filename = os.path.join(output_dir, "%s_jcts.data" % prefix)
  spark_total_jcts_data_file = open(spark_total_jcts_data_filename, 'w')

  # First, parse the Spark results.
  for _, spark_results in spark_results_list:
    spark_total_jct_filename = "%s_total_jct" % spark_results
    spark_total_jct_file = open(spark_total_jct_filename, 'r')
    spark_total_jcts_data_file.write(str(float(spark_total_jct_file.readline()) / 1000) + "\n")
    spark_total_jct_file.close()

  spark_total_jcts_data_file.close()

  # Then, parse the Monotasks results.
  monotasks_total_jct_filename = "%s_total_jct" % monotasks_results
  monotasks_total_jct_file = open(monotasks_total_jct_filename, 'r')
  monotasks_total_jct = float(monotasks_total_jct_file.readline()) / 1000
  monotasks_total_jct_file.close()
  monotasks_jct_description_position = "0.1,%s" % (monotasks_total_jct - 250)

  # Construct the plot files.
  plot_filename = os.path.join(output_dir, "plot_%s_jcts.gp" % prefix)
  plot_file = open(plot_filename, 'w')
  graph_filename = os.path.join(output_dir, "%s_jcts.pdf" % prefix)

  for line in open(base_plot_filename, 'r'):
     new_line = line.replace("__SPARK_JCT_DATA_FILENAME__", spark_total_jcts_data_filename)
     new_line = new_line.replace("__MONOTASKS_JCT__", str(int(monotasks_total_jct)))
     new_line = new_line.replace(
       "__MONOTASKS_JCT_DESCRIPTION_POSITION__", monotasks_jct_description_position)
     new_line = new_line.replace("__OUTPUT_FILENAME__", graph_filename)
     plot_file.write(new_line)

  plot_file.close()

  # Generate the graph.
  subprocess.check_call("gnuplot %s" % plot_filename, shell=True)
  print "Generated total JCT graph for machine: %s" % machine_name


"""
It is assumed that the proper event log analysis results files have already been generated.
"""
def generate_phase_throughputs_graph(
    machine_name,
    phase,
    monotasks_results,
    spark_results_list,
    base_plot_filename,
    output_dir):
  if (phase not in {"write", "read"}):
    print "Unknown phase: %s" % phase
    exit

  prefix = "%s_%s-phase" % (machine_name, phase)

  # Construct the data files.
  write_data_filename = os.path.join(output_dir, "%s_write_throughputs.data" % prefix)
  write_data_file = open(write_data_filename, 'w')
  read_data_filename = os.path.join(output_dir, "%s_read_throughputs.data" % prefix)
  read_data_file = open(read_data_filename, 'w')
  totals_data_filename = os.path.join(output_dir, "%s_total_throughputs.data" % prefix)
  totals_data_file = open(totals_data_filename, 'w')

  i = 1

  # First, parse the Spark results.
  for (experiment, spark_results) in spark_results_list:
    spark_write_throughputs_filename = "%s_disk_write_throughput" % spark_results
    write_data_file.write(build_data_line(spark_write_throughputs_filename, experiment, i, 1000000))

    spark_read_throughputs_filename = "%s_disk_read_throughput" % spark_results
    read_data_file.write(build_data_line(spark_read_throughputs_filename, experiment, i, 1000000))

    spark_total_throughputs_filename = "%s_total_disk_throughput" % spark_results
    totals_data_file.write(
      build_data_line(spark_total_throughputs_filename, experiment, i, 1000000))

    i += 1

  # Then, add the Monotasks throughputs.
  monotasks_write_throughputs_filename = "%s_disk_write_throughput" % monotasks_results
  write_data_file.write(
    build_data_line(monotasks_write_throughputs_filename, experiment, i, 1000000))

  monotasks_read_throughputs_filename = "%s_disk_read_throughput" % monotasks_results
  read_data_file.write(
    build_data_line(monotasks_read_throughputs_filename, experiment, i, 1000000))

  monotasks_total_throughputs_filename = "%s_total_disk_throughput" % monotasks_results
  totals_data_file.write(
    build_data_line(monotasks_total_throughputs_filename, experiment, i, 1000000))

  write_data_file.close()
  read_data_file.close()
  totals_data_file.close()

  # Construct the plot files.
  plot_filename = os.path.join(output_dir, "plot_%s_throughput.gp" % prefix)
  plot_file = open(plot_filename, 'w')
  graph_filename = os.path.join(output_dir, "%s_throughput.pdf" % prefix)

  for line in open(base_plot_filename, 'r'):
    new_line = line.replace("__WRITE_DATA_FILENAME__", write_data_filename)
    new_line = new_line.replace("__READ_DATA_FILENAME__", read_data_filename)
    new_line = new_line.replace("__TOTALS_DATA_FILENAME__", totals_data_filename)
    new_line = new_line.replace("__OUTPUT_FILENAME__", graph_filename)
    plot_file.write(new_line)

  plot_file.close()

  # Generate the graph.
  subprocess.check_call("gnuplot %s" % plot_filename, shell=True)
  print "Generated graph of throughputs for machine: %s and phase: %s" % (machine_name, phase)


"""
It is assumed that the proper event log analysis results files have already been generated.
"""
def generate_phase_utilizations_graph(
    machine_name,
    phase,
    monotasks_results,
    spark_results_list,
    base_plot_filename,
    output_dir):
  if (phase not in {"write", "read"}):
    print "Unknown phase: %s" % phase
    exit

  # Construct the data files.
  prefix = "%s_%s-phase" % (machine_name, phase)
  utilizations_data_filename = os.path.join(output_dir, "%s_disk_utilizations.data" % prefix)
  utilizations_data_file = open(utilizations_data_filename, 'w')

  i = 1

  # First, parse the Spark results.
  for (experiment, spark_results) in spark_results_list:
    spark_utilizations_filename = "%s_disk_utilization" % spark_results
    utilizations_data_file.write(build_data_line(spark_utilizations_filename, experiment, i, 1))

    i += 1

  # Then, add the Monotasks throughputs.
  monotasks_utilizations_filename = "%s_disk_utilization" % monotasks_results
  utilizations_data_file.write(build_data_line(monotasks_utilizations_filename, experiment, i, 1))

  utilizations_data_file.close()

  # Construct the plot files.
  plot_filename = os.path.join(output_dir, "plot_%s_utilization.gp" % prefix)
  plot_file = open(plot_filename, 'w')
  graph_filename = os.path.join(output_dir, "%s_utilization.pdf" % prefix)

  for line in open(base_plot_filename, 'r'):
    new_line = line.replace("__UTILIZATIONS_DATA_FILENAME__", utilizations_data_filename)
    new_line = new_line.replace("__OUTPUT_FILENAME__", graph_filename)
    plot_file.write(new_line)

  plot_file.close()

  # Generate the graph.
  subprocess.check_call("gnuplot %s" % plot_filename, shell=True)
  print "Generated graph of utilizations for machine: %s and phase: %s" % (machine_name, phase)


def filter_write_phase(all_jobs_dict):
  sorted_jobs = sorted(all_jobs_dict.iteritems())
  return {k:v for (k, v) in sorted_jobs[1:(len(sorted_jobs) / 2) + 1]}


def filter_read_phase(all_jobs_dict):
  sorted_jobs = sorted(all_jobs_dict.iteritems())
  return {k:v for (k, v) in sorted_jobs[(len(sorted_jobs) / 2) + 1:]}


def build_data_line(path, experiment, x_coordinate, reduction_factor):
  results_file = open(path, 'r')
  line = results_file.readline()
  results_file.close()

  new_line = "%s\t%s" % (experiment, x_coordinate)
  for val in map(lambda x: float(x) / reduction_factor, line.split("\t")):
    new_line = "%s\t%s" % (new_line, val)

  return new_line + "\n"


if __name__ == "__main__":
  main(sys.argv[1:])
