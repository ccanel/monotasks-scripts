
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
* There can be any number of programs, name in any fashion.
"""

import collections
import os
import subprocess
import sys

import parse_event_logs


def main(argv):
  root_dir = argv[0]
  output_dir = argv[1]
  base_plot_filename = argv[2]

  event_logs = find_event_logs(root_dir)
  if (event_logs is None):
    print "Log directory structure is not properly formed!"
    return

  for (machine_name, programs) in format_dictionary(event_logs).iteritems():
    for (program_name, event_logs) in programs.iteritems():
      generate_program_graphs(machine_name, program_name, event_logs, base_plot_filename, output_dir)

  print "Done"


"""
Navigates the filesystem hierarchy described in the comment and creates the following dictionary:
  {machine name -> {experiment name -> {program name -> event log file}}}

Returns None if the directory structure is not properly formed.
"""
def find_event_logs(root_dir):
  master_experiments = None
  master_programs = None
  machines = dict()

  for machine_name in os.listdir(root_dir):
    # For ever machine:
    machine_dir = os.path.join(root_dir, machine_name)

    if (os.path.isdir(machine_dir)):
      experiments = dict()

      for experiment_name in os.listdir(machine_dir):
        # For every experiment:
        experiment_dir = os.path.join(machine_dir, experiment_name)
        if (os.path.isdir(experiment_dir)):
          programs = dict()

          for program_name in os.listdir(experiment_dir):
            # For every program:
            program_dir = os.path.join(experiment_dir, program_name)

            if (os.path.isdir(program_dir)):
              # Get the event log filename.
              programs[program_name] = os.path.join(
                program_dir, "%s_%s_%s_event_log" % (machine_name, experiment_name, program_name))

          # Verify that each experiment directory contains the correct program directories.
          program_names = programs.keys()
          if (master_programs is None):
            master_programs = program_names
          elif (collections.Counter(program_names) != collections.Counter(master_programs)):
            print "Programs are wrong! expected: %s found:%s " % (master_programs, program_names)
            return None

          experiments[experiment_name] = programs

      # Verify that each machine directory contains the correct experiment directories.
      experiment_names = experiments.keys()
      if (master_experiments is None):
        master_experiments = experiment_names
      elif (collections.Counter(experiment_names) != collections.Counter(master_experiments)):
        print "Experiments are wrong! expected: %s found:%s " % (master_experiments, experiment_names)
        return None

      machines[machine_name] = experiments

  return machines


"""
Transforms the event_logs dictionary (whose format is described in the comment for the function
find_event_logs()) into this format:
  {machine name -> {program name -> List(experiment 1 event log, experiment 2 event log, etc.)}}
"""
def format_dictionary(event_logs):
  machines = dict()

  for (machine_name, experiments) in event_logs.iteritems():
    programs = dict()

    # Loop over the experiments in sorted order.
    for experiment in sorted(map(lambda x: int(x), experiments.keys())):
      for (program_name, event_log) in experiments[str(experiment)].iteritems():
        if (program_name in programs):
          programs[program_name].append((experiment, event_log))
        else:
          programs[program_name] = [(experiment, event_log)]

    machines[machine_name] = programs

  return machines

"""
Generates two graphs for the specified program: one for the write phase and one for the read phase.
The program's raw event logs are provided.
"""
def generate_program_graphs(machine, program, event_logs, base_plot_filename, output_dir):
  generate_phase_graph(machine, program, event_logs, True, base_plot_filename, output_dir)
  generate_phase_graph(machine, program, event_logs, False, base_plot_filename, output_dir)


"""
Generates a disk throughput graph for a single experiment phase for the specified program. Creates
the write phase graph if do_write is True, or the read phase graph if do_read is False. The
program's raw event logs are provided.
"""
def generate_phase_graph(machine, program, event_logs, do_write, base_plot_filename, output_dir):

  (phase, filter) = ("write", filter_write_phase) if do_write else ("read", filter_read_phase)
  identifier = "%s_%s_%s-phase" % (machine, program, phase)
  write_data_filename = os.path.join(output_dir, "%s_write_throughputs.data" % identifier)
  read_data_filename = os.path.join(output_dir, "%s_read_throughputs.data" % identifier)
  totals_data_filename = os.path.join(output_dir, "%s_total_throughputs.data" % identifier)
  graph_filename = os.path.join(output_dir, "%s_throughput.pdf" % identifier)

  # Construct the plot files.
  plot_filename = os.path.join(output_dir, "plot_%s_throughput.gp" % identifier)
  plot_file = open(plot_filename, 'w')

  for line in open(base_plot_filename, 'r'):
    new_line = line.replace("__WRITE_DATA_FILENAME__", write_data_filename)
    new_line = new_line.replace("__READ_DATA_FILENAME__", read_data_filename)
    new_line = new_line.replace("__TOTALS_DATA_FILENAME__", totals_data_filename)
    new_line = new_line.replace("__OUTPUT_FILENAME__", graph_filename)
    plot_file.write(new_line)
  plot_file.close()

  # Construct the data files.
  write_data_file = open(write_data_filename, 'w')
  read_data_file = open(read_data_filename, 'w')
  totals_data_file = open(totals_data_filename, 'w')
  i = 1

  for (experiment, event_log) in event_logs:
    parse_event_logs.Analyzer(event_log, filter).output_utilizations(event_log)

    write_throughputs_filename = "%s_disk_write_throughput" % event_log
    write_data_file.write(build_data_line(write_throughputs_filename, experiment, i))

    read_throughputs_filename = "%s_disk_read_throughput" % event_log
    read_data_file.write(build_data_line(read_throughputs_filename, experiment, i))

    total_throughputs_filename = "%s_total_disk_throughput" % event_log
    totals_data_file.write(build_data_line(total_throughputs_filename, experiment, i))

    i += 1

  write_data_file.close()
  read_data_file.close()
  totals_data_file.close()

  # Generate the graph.
  subprocess.check_call("gnuplot %s" % plot_filename, shell=True)
  print "Generated graph for machine: %s, program: %s, phase: %s" % (machine, program, phase)


def filter_write_phase(all_jobs_dict):
  sorted_jobs = sorted(all_jobs_dict.iteritems())
  return {k:v for (k, v) in sorted_jobs[1:(len(sorted_jobs) / 2) + 1]}


def filter_read_phase(all_jobs_dict):
  sorted_jobs = sorted(all_jobs_dict.iteritems())
  return {k:v for (k, v) in sorted_jobs[(len(sorted_jobs) / 2) + 1:]}


def build_data_line(path, experiment, x_coordinate):
  file = open(path, 'r')
  line = file.readline()
  file.close()

  new_line = str(experiment) + "\t" + str(x_coordinate)
  for val in map(lambda x: float(x) / 1000000, line.split("\t")):
    new_line = new_line + "\t" + str(val)

  return new_line + "\n"


if __name__ == "__main__":
  main(sys.argv[1:])
