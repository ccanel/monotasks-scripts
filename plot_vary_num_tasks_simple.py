#
# Copyright 2016 The Regents of The University California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This script graphs the runtimes of a series of experiments with different numbers of tasks.
"""

import argparse
import functools
from matplotlib import pyplot
from matplotlib.backends import backend_pdf
import numpy
import os
from os import path
import re
import subprocess

import parse_event_logs


def main():
  args = __parse_args()
  num_warmup_trials = args.num_warmup_trials
  monotasks_num_tasks_to_jcts = __get_num_tasks_to_jcts(args.monotasks_dir, num_warmup_trials)
  spark_num_tasks_to_jcts = __get_num_tasks_to_jcts(args.spark_dir, num_warmup_trials)
  #__plot_num_tasks_vs_jct(monotasks_num_tasks_to_jcts, spark_num_tasks_to_jcts, args.output_dir)
  __plot_num_tasks_vs_jct_gnuplot(monotasks_num_tasks_to_jcts, spark_num_tasks_to_jcts, args.gnuplot_base, args.output_dir)


def __parse_args():
  parser = argparse.ArgumentParser(description="Plot num tasks versus JCT")
  parser.add_argument(
    "-m",
    "--monotasks-dir",
    help="The directory in which the experiment log directories for Monotasks are stored.",
    required=True)
  parser.add_argument(
    "-s",
    "--spark-dir",
    help="The directory in which the experiment log directories for Spark are stored.",
    required=True)
  parser.add_argument(
    "-o",
    "--output-dir",
    help="The firectory in which the graph and data files will be stored.",
    required=True)
  parser.add_argument(
    "-w",
    "--num-warmup-trials",
    default=0,
    help=("The number of warmup trials from the beginning of each experiment that should be " +
      "discarded."),
    required=False,
    type=int)
  parser.add_argument(
    "-g",
    "--gnuplot-base",
    help="The gnuplot .gp file to use as a template.",
    required=True)
  return parser.parse_args()


def __get_num_tasks_to_event_log(log_dir):
  """
  Returns a mapping from a number of tasks to the path to an event log file, for each experiment
  result directory in log_dir. The name of each directory in log_dir must start with
  "experiment_log".
  """
  all_dirs = [path.join(log_dir, dirname)
    for dirname in os.listdir(log_dir) if "experiment_log" in dirname]
  return {__extract_num_tasks(dirpath): path.join(log_dir, dirpath, "event_log")
    for dirpath in all_dirs if "event_log" in os.listdir(dirpath)}


def __extract_num_tasks(dirpath):
  """
  Extracts the number of tasks from the provided result directory path. The result directory name
  must be of the form "experiment_log_<num workers>_<num tasks>_<other params>".
  """
  return int(re.search('experiment_log_[0-9]*_([0-9]*)_', dirpath).group(1))


def __get_num_tasks_to_jcts(log_dir, num_warmup_trials):
  """
  Returns a mapping from number of tasks to a list of the JCTs from the jobs that used that number
  of tasks.
  """
  num_tasks_to_event_log = __get_num_tasks_to_event_log(log_dir)
  partial_filterer = functools.partial(__filterer, num_warmup_trials)
  return {num_tasks:
      [float(job.runtime()) / 1000
        for job in parse_event_logs.Analyzer(event_log, partial_filterer).jobs.itervalues()]
    for num_tasks, event_log in num_tasks_to_event_log.iteritems()}


def __filterer(num_warmup_trials, all_jobs_dict):
  """
  Eliminate the first job, because it generates the input data and writes it to HDFS. Then drop the
  warmup trials, keeping in mind that each job is preceeded by a job that clears the buffer cache
  and forces a GC. Then drop the remainder of the GC/buffer cache jobs.
  """
  return {job_id: job
    for job_id, job in sorted(all_jobs_dict.iteritems())[(2 + (2 * num_warmup_trials))::2]}


def __plot_num_tasks_vs_jct(monotasks_num_tasks_to_jcts, spark_num_tasks_to_jcts, output_dir):
  """ Creates a graph of num tasks vs. JCT, with a line for Monotasks and a line for Spark. """
  pyplot.title("Num tasks vs. JCT")
  pyplot.xlabel("Num tasks")
  pyplot.ylabel("JCT (s)")
  pyplot.grid(b=True)
  max_jct = max(__get_max_jct(monotasks_num_tasks_to_jcts), __get_max_jct(spark_num_tasks_to_jcts))
  pyplot.ylim(ymin=0, ymax=(1.1 * max_jct))

  __plot_single_num_tasks_vs_jcts(monotasks_num_tasks_to_jcts, label="Monotasks")
  __plot_single_num_tasks_vs_jcts(spark_num_tasks_to_jcts, label="Spark")
  pyplot.legend()

  with backend_pdf.PdfPages(path.join(output_dir, "num_tasks_vs_jct.pdf")) as pdf:
    pdf.savefig()
  pyplot.close()


def __plot_num_tasks_vs_jct_gnuplot(monotasks_num_tasks_to_jcts, spark_num_tasks_to_jcts, gnuplot_base_filepath, output_dir):
  plot_filepath = path.join(output_dir, "num_tasks_vs_jct_gnuplot.gp")
  spark_data_filepath = path.join(output_dir, "spark_results.data")
  monotasks_data_filepath = path.join(output_dir, "monotasks_results.data")
  graph_filepath = path.join(output_dir, "num_tasks_vs_jct_gnuplot.pdf")

  print "monotasks: {}".format(monotasks_num_tasks_to_jcts)
  print "spark: {}".format(spark_num_tasks_to_jcts)

  print sorted(monotasks_num_tasks_to_jcts.keys())

  base_i = 0

  sorted_num_tasks_values = sorted(monotasks_num_tasks_to_jcts.keys())
  xtics = []
  i = base_i
  for num_tasks_value in sorted_num_tasks_values:
    xtics.append("\"{}\" {}".format(num_tasks_value, i))
    i += 1
  xtics = "({})".format(", ".join(xtics))

  with open(plot_filepath, "w") as plot_file:
    for line in open(gnuplot_base_filepath, "r"):
      new_line = line.replace("__OUTPUT_FILEPATH__", graph_filepath)
      new_line = new_line.replace("__XTICS__", xtics)
      new_line = new_line.replace("__SPARK_DATA_FILEPATH__", spark_data_filepath)
      new_line = new_line.replace("__MONOTASKS_DATA_FILEPATH__", monotasks_data_filepath)
      plot_file.write(new_line)

  with open(monotasks_data_filepath, "w") as monotasks_data_file, \
       open(spark_data_filepath, "w") as spark_data_file:
    i = base_i
    for num_tasks_value in sorted_num_tasks_values:
      spark_jcts = spark_num_tasks_to_jcts[num_tasks_value]
      spark_min = min(spark_jcts)
      spark_median = numpy.median(spark_jcts)
      spark_max = max(spark_jcts)
      print "spark: {}: {}".format(num_tasks_value, sorted(spark_jcts))

      monotasks_jcts = monotasks_num_tasks_to_jcts[num_tasks_value]
      monotasks_min = min(monotasks_jcts)
      monotasks_median = numpy.median(monotasks_jcts)
      monotasks_max = max(monotasks_jcts)
      print "monotasks: {}: {}".format(num_tasks_value, sorted(monotasks_jcts))

      spark_data_file.write("{} {} {} {}\n".format(i, spark_median, spark_min, spark_max))
      monotasks_data_file.write("{} {} {} {}\n".format(i, monotasks_median, monotasks_min, monotasks_max))
      i += 1

  subprocess.check_call("gnuplot {}".format(plot_filepath), shell=True)
  print "plot_filepath: {}".format(plot_filepath)


def __get_max_jct(num_tasks_to_jcts):
  return max([jct
    for jcts in num_tasks_to_jcts.itervalues()
    for jct in jcts])


def __plot_single_num_tasks_vs_jcts(num_tasks_to_jcts, label):
  """ Adds a line to the current graph of num tasks vs. JCT. """
  all_num_tasks, all_jcts = zip(*sorted(num_tasks_to_jcts.iteritems()))
  medians = [numpy.median(jcts) for jcts in all_jcts]
  yerr = zip(*[[medians[i] - min(all_jcts[i]), max(all_jcts[i]) - medians[i]]
    for i in xrange(len(all_jcts))])
  pyplot.errorbar(all_num_tasks, medians, yerr=yerr, label=label)


if __name__ == "__main__":
  main()
