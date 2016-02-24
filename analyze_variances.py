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

import argparse
import json
import numpy
from matplotlib import pyplot
from matplotlib.backends import backend_pdf
from scipy.stats import beta, norm

import parse_event_logs


def main():
  args = __parse_args()
  Analyzer(args.event_log_file, args.continuous_monitor_log_file, args.output_file)


def __parse_args():
  parser = argparse.ArgumentParser(description="Extracts compute task time information.")
  parser.add_argument(
    "-e",
    "--event-log-file",
    help="The path to an event log file to analyze.",
    required=True)
  parser.add_argument(
    "-c",
    "--continuous-monitor-log-file",
    help="The path to a continuous monitor log file to analysze.")
  parser.add_argument(
    "-o",
    "--output-file",
    help="The file to which the analysis results will be written.")
  return parser.parse_args()


class Analyzer(object):

  def __init__(self, event_log_file_path, continuous_monitor_file_path, output_file_path):
    self.event_log_file_path = event_log_file_path
    self.continuous_monitor_file_path = continuous_monitor_file_path
    if output_file_path is None:
      self.output_file = None
    else:
      self.output_file = open(output_file_path, "w")

    try:
      all_jobs = parse_event_logs.Analyzer(self.event_log_file_path).jobs
      self.__log_start_end_times(all_jobs)
      self.__log_detailed_job_info(all_jobs)

      map_stage_compute_times_ms, reduce_stage_compute_times_ms = self.__get_compute_times_ms()
      map_stage_compute_dist_output_file_path = (
        "%s_map_stage_compute_time_dist.pdf" % event_log_file_path)
      reduce_stage_compute_dist_output_file_path = (
        "%s_reduce_stage_compute_time_dist.pdf" % event_log_file_path)

      self.__log("\nStatistics:")
      self.__report_statistics(map_stage_compute_times_ms, map_stage_compute_dist_output_file_path,
        x_label="map stage compute time (ms)")
      self.__report_statistics(reduce_stage_compute_times_ms,
        reduce_stage_compute_dist_output_file_path, x_label="reduce stage compute time (ms)")

      if self.continuous_monitor_file_path is None:
        self.__log("No continuous monitor provided! Cannot analyze network bandwidth!")
      else:
        incoming_network_bandwidths_Mbps, outgoing_network_bandwidths_Mbps = (
          self.__get_network_bandwidths_Mbps())
        self.__report_statistics(
          incoming_network_bandwidths_Mbps,
          output_file_path="%s_incoming_network_bandwidth_dist.pdf" % continuous_monitor_file_path,
          x_label="incoming network bandwidth (Mb/s)")
        self.__report_statistics(
          outgoing_network_bandwidths_Mbps,
          output_file_path="%s_outgoing_network_bandwidth_dist.pdf" % continuous_monitor_file_path,
          x_label="outgoing network bandwidth (Mb/s)")
    finally:
      if self.output_file is not None:
        self.output_file.close()

  def __log(self, message):
    print message
    if self.output_file is not None:
      self.output_file.write("%s\n" % message)

  @staticmethod
  def keep_only_experiment_jobs_filterer(all_jobs_dict):
    """ Keeps all two-stage jobs except the first job, which is a warmup job. """
    num_warmup_jobs = 1
    # In MemorySortJob.scala, first there are num_warmup_jobs warmup jobs, then one job to generate
    # the input data, then the experiment jobs.
    return {job_id: job for job_id, job in all_jobs_dict.iteritems()
      if job_id >= (num_warmup_jobs + 1)}

  def __get_compute_times_ms(self):
    """
    Returns a tuple:
      (list of map stage compute monotask times, list of reduce stage compute monotask times)
    """
    experiment_jobs = parse_event_logs.Analyzer(
      self.event_log_file_path, Analyzer.keep_only_experiment_jobs_filterer).jobs
    self.__log("Experiment jobs: %s" % experiment_jobs.keys())

    all_map_stage_compute_times_ms = []
    all_reduce_stage_compute_times_ms = []
    for job in experiment_jobs.itervalues():
      stages = job.stages
      assert len(stages) == 2

      map_stage_compute_times_ms, reduce_stage_compute_times_ms = [
        compute_times_ms for _, compute_times_ms
        in sorted([
          (stage_id, [task.compute_monotask_millis for task in stage.tasks])
          for stage_id, stage in stages.iteritems()])]

      all_map_stage_compute_times_ms.extend(map_stage_compute_times_ms)
      all_reduce_stage_compute_times_ms.extend(reduce_stage_compute_times_ms)
    return all_map_stage_compute_times_ms, all_reduce_stage_compute_times_ms

  def __log_detailed_job_info(self, all_jobs):
    self.__log("\nDetailed job info:\n")

    for job_id, job in all_jobs.iteritems():
      self.__log("job %s:" % job_id)
      stages = job.stages
      num_stages = len(stages)
      if num_stages == 2:
        for stage_id, stage in stages.iteritems():
          executor_id_to_num_tasks = {}
          for task in stage.tasks:
            executor_id = task.executor_id
            old_num_tasks = executor_id_to_num_tasks.get(executor_id, 0)
            executor_id_to_num_tasks[executor_id] = old_num_tasks + 1
          self.__log("Stage %s num tasks per worker: %s" % (stage_id, executor_id_to_num_tasks))

        (_, map_stage), (_, reduce_stage) = sorted(stages.iteritems())

        map_stage_start_time_ms = map_stage.start_time
        self.__log("map stage start: %s ms" % map_stage_start_time_ms)
        reduce_stage_end_time_ms = reduce_stage.finish_time()
        self.__log(("reduce stage end: %s ms (delta: %s ms)" %
          (reduce_stage_end_time_ms, reduce_stage_end_time_ms - map_stage_start_time_ms)))
        actual_jct_ms = job.runtime()
        self.__log("actual jct: %s ms" % actual_jct_ms)

        ideal_jct_ms = (
          map_stage.ideal_time(
            cores_per_machine=8, disks_per_machine=0, output_file=self.output_file) +
          reduce_stage.ideal_time(
            cores_per_machine=8, disks_per_machine=0, output_file=self.output_file))
        self.__log("ideal jct: %.2f ms" % ideal_jct_ms)
        self.__log("jct inflation: %.2f%%" % (100 * (actual_jct_ms - ideal_jct_ms) / ideal_jct_ms))

        self.__log("%s tasks in the map stage" % len(map_stage.tasks))
        reduce_tasks = reduce_stage.tasks
        self.__log("%s tasks in reduce stage" % len(reduce_tasks))

        total_local_shuffle_bytes = 0
        executor_id_to_network_bytes_received = {}
        for task in reduce_tasks:
          total_local_shuffle_bytes += task.local_mb_read * 1048576
          executor_id = task.executor_id
          bytes_received = executor_id_to_network_bytes_received.get(executor_id, 0)
          executor_id_to_network_bytes_received[executor_id] = (bytes_received +
            (task.remote_mb_read * 1048576))

        total_remote_shuffle_bytes = sum(executor_id_to_network_bytes_received.values())
        self.__log(
          "Total shuffle bytes: %s" % (total_local_shuffle_bytes + total_remote_shuffle_bytes))
        self.__log("Bytes received per worker:")
        for executor_id, bytes_received in executor_id_to_network_bytes_received.iteritems():
          self.__log("  %s: %s bytes" % (executor_id, bytes_received))
        self.__log("Total remote shuffle bytes: %s\n" % total_remote_shuffle_bytes)
      else:
        self.__log("num stages: %s\n" % num_stages)

  def __log_start_end_times(self, all_jobs):
    """ Logs the start and end times of all jobs and stages in the provided event log. """
    self.__log("Start and end times:\n")
    beginning_of_time_ms = 1
    for job_id, job in all_jobs.iteritems():
      job_start_time_ms = min([stage.start_time for stage in job.stages.values()])
      if job_id == 0:
        beginning_of_time_ms = job_start_time_ms
      self.__log("job %s start: %s ms" % (job_id, job_start_time_ms - beginning_of_time_ms))

      for stage_id, stage in sorted(job.stages.iteritems()):
        stage_start_time_ms = stage.start_time
        stage_end_time_ms = stage.finish_time()
        self.__log("stage %s start: %s ms, end: %s ms, delta: %s ms" % (stage_id,
          stage_start_time_ms - beginning_of_time_ms, stage_end_time_ms - beginning_of_time_ms,
          stage_end_time_ms - stage_start_time_ms))

      job_end_time_ms = max([stage.finish_time() for stage in job.stages.values()])
      self.__log("job %s end: %s ms, delta: %s ms\n" % (
        job_id, job_end_time_ms - beginning_of_time_ms, job_end_time_ms - job_start_time_ms))

  def __get_network_bandwidths_Mbps(self):
    incoming_network_bandwidths_Mbps = []
    outgoing_network_bandwidths_Mbps = []
    with open(self.continuous_monitor_file_path) as continuous_monitor_file:
      for line in continuous_monitor_file:
        json_data = json.loads(line)
        network_utilization = json_data["Network Utilization"]
        incoming_network_bandwidths_Mbps.append(
          network_utilization["Bytes Received Per Second"] * 8 / (10 ** 6))
        outgoing_network_bandwidths_Mbps.append(
          network_utilization["Bytes Transmitted Per Second"] * 8 / (10 ** 6))

    threshold = 10
    return (Analyzer.__discard_outliers(incoming_network_bandwidths_Mbps, threshold),
      Analyzer.__discard_outliers(outgoing_network_bandwidths_Mbps, threshold))

  @staticmethod
  def __discard_outliers(values, threshold):
    return [value for value in values if value > threshold]

  def __report_statistics(self, values, output_file_path, x_label):
    """ Logs statistics about the provided values and generates a graph of their distribution. """
    if len(values) == 0:
      self.__log("No values to analyze for x_label: %s" % x_label)
      return

    sorted_values = sorted(values)
    min_value = sorted_values[0]
    max_value = sorted_values[-1]
    median_value = numpy.median(sorted_values)
    mean_value = numpy.mean(sorted_values)
    variance = float(max_value - min_value) / (max_value + min_value)

    self.__log("\nMin %s: %s" % (x_label, min_value))
    self.__log("Max %s: %s" % (x_label, max_value))
    self.__log("Median %s: %s" % (x_label, median_value))
    self.__log("Average %s : %.2f" % (x_label, mean_value))
    self.__log("Variance using current model: %.2f" % variance)

    pyplot.clf()
    pyplot.cla()
    pyplot.title("Distribution of %s" % x_label)
    pyplot.xlabel(x_label)
    pyplot.xlim(xmin=0, xmax=max_value)

    x = numpy.linspace(stop=0, start=max_value, num=100)
    # Fit a normal distribution
    norm_loc, norm_scale = norm.fit(sorted_values)
    norm_pdf_fitted = norm.pdf(x, norm_loc, norm_scale)
    pyplot.plot(x, norm_pdf_fitted, "r-", label="normal")
    self.__log("Normal distribution: loc: %.4f, scale: %.4f" % (norm_loc, norm_scale))

    # Fit a beta distribution
    a, b, beta_loc, beta_scale = beta.fit(sorted_values)
    beta_pdf_fitted = beta.pdf(x, a, b, beta_loc, beta_scale)
    pyplot.plot(x, beta_pdf_fitted, "y-", label="beta")
    self.__log(
      "Beta distribution: a: %.4f, b: %.4f, loc: %.4f, scale: %.4f" % (a, b, beta_loc, beta_scale))

    pyplot.hist(sorted_values, bins=50, normed=True)
    pyplot.legend(loc="upper right")
    pdf_output = backend_pdf.PdfPages(output_file_path)
    pdf_output.savefig()
    pdf_output.close()


if __name__ == "__main__":
  main()
