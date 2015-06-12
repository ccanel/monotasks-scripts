import json
import math
import optparse
import subprocess
import sys

def write_data(out_file, data):
  stringified = [str(x) for x in data]
  out_file.write("\t".join(stringified))
  out_file.write("\n")

def plot_continuous_monitor(filename):
  out_filename = "%s_utilization" % filename
  out_file = open(out_filename, "w")

  # Write plot files.
  utilization_plot_filename = "%s_utilization.gp" % filename
  utilization_plot_file = open(utilization_plot_filename, "w")
  for line in open("plot_utilization_base.gp", "r"):
    new_line = line.replace("__NAME__", out_filename)
    utilization_plot_file.write(new_line)
  utilization_plot_file.close()

  monotasks_plot_filename = "%s_monotasks.gp" % filename
  monotasks_plot_file = open(monotasks_plot_filename, "w")
  for line in open("plot_monotasks_base.gp", "r"):
    new_line = line.replace("__OUT_FILENAME__", "%s_monotasks.pdf" % filename).replace(
      "__NAME__", out_filename)
    monotasks_plot_file.write(new_line)
  monotasks_plot_file.close()

  start = -1
  at_beginning = True
  for (i, line) in enumerate(open(filename, "r")):
    try:
      json_data = json.loads(line)
    except ValueError:
      # This typically happens at the end of the file, which can get cutoff when the job stops.
      print "Stopping parsing due to incomplete line"
      if not at_beginning:
        break
      else:
        # There are some non-JSON lines at the beginning of the file.
        print "Skipping non-JSON line at beginning of file: %s" % line
        continue
    at_beginning = False
    time = json_data["Current Time"]
    if start == -1:
      start = time

    disk_utilization = json_data["Disk Utilization"]["Device Name To Utilization"]
    xvdf_total_utilization = disk_utilization[0]["xvdf"]["Disk Utilization"]
    xvdb_total_utilization = disk_utilization[1]["xvdb"]["Disk Utilization"]
    xvdf_write_throughput = disk_utilization[0]["xvdf"]["Write Throughput"]
    xvdb_write_throughput = disk_utilization[1]["xvdb"]["Write Throughput"]
    xvdf_read_throughput = disk_utilization[0]["xvdf"]["Read Throughput"]
    xvdb_read_throughput = disk_utilization[1]["xvdb"]["Read Throughput"]
    cpu_utilization = json_data["Cpu Utilization"]
    cpu_total = (cpu_utilization["Total User Utilization"] +
    cpu_utilization["Total System Utilization"])
    network_utilization = json_data["Network Utilization"]
    bytes_received = network_utilization["Bytes Received Per Second"]
    running_compute_monotasks = 0
    if "Running Compute Monotasks" in json_data:
      running_compute_monotasks = json_data["Running Compute Monotasks"]
    running_macrotasks = 0
    if "Running Macrotasks" in json_data:
      running_macrotasks = json_data["Running Macrotasks"]
    total_disk_queue_length = 0
    if "Total Disk Queue Length" in json_data:
      total_disk_queue_length = json_data["Total Disk Queue Length"]
    running_disk_monotasks = 0
    if "Running Disk Monotasks" in json_data:
      running_disk_monotasks = json_data["Running Disk Monotasks"]
    gc_fraction = 0
    if "Fraction GC Time" in json_data:
      gc_fraction = json_data["Fraction GC Time"]
    outstanding_network_bytes = 0
    if "Outstanding Network Bytes" in json_data:
      outstanding_network_bytes = json_data["Outstanding Network Bytes"]
    if bytes_received == "NaN" or bytes_received == "Infinity":
      continue
    bytes_transmitted = network_utilization["Bytes Transmitted Per Second"]
    if bytes_transmitted == "NaN" or bytes_transmitted == "Infinity":
      continue
    if str(cpu_total).find("NaN") > -1 or str(cpu_total).find("Infinity") > -1:
      continue
    macrotasks_in_network = 0
    if "Macrotasks In Network" in json_data:
      macrotasks_in_network = json_data["Macrotasks In Network"]
    macrotasks_in_compute = 0
    if "Macrotasks In Compute" in json_data:
      macrotasks_in_compute = json_data["Macrotasks In Compute"]

    if (math.isnan(float(xvdf_write_throughput)) or math.isinf(float(xvdf_write_throughput))):
      print "xvdf write throughput is NaN or inf"
      xvdf_write_throughput = 0.0
    if (math.isnan(float(xvdb_write_throughput)) or math.isinf(float(xvdb_write_throughput))):
      print "xvdb write throughput is NaN or inf"
      xvdb_write_throughput = 0.0
    if (math.isnan(float(xvdf_read_throughput)) or math.isinf(float(xvdf_read_throughput))):
      print "xvdf read throughput is NaN or inf"
      xvdf_read_throughput = 0.0
    if (math.isnan(float(xvdb_read_throughput)) or math.isinf(float(xvdb_read_throughput))):
      print "xvdb read throughput is NaN or inf"
      xvdb_read_throughput = 0.0

    data = [
      time - start,
      xvdf_total_utilization,
      xvdb_total_utilization,
      xvdf_write_throughput / 100000000.0,
      xvdb_write_throughput / 100000000.0,
      xvdf_read_throughput / 100000000.0,
      xvdb_read_throughput / 100000000.0,
      cpu_total / 8.0,
      bytes_received / 125000000.,
      bytes_transmitted / 125000000.,
      running_compute_monotasks,
      running_macrotasks,
      total_disk_queue_length,
      running_disk_monotasks,
      gc_fraction,
      outstanding_network_bytes / (1024 * 1024),
      macrotasks_in_network,
      macrotasks_in_compute]
    write_data(out_file, data)
  out_file.close()

  subprocess.check_call("gnuplot %s" % utilization_plot_filename, shell=True)
  subprocess.check_call("open %s_utilization.pdf" % filename, shell=True)

  subprocess.check_call("gnuplot %s" % monotasks_plot_filename, shell=True)
  subprocess.check_call("open %s_monotasks.pdf" % filename, shell=True)

def main():
  parser = optparse.OptionParser(usage="foo.py <log filename")
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)

  filename = args[0]
  plot_continuous_monitor(filename)

if __name__ == "__main__":
  main()
