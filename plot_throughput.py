
import random
import subprocess

MAIN_RESULTS_DIR = "/Users/christophercanel/Dropbox/Research/results/disk_experiments/graphs/03_num_concurrent_tasks_vs_throughput_02/ec2_m2.4xlarge/"

MONOTASKS_WRITE_TEST_RESULT_PATHS = [
    MAIN_RESULTS_DIR + "1/monotasks/ec2_1_monotasks_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "2/monotasks/ec2_2_monotasks_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "4/monotasks/ec2_4_monotasks_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "8/monotasks/ec2_8_monotasks_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "16/monotasks/ec2_16_monotasks_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "24/monotasks/ec2_24_monotasks_event_log_disk_write_throughput"
]

MONOTASKS_READ_TEST_RESULT_PATHS = [
    MAIN_RESULTS_DIR + "1/monotasks/ec2_1_monotasks_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "2/monotasks/ec2_2_monotasks_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "4/monotasks/ec2_4_monotasks_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "8/monotasks/ec2_8_monotasks_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "16/monotasks/ec2_16_monotasks_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "24/monotasks/ec2_24_monotasks_event_log_disk_read_throughput"
]

SPARK_WRITE_TEST_RESULT_PATHS = [
    MAIN_RESULTS_DIR + "1/spark/ec2_1_spark_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "2/spark/ec2_2_spark_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "4/spark/ec2_4_spark_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "8/spark/ec2_8_spark_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "16/spark/ec2_16_spark_event_log_disk_write_throughput",
    MAIN_RESULTS_DIR + "24/spark/ec2_24_spark_event_log_disk_write_throughput"
]

SPARK_READ_TEST_RESULT_PATHS = [
    MAIN_RESULTS_DIR + "1/spark/ec2_1_spark_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "2/spark/ec2_2_spark_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "4/spark/ec2_4_spark_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "8/spark/ec2_8_spark_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "16/spark/ec2_16_spark_event_log_disk_read_throughput",
    MAIN_RESULTS_DIR + "24/spark/ec2_24_spark_event_log_disk_read_throughput"
]

def build_output_line(path, i):
    file = open(path, 'r')
    line = file.readline()
    file.close()

    vals = line.split("\t")
    new_vals = map(lambda x: float(x) / 1000000, vals)
    new_line = str(random.randint(1, 10)) + "\t" + str(i)
    for val in new_vals:
        new_line = new_line + "\t" + str(val)

    return new_line + "\n"

def main():
    monotasks_write_output_filename = "monotasks_write_throughput.data"
    monotasks_read_output_filename = "monotasks_read_throughput.data"
    spark_write_output_filename = "spark_write_throughput.data"
    spark_read_output_filename = "spark_read_throughput.data"
    write_graph_filename = "write_throughput.pdf"
    read_graph_filename = "read_throughput.pdf"

    # Write a plot file for the write tests.
    write_plot_filename = "plot_write_throughput.gp"
    plot_file = open(write_plot_filename, "w")
    for line in open("plot_throughput_base.gp", "r"):
        new_line = line.replace("__MONOTASKS_NAME__", monotasks_write_output_filename)
        new_line = new_line.replace("__SPARK_NAME__", spark_write_output_filename)
        new_line = new_line.replace("__OUTPUT_FILENAME__", write_graph_filename)
        plot_file.write(new_line)
    plot_file.close()

    # Write a plot file for the read tests.
    read_plot_filename = "plot_read_throughput.gp"
    plot_file = open(read_plot_filename, "w")
    for line in open("plot_throughput_base.gp", "r"):
        new_line = line.replace("__MONOTASKS_NAME__", monotasks_read_output_filename)
        new_line = new_line.replace("__SPARK_NAME__", spark_read_output_filename)
        new_line = new_line.replace("__OUTPUT_FILENAME__", read_graph_filename)
        plot_file.write(new_line)
    plot_file.close()

    # Construct the monotasks write data file.
    monotasks_write_output_lines = []
    i = 1
    for path in MONOTASKS_WRITE_TEST_RESULT_PATHS:
        monotasks_write_output_lines.append(build_output_line(path, i))
        i += 1
    monotasks_write_output_file = open(monotasks_write_output_filename, 'w')
    for line in monotasks_write_output_lines:
        monotasks_write_output_file.write(line)
    monotasks_write_output_file.close()

    # Construct the monotasks write data file.
    monotasks_read_output_lines = []
    i = 1
    for path in MONOTASKS_READ_TEST_RESULT_PATHS:
        monotasks_read_output_lines.append(build_output_line(path, i))
        i += 1
    monotasks_read_output_file = open(monotasks_read_output_filename, 'w')
    for line in monotasks_read_output_lines:
        monotasks_read_output_file.write(line)
    monotasks_read_output_file.close()

    # Construct the Spark write data file.
    spark_write_output_lines = []
    i = 1
    for path in SPARK_WRITE_TEST_RESULT_PATHS:
        spark_write_output_lines.append(build_output_line(path, i))
        i += 1
    spark_write_output_file = open(spark_write_output_filename, 'w')
    for line in spark_write_output_lines:
        spark_write_output_file.write(line)
    spark_write_output_file.close()

    # Construct the Spark write data file.
    spark_read_output_lines = []
    i = 1
    for path in SPARK_READ_TEST_RESULT_PATHS:
        spark_read_output_lines.append(build_output_line(path, i))
        i += 1
    spark_read_output_file = open(spark_read_output_filename, 'w')
    for line in spark_read_output_lines:
        spark_read_output_file.write(line)
    spark_read_output_file.close()

    # Plot the graph and open the resulting PDF.
    subprocess.check_call("gnuplot %s" % write_plot_filename, shell=True)
    subprocess.check_call("gnuplot %s" % read_plot_filename, shell=True)
    subprocess.check_call("open %s" % write_graph_filename, shell=True)
    subprocess.check_call("open %s" % read_graph_filename, shell=True)

if __name__ == "__main__":
    main()
