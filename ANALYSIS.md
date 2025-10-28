Analysis Report:

Prob1:
For Problem 1, the objective was to parse Spark log files and reconstruct application timelines using PySpark. I began by loading the raw text log files from the cluster into Spark and applying regular expressions to extract structured fields, including timestamps, event types, and application IDs. These events were then aggregated to determine the start and end time for each application. The analysis produced three key outputs: problem1_counts.csv containing counts of each event type, problem1_sample.csv with representative application timeline entries, and problem1_summary.txt summarizing global statistics.

The results revealed that a total of 194 applications were recorded in the logs. The timeline data showed clear clusters of job activity concentrated within similar time windows, which suggests that the workload may be driven by automated job scheduling or batch submission processes. This pattern is consistent with typical Spark production environments where most jobs are triggered at regular intervals.

In terms of performance, running the script in local mode was fast and reliable. Cluster execution, by contrast, encountered significant issues caused by JAR distribution overhead and network streaming failures during dependency resolution.

For innovative visualization, I think a heatmap across time could reveal peak hours and support planning.

Prob2:
Problem 2 focused on analyzing the distribution of application workloads across clusters and understanding job duration characteristics. Using the same log data, I extracted cluster IDs, application IDs, start and end times, and calculated per-cluster summary statistics. The outputs of this analysis included problem2_timeline.csv, problem2_cluster_summary.csv, and problem2_stats.txt as well as two visualizations: problem2_bar_chart.png showing the number of applications per cluster and problem2_density_plot.png showing the distribution of application durations on a log scale.

The analysis revealed a strong imbalance in workload distribution. One cluster processed approximately 181 out of 194 applications, which accounts for more than 90% of the total workload. The distribution of job durations was right-skewed and followed a log-normal pattern. Most applications completed within tens of minutes, but a small number of outliers ran for several hours, contributing to a heavy-tailed distribution.The summary statistics in problem2_stats.txt confirm the presence of six clusters with an average of 32.33 applications per cluster, although most of this average is skewed by a single dominant cluster.

The patterns discovered in both problems are meaningful. In Problem 1, timeline clustering suggests predictable, periodic workloads that may be tied to automated job scheduling. In Problem 2, the extreme cluster imbalance highlights opportunities for load balancing, while the heavy-tailed duration distribution points to a small number of outlier jobs that dominate total execution time.

Additional analysis could further enrich these findings. A cumulative distribution function (CDF) of job durations would reveal how much of the total runtime is accounted for by the longest-running jobs. Per-cluster boxplots could highlight differences in median and tail durations across clusters, while a heatmap of submission times would help identify daily or weekly usage patterns. These extra insights would provide valuable information for cluster resource planning and job scheduling optimization.