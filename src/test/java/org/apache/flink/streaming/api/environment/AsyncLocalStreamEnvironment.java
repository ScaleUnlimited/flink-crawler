package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.graph.StreamGraph;

public class AsyncLocalStreamEnvironment extends LocalStreamEnvironment {

	private Configuration _conf;
	private LocalFlinkMiniCluster _exec;
	
	public AsyncLocalStreamEnvironment() {
		this(new Configuration());
	}

	public AsyncLocalStreamEnvironment(Configuration config) {
		super(config);
		
		_conf = config;
	}
	
	public JobSubmissionResult executeAsync(String jobName) throws Exception {
		// transform the streaming program into a JobGraph
		StreamGraph streamGraph = getStreamGraph();
		streamGraph.setJobName(jobName);

		JobGraph jobGraph = streamGraph.getJobGraph();

		Configuration configuration = new Configuration();
		configuration.addAll(jobGraph.getJobConfiguration());

		configuration.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, -1L);
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

		// add (and override) the settings with what the user defined
		configuration.addAll(_conf);

		_exec = new LocalFlinkMiniCluster(configuration, true);
		try {
			_exec.start();
			return _exec.submitJobDetached(jobGraph);
		} catch (Exception e) {
			stop();
			
			throw e;
		}
	}
	
	public void stop() {
		transformations.clear();
		_exec.stop();
	}


}
