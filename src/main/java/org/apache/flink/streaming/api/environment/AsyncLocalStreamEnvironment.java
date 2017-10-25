package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationResponse;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.CurrentJobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages.JobNotFound;
import org.apache.flink.runtime.messages.JobManagerMessages.JobStatusResponse;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.graph.StreamGraph;

import scala.concurrent.Await;
import scala.concurrent.Future;

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

		_exec.start();
		JobSubmissionResult result = _exec.submitJobDetached(jobGraph);
		return result;
	}
	
	public boolean isRunning(JobID jobID) throws Exception {
		ActorGateway leader = _exec.getLeaderGateway(_exec.timeout());
		Future<Object> response = leader.ask(new JobManagerMessages.RequestJobStatus(jobID), _exec.timeout());
		Object result = Await.result(response, _exec.timeout());
		if (result instanceof CurrentJobStatus) {
			JobStatus jobStatus = ((CurrentJobStatus)result).status();
			return !jobStatus.isGloballyTerminalState();
		} else if (response instanceof JobNotFound) {
			return false;
		} else {
			throw new RuntimeException("Unexpected response to job status: " + result);
		}
	}
	
	public void stop(JobID jobID) throws Exception {
		try {
			// Try to cancel the job.
			ActorGateway leader = _exec.getLeaderGateway(_exec.timeout());
			Future<Object> response = leader.ask(new JobManagerMessages.CancelJob(jobID), _exec.timeout());

			Object result = Await.result(response, _exec.timeout());
			if (result instanceof CancellationSuccess) {
				// All good.
			} else if (result instanceof CancellationFailure) {
				CancellationFailure failure = (CancellationFailure)result;
				throw new RuntimeException("Failure cancelling job", failure.cause());
			} else {
				throw new RuntimeException("Unexpected result of cancelling job: " + result);
			}
		} finally {
			transformations.clear();
			_exec.stop();
		}
	}

}
