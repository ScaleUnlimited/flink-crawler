package com.scaleunlimited.flinkcrawler.topology;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironmentWithAsyncExecution;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.utils.FlinkUtils;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

/**
 * A Flink streaming workflow that can be executed.
 * 
 * State Checkpoints in Iterative Jobs
 * 
 * Flink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an
 * iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a
 * special flag when enabling checkpointing: env.enableCheckpointing(interval, force = true).
 * 
 * Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during
 * failure.
 * 
 */
public class CrawlTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(CrawlTopology.class);

    private StreamExecutionEnvironment _env;
    private String _jobName;
    private JobID _jobID;
    
    protected CrawlTopology(StreamExecutionEnvironment env, String jobName) {
        _env = env;
        _jobName = jobName;
    }

    public void printDotFile(File outputFile) throws IOException {
    	String dotAsString = FlinkUtils.planToDot(_env.getExecutionPlan());
    	FileUtils.write(outputFile, dotAsString, "UTF-8");
    }
    
    public JobExecutionResult execute() throws Exception {
        return _env.execute(_jobName);
    }

    public JobSubmissionResult executeAsync() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	JobSubmissionResult result = env.executeAsync(_jobName);
    	_jobID = result.getJobID();
    	return result;
    }
    
    public boolean isRunning() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	return env.isRunning(_jobID);
    }
    
    public void stop() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	env.stop(_jobID);
    	
    	// Wait for 5 seconds for the job to terminate.
    	long endTime = System.currentTimeMillis() + 5_000L;
    	while (env.isRunning(_jobID) && (System.currentTimeMillis() < endTime)) {
    		Thread.sleep(100L);
    	}
    	
    	// Stop the job execution environment.
    	env.stop();
    }
    
	/**
	 * Trigger async execution, and then monitor the job. Fail if it the job is
	 * still running after <maxDurationMS> milliseconds.
	 * 
	 * @param maxDurationMS Maximum allowable execution time.
	 * @param maxQuietTimeMS Length of time w/no recorded activity after which we'll terminate.
	 * @throws Exception
	 */
	public void execute(int maxDurationMS, int maxQuietTimeMS) throws Exception {
		LOGGER.info("Starting async job {}", _jobName);
		
		// Reset time, since this is a static that can keep its value from a previous
		// test run.
		UrlLogger.resetActivityTime();
		executeAsync();
		
		boolean terminated = false;
		long endTime = System.currentTimeMillis() + maxDurationMS;
		while (System.currentTimeMillis() < endTime) {
			long lastActivityTime = UrlLogger.getLastActivityTime();
			if (lastActivityTime != UrlLogger.NO_ACTIVITY_TIME) {
				long curTime = System.currentTimeMillis();
				if ((curTime - lastActivityTime) > maxQuietTimeMS) {
					LOGGER.info("Stopping async job {} due to lack of activity", _jobName);

					stop();
					terminated = true;
					break;
				}
			}

			Thread.sleep(100L);
		}
		
		if (!terminated) {
			LOGGER.info("Stopping async job {} due to timeout", _jobName);
			stop();
			throw new RuntimeException("Job did not terminate in time");
		}
	}
}
