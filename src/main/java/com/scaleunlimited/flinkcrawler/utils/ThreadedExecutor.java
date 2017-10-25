package com.scaleunlimited.flinkcrawler.utils;

/*
 * Copyright 2009-2017 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper for ThreadPoolExecutor that implements a specific behavior we need.
 * When execute() is called, it succeeds unless all of the threads are busy and the
 * specified timeout is exceeded (no threads finish up in that amount of time).
 *
 */
public class ThreadedExecutor {
    
    /**
     * Always wait for some time when offer() is called. This gives any
     * active threads that much time to complete, before a RejectedExectionException
     * is thrown.
     *
     * @param <E> element stored in queue
     */
    @SuppressWarnings("serial")
    private class MyBlockingQueue<E> extends SynchronousQueue<E> {

        public MyBlockingQueue() {
            super(true);
        }

        @Override
        public boolean offer(E element) {
            try {
                return offer(element, _requestTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }


    private long _requestTimeout;
    private ThreadPoolExecutor _pool;
    
    public ThreadedExecutor(int numThreads) {
    	this(numThreads, Long.MAX_VALUE);
    }
    
    public ThreadedExecutor(int numThreads, long requestTimeout) {
        _requestTimeout = requestTimeout;
        
        // With the "always offer with a timeout" queue, the maximumPoolSize should always
        // be set to the same as the corePoolSize, as otherwise things get very inefficient
        // since each execute() call will will delay by <requestTimeout> even if we could add more
        // threads. And since these two values are the same, the keepAliveTime value has
        // no meaning.
        
        BlockingQueue<Runnable> queue = new MyBlockingQueue<Runnable>();
        _pool = new ThreadPoolExecutor(numThreads, numThreads, Long.MAX_VALUE, TimeUnit.MILLISECONDS, queue);
    }
    
    /**
     * Execute <command> using the thread pool.
     * 
     * @param command
     * @throws RejectedExecutionException
     */
    public void execute(Runnable command) throws RejectedExecutionException {
        _pool.execute(command);
    }
    
    
    /**
     * Return number of active threads
     * 
     * @return count of active threads
     */
    public int getActiveCount() {
        return _pool.getActiveCount();
    }
    
    /**
     * Terminate the thread pool.
     * 
     * @return true if we did a normal termination, false if we had to do a hard shutdown
     * @throws InterruptedException 
     */
    public boolean terminate(int duration, TimeUnit timeUnit) throws InterruptedException {
        
        // First just wait for threads to terminate naturally.
        _pool.shutdown();
        if (_pool.awaitTermination(duration, timeUnit)) {
            return true;
        }
        
        // We need to do a hard shutdown
        List<Runnable> remainingTasks = _pool.shutdownNow();
        if (remainingTasks.size() != 0) {
            // Houston, we have a problem. Since ThreadedExecutor isn't multi-threaded, we should
            // never hit the one edge case where this _might_ be true (execute was called, waiting
            // for a thread to terminate, and then this terminate was called).
            throw new RuntimeException("There should never be any tasks in the queue");
        }
        
        return false;
    }

}
