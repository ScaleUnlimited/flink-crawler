package com.scaleunlimited.flinkcrawler.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.config.CrawlTerminator;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.urldb.BaseUrlStateMerger;
import com.scaleunlimited.flinkcrawler.urldb.DefaultUrlStateMerger;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.FlinkUtils;

public class UrlDBFunctionTest {
    static final Logger LOGGER = LoggerFactory.getLogger(UrlDBFunctionTest.class);

//    private static final int NUM_INPUT_URLS = 50;
    private static final int NUM_INPUT_URLS = 5;
    private static final int MAX_PARALLELISM = 10;
    private static final long MAX_URL_PROCESS_TIME = 100L;

    ManualCrawlTerminator _terminator;
    PldKeySelector<CrawlStateUrl> _pldKeySelector;
    OneInputStreamOperatorTestHarness<CrawlStateUrl, FetchUrl> _testHarnesses[];

    @Before
    public void setUp() throws Exception {
        _pldKeySelector = new PldKeySelector<CrawlStateUrl>();
    }    
    
    @Test
    public void test() throws Throwable {
            
        LOGGER.info("Set up a parallelism of 1");
        _testHarnesses = makeTestHarnesses(1, null);
        
        setProcessingTime(0);
        
        LOGGER.info("Processing some input URLs...");
        List<CrawlStateUrl> inputUrls = makeInputUrls();
        processUrls(inputUrls);
        
        // Ensure that timerService has had enough time to call UrlDBFunction.onTimer()
        // enough times that all URLs get added to the fetch queue and then emitted
        // via the side channel as (UNFETCHED->FETCHING) CrawlStateUrls.
        addProcessingTime(MAX_URL_PROCESS_TIME);
        
        // Grab the status update (UNFETCHED->FETCHING) CrawlStateUrls from the side channel
        // and feed them back into processUrls()
        List<CrawlStateUrl> fetchingUrls = getStatusUpdateUrls(0);
        checkFetchingUrls(inputUrls, fetchingUrls);
        processUrls(fetchingUrls);
        
        // Grab the FetchUrls from the main output, build FETCHED CrawlStateUrls from them
        // and feed them back into processUrls()
        List<FetchUrl> outputUrls = getOutputUrls(0);
        List<CrawlStateUrl> fetchedUrls = makeFetchedUrls(outputUrls);
        processUrls(fetchedUrls);
        
        // Make sure there's no more records in either the side channel or the main output
        assertEquals(fetchingUrls.size(), getStatusUpdateUrls(0).size());
        assertEquals(outputUrls.size(), getOutputUrls(0).size());
        
        // TODO These expectations currently fail, since Timers don't
        // seem to be included in the snapshot, at least the one produced by
        // the test harness.
        
        LOGGER.info("Stop and then restart with parallelism of 2");
        OperatorSubtaskState savedState = _testHarnesses[0].snapshot(0L, 0L);
        long processingTime = _testHarnesses[0].getProcessingTime();
        closeTestHarnesses();
        _testHarnesses = makeTestHarnesses(2, savedState);
        
        setProcessingTime(processingTime);
        
        LOGGER.info("Re-fetching those same URLs...");
        
        // Ensure that timerService has had enough time to call UrlDBFunction.onTimer()
        // enough times that all URLs get added to the fetch queue and then emitted
        // via the side channel as (UNFETCHED->FETCHING) CrawlStateUrls.
        addProcessingTime(MAX_URL_PROCESS_TIME);
        
        // Grab the status update (UNFETCHED->FETCHING) CrawlStateUrls from the side channel
        // and feed them back into processUrls()
        fetchingUrls = getStatusUpdateUrls(0);
        fetchingUrls.addAll(getStatusUpdateUrls(1));
        checkFetchingUrls(inputUrls, fetchingUrls);
        processUrls(fetchingUrls);
        
        // Grab the FetchUrls from the main output, build FETCHED CrawlStateUrls from them
        // and feed them back into processUrls()
        outputUrls = getOutputUrls(0);
        outputUrls.addAll(getOutputUrls(1));
        fetchedUrls = makeFetchedUrls(outputUrls);
        processUrls(fetchedUrls);
        
        // Make sure there's no more records in either the side channel or the main output
        assertEquals(fetchingUrls.size(), (getStatusUpdateUrls(0).size() + getStatusUpdateUrls(1).size()));
        assertEquals(outputUrls.size(), (getOutputUrls(0).size() + getOutputUrls(1).size()));
        
        LOGGER.info("Terminating the crawl...");
        _terminator.terminate();
        closeTestHarnesses();
    }
    
    private void setProcessingTime(long newTime) throws Exception {
        for (int subTaskIndex = 0; subTaskIndex < _testHarnesses.length; subTaskIndex++) {
            OneInputStreamOperatorTestHarness<CrawlStateUrl, FetchUrl> testHarness = 
                _testHarnesses[subTaskIndex];
            LOGGER.debug("Processing time is now {} for subTask {}", newTime, subTaskIndex);
            testHarness.setProcessingTime(newTime);
        }
    }
        
    private void addProcessingTime(long extraTime) throws Exception {
        for (int subTaskIndex = 0; subTaskIndex < _testHarnesses.length; subTaskIndex++) {
            OneInputStreamOperatorTestHarness<CrawlStateUrl, FetchUrl> testHarness = 
                _testHarnesses[subTaskIndex];
            long newTime = testHarness.getProcessingTime() + extraTime;
            LOGGER.debug("Processing time is now {} for subTask {}", newTime, subTaskIndex);
            testHarness.setProcessingTime(newTime);
        }
    }
    
    private List<CrawlStateUrl> makeFetchedUrls(List<FetchUrl> outputUrls) {
        List<CrawlStateUrl> result = new ArrayList<CrawlStateUrl>();
        for (FetchUrl outputUrl : outputUrls) {
            FetchResultUrl fetchedUrl = new FetchResultUrl(outputUrl, FetchStatus.FETCHED, System.currentTimeMillis());
            result.add(new CrawlStateUrl(fetchedUrl));
        }
        return result;
    }

    private void checkFetchingUrls(List<CrawlStateUrl> inputUrls, List<CrawlStateUrl> fetchingUrls) {
        assertEquals(inputUrls.size(), fetchingUrls.size());
        Set<String> expectedUrlStrings = new HashSet<String>();
        for (CrawlStateUrl inputUrl : inputUrls) {
            expectedUrlStrings.add(inputUrl.getUrl());
        }
        for (CrawlStateUrl fetchingUrl : fetchingUrls) {
            assertTrue(expectedUrlStrings.remove(fetchingUrl.getUrl()));
            assertEquals(FetchStatus.FETCHING, fetchingUrl.getStatus());
        }
    }

    @SuppressWarnings("unused")
    private void checkNoOverlap(List<CrawlStateUrl> outputDomains0,
                                List<CrawlStateUrl> outputDomains1, 
                                List<CrawlStateUrl> inputUrls) {
        for (CrawlStateUrl outputDomain : outputDomains0) {
            String domainString = outputDomain.getPld();
            for (CrawlStateUrl otherDomain : outputDomains1) {
                assertFalse(domainString.equals(otherDomain.getPld()));
            }
            for (CrawlStateUrl inputUrl : inputUrls) {
                String inputDomainString = inputUrl.getPld();
                if (domainString.equals(inputDomainString)) {
                    inputUrls.remove(inputUrl);
                    break;
                }
            }
        }
    }

    private List<CrawlStateUrl> getStatusUpdateUrls(int subTaskIndex) {
        ConcurrentLinkedQueue<StreamRecord<CrawlStateUrl>> recordQueue =
            _testHarnesses[subTaskIndex].getSideOutput(UrlDBFunction.STATUS_OUTPUT_TAG);
        Iterator<StreamRecord<CrawlStateUrl>> iterator = recordQueue.iterator();
        List<CrawlStateUrl> result = new ArrayList<CrawlStateUrl>();
        while (iterator.hasNext()) {
            CrawlStateUrl url = iterator.next().getValue();
            result.add(url);
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    private List<FetchUrl> getOutputUrls(int subTaskIndex) {
        
        // TODO I'm not sure what Objects the iterator of the harness returns,
        // StreamRecord<FetchUrl>? FetchUrl? something else?
        ConcurrentLinkedQueue<Object> recordQueue =
            _testHarnesses[subTaskIndex].getOutput();
        List<FetchUrl> result = new ArrayList<FetchUrl>();
        Iterator<Object> iterator = recordQueue.iterator();
        while (iterator.hasNext()) {
            StreamRecord<FetchUrl> streamRecord = 
                (StreamRecord<FetchUrl>)(iterator.next());
            FetchUrl url = streamRecord.getValue();
            result.add(url);
        }
        return result;
    }
    
    private ArrayList<CrawlStateUrl> makeInputUrls() throws MalformedURLException {
        return makeInputUrls(1);
    }
    
    private ArrayList<CrawlStateUrl> makeInputUrls(int pageIndex) throws MalformedURLException {
        ArrayList<CrawlStateUrl> result = new ArrayList<CrawlStateUrl>();
        for (int i = 0; i < NUM_INPUT_URLS; i++) {
            String urlString = String.format("https://domain-%d.com/page%d", i, pageIndex);
            CrawlStateUrl url = new CrawlStateUrl(new RawUrl(urlString));
            result.add(url);
        }
        return result;
    }

    private void processUrls(List<CrawlStateUrl> urls) throws Exception {
        int parallelism = _testHarnesses.length;
        for (CrawlStateUrl url : urls) {
            String key = _pldKeySelector.getKey(url);
            int subTaskIndex = FlinkUtils.getOperatorIndexForKey(key, MAX_PARALLELISM, parallelism);
            _testHarnesses[subTaskIndex].processElement(new StreamRecord<>(url));
        }
    }

    private OneInputStreamOperatorTestHarness<CrawlStateUrl, FetchUrl>[] makeTestHarnesses(
        int parallelism, 
        OperatorSubtaskState savedState)
        throws Exception {
        
        @SuppressWarnings("unchecked")
        OneInputStreamOperatorTestHarness<CrawlStateUrl, FetchUrl> result[] = 
            new OneInputStreamOperatorTestHarness[parallelism];
        
        _terminator = new ManualCrawlTerminator();
        for (int i = 0; i < parallelism; i++) {
            result[i] = makeTestHarness(parallelism, i, savedState);
        }
        return result;
    }
    
    private void closeTestHarnesses() throws Exception {
        for (OneInputStreamOperatorTestHarness<CrawlStateUrl, FetchUrl> harness : _testHarnesses) {
            harness.close();
        }
    }

    private OneInputStreamOperatorTestHarness<CrawlStateUrl, FetchUrl> makeTestHarness(
        int parallelism, 
        int subTaskIndex, 
        OperatorSubtaskState savedState)
        throws Exception {
        
        BaseUrlStateMerger merger = new DefaultUrlStateMerger();
        FetchQueue fetchQueue = new ReFetchingQueue();
        KeyedProcessOperator<String, CrawlStateUrl, FetchUrl> operator = 
            new KeyedProcessOperator<>(new UrlDBFunction(_terminator, merger, fetchQueue));
        OneInputStreamOperatorTestHarness<CrawlStateUrl, FetchUrl> result =
            new KeyedOneInputStreamOperatorTestHarness<String, CrawlStateUrl, FetchUrl>(
                    operator,
                    _pldKeySelector,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    MAX_PARALLELISM,
                    parallelism,
                    subTaskIndex);
        result.setStateBackend(new MemoryStateBackend());
        result.setup();
        if (savedState != null) {
            result.initializeState(savedState);
        }
        result.open();
        return result;
    }
    
    /**
     * A FetchQueue that will just go ahead and re-fetch any URL
     * that hasn't been fetched since the queue was opened.
     */
    @SuppressWarnings("serial")
    private static class ReFetchingQueue extends FetchQueue {
        private long _openTime;

        public ReFetchingQueue() {
            super(NUM_INPUT_URLS);
        }

        @Override
        public void open() {
            super.open();
            _openTime = System.currentTimeMillis();
        }

        @Override
        public CrawlStateUrl add(CrawlStateUrl url) {
            if  (   (url.getStatus() == FetchStatus.FETCHED)
                &&  (url.getStatusTime() < _openTime)) {
                addToQueue(url);
            }
            return super.add(url);
        }
        
    }

    @SuppressWarnings("serial")
    private static class ManualCrawlTerminator extends CrawlTerminator {
        
        private boolean _isTerminated = false;
        
        public void terminate() {
            _isTerminated = true;
        }

        @Override
        public boolean isTerminated() {
            return _isTerminated;
        }
        
    }
}
