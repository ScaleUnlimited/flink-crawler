package com.scaleunlimited.flinkcrawler.functions;

import static org.junit.Assert.assertEquals;
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
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.config.CrawlTerminator;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.DomainScore;
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

    private static final int NUM_INPUT_URLS = 50;
    private static final int MAX_PARALLELISM = 10;

    ManualCrawlTerminator _terminator;
    PldKeySelector<CrawlStateUrl> _pldKeySelector;
    DomainScoreKeySelector _domainScoreKeySelector;
    KeyedTwoInputStreamOperatorTestHarness<String, CrawlStateUrl, DomainScore, FetchUrl> _testHarnesses[];

    @Before
    public void setUp() throws Exception {
        _pldKeySelector = new PldKeySelector<CrawlStateUrl>();
        _domainScoreKeySelector = new DomainScoreKeySelector();
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
        addProcessingTime(UrlDBFunction.MAX_DOMAIN_CHECK_INTERVAL);
        
        // Grab the status update (UNFETCHED->FETCHING) CrawlStateUrls from the side channel,
        // make sure they refer to the same set of input URLs, and feed them back into processUrls()
        // to force UrlDBFunction to update their states in the URL DB.
        List<CrawlStateUrl> fetchingUrls = getStatusUpdateUrls(0);
        checkFetchingUrls(inputUrls, fetchingUrls);
        processUrls(fetchingUrls);
        
        // Ensure that UrlDBFunction outputs the same set of FetchUrls as a result.
        // Pretend to fetch the URLs by building FETCHED CrawlStateUrls from them 
        // and feeding those back into processUrls() to update their states in the URL DB.
        List<FetchUrl> outputUrls = getOutputUrls(0);
        checkOutputUrls(inputUrls, outputUrls);
        List<CrawlStateUrl> fetchedUrls = makeFetchedUrls(outputUrls);
        processUrls(fetchedUrls);
        
        // UrlDBFunction should not output any additional URLs or generate any side channel
        // status updates as a result, so those Tuple lists should not have increased in size.
        assertEquals(fetchingUrls.size(), getStatusUpdateUrls(0).size());
        assertEquals(outputUrls.size(), getOutputUrls(0).size());
        
        // Note: Our test harness employs a ReFetchingQueue that re-fetches any URLs in the DB
        // that haven't been fetched since the queue was constructed (along with the harness).
        // Therefore, constructing the (2-subtask) harness from the saved state of the (1-subtask)
        // harness should force all of the URLs in the DB to get fetched again.
        
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
        addProcessingTime(UrlDBFunction.MAX_DOMAIN_CHECK_INTERVAL);
        
        // Grab the status update (UNFETCHED->FETCHING) CrawlStateUrls from the side channel,
        // make sure they refer to the same set of input URLs, and feed them back into processUrls()
        // to force UrlDBFunction to update their states in the URL DB.
        fetchingUrls = getStatusUpdateUrls(0);
        fetchingUrls.addAll(getStatusUpdateUrls(1));
        checkFetchingUrls(inputUrls, fetchingUrls);
        processUrls(fetchingUrls);
        
        // Ensure that UrlDBFunction outputs the same set of FetchUrls as a result.
        // Pretend to fetch the URLs by building FETCHED CrawlStateUrls from them 
        // and feeding those back into processUrls() to update their states in the URL DB.
        outputUrls = getOutputUrls(0);
        outputUrls.addAll(getOutputUrls(1));
        checkOutputUrls(inputUrls, outputUrls);
        fetchedUrls = makeFetchedUrls(outputUrls);
        processUrls(fetchedUrls);
        
        // UrlDBFunction should not output any additional URLs or generate any side channel
        // status updates as a result, so those Tuple lists should not have increased in size.
        assertEquals(fetchingUrls.size(), (getStatusUpdateUrls(0).size() + getStatusUpdateUrls(1).size()));
        assertEquals(outputUrls.size(), (getOutputUrls(0).size() + getOutputUrls(1).size()));
        
        LOGGER.info("Terminating the crawl...");
        _terminator.terminate();
        closeTestHarnesses();
    }
    
    /**
     * @return UNFETCHED CrawlStateUrl Tuples to feed into UrlDBFunction
     * @throws MalformedURLException
     */
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

    /**
     * @param urls to partition and send to the appropriate instance of UrlDBFunction
     * via that partition's test harness
     * @throws Exception
     */
    private void processUrls(List<CrawlStateUrl> urls) throws Exception {
        int parallelism = _testHarnesses.length;
        for (CrawlStateUrl url : urls) {
            String key = _pldKeySelector.getKey(url);
            int subTaskIndex = FlinkUtils.getOperatorIndexForKey(key, MAX_PARALLELISM, parallelism);
            _testHarnesses[subTaskIndex].processElement1(new StreamRecord<>(url));
        }
    }

    /**
     * @param outputUrls from main output of UrlDBFunction that it wants to be fetched
     * @return FETCHED CrawlStateUrl Tuples that would result from such fetching
     */
    private List<CrawlStateUrl> makeFetchedUrls(List<FetchUrl> outputUrls) {
        List<CrawlStateUrl> result = new ArrayList<CrawlStateUrl>();
        for (FetchUrl outputUrl : outputUrls) {
            FetchResultUrl fetchedUrl = new FetchResultUrl(outputUrl, FetchStatus.FETCHED, System.currentTimeMillis());
            result.add(new CrawlStateUrl(fetchedUrl));
        }
        return result;
    }

    /**
     * Ensure that each URL in <code>inputUrls</code> resulted in exactly one UNFETCHED->FETCHING
     * status update
     * @param inputUrls that were fed into (all partitions of) UrlDBFunction
     * @param fetchingUrls that (all partitions of) UrlDBFunction sent to its side channel
     */
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

    /**
     * Ensure that each URL in <code>inputUrls</code> resulted in exactly one FetchUrl output
     * @param inputUrls that were fed into (all partitions of) UrlDBFunction
     * @param fetchingUrls that (all partitions of) UrlDBFunction sent to its side channel
     */
    private void checkOutputUrls(List<CrawlStateUrl> inputUrls, List<FetchUrl> outputUrls) {
        assertEquals(inputUrls.size(), outputUrls.size());
        Set<String> expectedUrlStrings = new HashSet<String>();
        for (CrawlStateUrl inputUrl : inputUrls) {
            expectedUrlStrings.add(inputUrl.getUrl());
        }
        for (FetchUrl outputUrl : outputUrls) {
            assertTrue(expectedUrlStrings.remove(outputUrl.getUrl()));
        }
    }

    /**
     * @param subTaskIndex of test harness partition
     * @return All CrawlStateUrl Tuples that the <code>subTaskIndex</code> instance of
     * UrlDBFunction sent to its side channel since its test harness was constructed
     */
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
    
    /**
     * @param subTaskIndex of test harness partition
     * @return All FetchUrl Tuples that the <code>subTaskIndex</code> instance of
     * UrlDBFunction has output since its test harness was constructed
     */
    @SuppressWarnings("unchecked")
    private List<FetchUrl> getOutputUrls(int subTaskIndex) {
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
    
    // Methods to manipulate the processing time on which our timers are based
    
    private void setProcessingTime(long newTime) throws Exception {
        for (int subTaskIndex = 0; subTaskIndex < _testHarnesses.length; subTaskIndex++) {
            KeyedTwoInputStreamOperatorTestHarness<String, CrawlStateUrl, DomainScore, FetchUrl> testHarness = 
                _testHarnesses[subTaskIndex];
            LOGGER.debug("Processing time is now {} for subTask {}", newTime, subTaskIndex);
            testHarness.setProcessingTime(newTime);
        }
    }
        
    private void addProcessingTime(long extraTime) throws Exception {
        for (int subTaskIndex = 0; subTaskIndex < _testHarnesses.length; subTaskIndex++) {
            KeyedTwoInputStreamOperatorTestHarness<String, CrawlStateUrl, DomainScore, FetchUrl> testHarness = 
                _testHarnesses[subTaskIndex];
            long newTime = testHarness.getProcessingTime() + extraTime;
            LOGGER.debug("Processing time is now {} for subTask {}", newTime, subTaskIndex);
            testHarness.setProcessingTime(newTime);
        }
    }
    
    // Methods to manipulate the test harness in which UrlDBFunction executes
    
    private KeyedTwoInputStreamOperatorTestHarness<String, CrawlStateUrl, DomainScore, FetchUrl>[] makeTestHarnesses(
        int parallelism, 
        OperatorSubtaskState savedState)
        throws Exception {
        
        @SuppressWarnings("unchecked")
        KeyedTwoInputStreamOperatorTestHarness<String, CrawlStateUrl, DomainScore, FetchUrl> result[] = 
            new KeyedTwoInputStreamOperatorTestHarness[parallelism];
        
        _terminator = new ManualCrawlTerminator();
        for (int i = 0; i < parallelism; i++) {
            result[i] = makeTestHarness(parallelism, i, savedState);
        }
        return result;
    }
    
    private KeyedTwoInputStreamOperatorTestHarness<String, CrawlStateUrl, DomainScore, FetchUrl> makeTestHarness(
        int parallelism, 
        int subTaskIndex, 
        OperatorSubtaskState savedState)
        throws Exception {
        
        BaseUrlStateMerger merger = new DefaultUrlStateMerger();
        FetchQueue fetchQueue = new ReFetchingQueue();
        KeyedCoProcessOperator<String, CrawlStateUrl, DomainScore, FetchUrl> operator = 
            new KeyedCoProcessOperator<>(new UrlDBFunction(_terminator, merger, fetchQueue));
        KeyedTwoInputStreamOperatorTestHarness<String, CrawlStateUrl, DomainScore, FetchUrl> result =
            new KeyedTwoInputStreamOperatorTestHarness<>(
                    operator,
                    _pldKeySelector,
                    _domainScoreKeySelector, 
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
    
    private void closeTestHarnesses() throws Exception {
        for (KeyedTwoInputStreamOperatorTestHarness<String, CrawlStateUrl, DomainScore, FetchUrl> harness : _testHarnesses) {
            harness.close();
        }
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
