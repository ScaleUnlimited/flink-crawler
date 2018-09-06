package com.scaleunlimited.flinkcrawler.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

//    private static final int NUM_INPUT_DOMAINS = 50;
    private static final int NUM_INPUT_DOMAINS = 5;
    private static final String HIGH_SCORING_DOMAIN = makeDomain(NUM_INPUT_DOMAINS / 2);
    private static final float HIGH_DOMAIN_SCORE_RATIO = 10.f;
    private static final float DEFAULT_DOMAIN_SCORE = 1.0f / (HIGH_DOMAIN_SCORE_RATIO + NUM_INPUT_DOMAINS - 1);
    private static final float HIGH_DOMAIN_SCORE = HIGH_DOMAIN_SCORE_RATIO * DEFAULT_DOMAIN_SCORE;
    private static final int PAGES_PER_DOMAIN = 10;
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
        
        // Send a high DomainScore in for one of the domains (others will continue to fetch
        // only a single URL per MAX_DOMAIN_CHECK_INTERVAL)
        List<DomainScore> domainScores = new ArrayList<DomainScore>();
        for (int i = 0; i < NUM_INPUT_DOMAINS; i++) {
            String domain = makeDomain(i);
            float score = (domain.equals(HIGH_SCORING_DOMAIN)) ? HIGH_DOMAIN_SCORE : DEFAULT_DOMAIN_SCORE;
            domainScores.add(new DomainScore(domain, score));
        }
        updateDomainScores(domainScores);
        
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
        
        
        LOGGER.info("Re-fetching those same URLs a second time (to restore their domain scores)...");
        
        // Ensure that timerService has had enough time to call UrlDBFunction.onTimer()
        // enough times that all URLs get added to the fetch queue and then emitted
        // via the side channel as (UNFETCHED->FETCHING) CrawlStateUrls.
        addProcessingTime(UrlDBFunction.MAX_DOMAIN_CHECK_INTERVAL);
        
        // Grab the status update (FETCHED->FETCHING) CrawlStateUrls from the side channel,
        // make sure they refer to the same set of input URLs, and feed them back into processUrls()
        // to force UrlDBFunction to update their states in the URL DB.
        List<CrawlStateUrl> fetchingUrls0 = getStatusUpdateUrls(0);
        List<CrawlStateUrl> fetchingUrls1 = getStatusUpdateUrls(1);
        fetchingUrls.clear();
        fetchingUrls.addAll(fetchingUrls0);
        fetchingUrls.addAll(fetchingUrls1);
        processUrls(fetchingUrls);
        
        // Ensure that UrlDBFunction outputs the same set of FetchUrls as a result.
        // Pretend to fetch the URLs by building FETCHED CrawlStateUrls from them 
        // and feeding those back into processUrls() to update their states in the URL DB.
        List<FetchUrl> outputUrls0 = getOutputUrls(0);
        List<FetchUrl> outputUrls1 = getOutputUrls(1);
        outputUrls.clear();
        outputUrls.addAll(outputUrls0);
        outputUrls.addAll(outputUrls1);
        fetchedUrls = makeFetchedUrls(outputUrls);
        processUrls(fetchedUrls);
        
        LOGGER.info("Re-fetching those same URLs a third time...");
        
        // Ensure that timerService has had enough time to call UrlDBFunction.onTimer()
        // enough times that all URLs get added to the fetch queue and then emitted
        // via the side channel as (FETCHED->FETCHING & UNFETCHED->FETCHING) CrawlStateUrls.
        // Here we divide up MAX_DOMAIN_CHECK_INTERVAL into HIGH_DOMAIN_SCORE_RATIO increments, 
        // since there should be one URL emitted in each of these for HIGH_SCORING_DOMAIN.
        int numSteps = (int)(HIGH_DOMAIN_SCORE_RATIO);
        long stepTime = UrlDBFunction.MAX_DOMAIN_CHECK_INTERVAL / numSteps;
        for (int i = 0; i < numSteps; i++) {
            addProcessingTime(stepTime);
        }
        addProcessingTime(UrlDBFunction.MAX_DOMAIN_CHECK_INTERVAL - (stepTime * numSteps));
        
        // Grab the (additional) status update (FETCHED->FETCHING) CrawlStateUrls from the side channel,
        // make sure they refer to the same set of input URLs, and feed them back into processUrls()
        // to force UrlDBFunction to update their states in the URL DB.
        fetchingUrls0 = getStatusUpdateUrls(0, fetchingUrls0);
        fetchingUrls1 = getStatusUpdateUrls(1, fetchingUrls1);
        fetchingUrls.clear();
        fetchingUrls.addAll(fetchingUrls0);
        fetchingUrls.addAll(fetchingUrls1);
        
        Map<String, Integer> expectedUrlsPerPld = new HashMap<String, Integer>();
        for (CrawlStateUrl inputUrl : inputUrls) {
            String domain = inputUrl.getPld();
            expectedUrlsPerPld.put(domain, domain.equals(HIGH_SCORING_DOMAIN) ? 10 : 1);
        }
        checkFetchingUrls(expectedUrlsPerPld, fetchingUrls);
        processUrls(fetchingUrls);
        
        // Ensure that UrlDBFunction outputs another copy of the same set of FetchUrls as a result.
        // Pretend to fetch the URLs by building FETCHED CrawlStateUrls from them 
        // and feeding those back into processUrls() to update their states in the URL DB.
        outputUrls0 = getOutputUrls(0, outputUrls0);
        outputUrls1 = getOutputUrls(1, outputUrls1);
        outputUrls.clear();
        outputUrls.addAll(outputUrls0);
        outputUrls.addAll(outputUrls1);
        checkOutputUrls(expectedUrlsPerPld, outputUrls);
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
        ArrayList<CrawlStateUrl> result = new ArrayList<CrawlStateUrl>();
        for (int i = 0; i < PAGES_PER_DOMAIN; i++) {
            result.addAll(makeInputUrls(i));
        }
        return result;
    }
    
    private ArrayList<CrawlStateUrl> makeInputUrls(int pageIndex) throws MalformedURLException {
        ArrayList<CrawlStateUrl> result = new ArrayList<CrawlStateUrl>();
        for (int i = 0; i < NUM_INPUT_DOMAINS; i++) {
            String domain = makeDomain(i);
            String urlString = String.format("https://%s/page%d", domain, pageIndex);
            CrawlStateUrl url = new CrawlStateUrl(new RawUrl(urlString));
            result.add(url);
        }
        return result;
    }
    
    private static String makeDomain(int domainIndex) {
        return String.format("domain-%d.com", domainIndex);
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
     * @param domainScores to partition and send to the appropriate instance of UrlDBFunction
     * via that partition's test harness
     * @throws Exception
     */
    private void updateDomainScores(List<DomainScore> domainScores) throws Exception {
        int parallelism = _testHarnesses.length;
        for (DomainScore domainScore : domainScores) {
            String key = _domainScoreKeySelector.getKey(domainScore);
            int subTaskIndex = FlinkUtils.getOperatorIndexForKey(key, MAX_PARALLELISM, parallelism);
            _testHarnesses[subTaskIndex].processElement2(new StreamRecord<>(domainScore));
        }
    }

    /**
     * @param outputUrls from main output of UrlDBFunction that it wants to be fetched
     * @return FETCHED CrawlStateUrl Tuples that would result from such fetching
     */
    private List<CrawlStateUrl> makeFetchedUrls(List<FetchUrl> outputUrls) {
        List<CrawlStateUrl> result = new ArrayList<CrawlStateUrl>();
        for (FetchUrl outputUrl : outputUrls) {
            FetchResultUrl fetchedUrl = 
                new FetchResultUrl(outputUrl, FetchStatus.FETCHED, System.currentTimeMillis());
            result.add(new CrawlStateUrl(fetchedUrl));
        }
        return result;
    }

    /**
     * Ensure that each PLD in <code>inputUrls</code> resulted in exactly one UNFETCHED->FETCHING
     * status update for a URL in that PLD
     * @param inputUrls that were fed into (all partitions of) UrlDBFunction
     * @param fetchingUrls that (all partitions of) UrlDBFunction sent to its side channel
     */
    private void checkFetchingUrls(List<CrawlStateUrl> inputUrls, List<CrawlStateUrl> fetchingUrls) {
        Map<String, Integer> expectedUrlsPerPld = new HashMap<String, Integer>();
        for (CrawlStateUrl inputUrl : inputUrls) {
            expectedUrlsPerPld.put(inputUrl.getPld(), 1);
        }
        checkFetchingUrls(expectedUrlsPerPld, fetchingUrls);
    }

    private void checkFetchingUrls( Map<String, Integer> expectedUrlsPerPld,
                                    List<CrawlStateUrl> fetchingUrls) {
        for (CrawlStateUrl fetchingUrl : fetchingUrls) {
            String pld = fetchingUrl.getPld();
            Integer numStillExpected = expectedUrlsPerPld.get(pld);
            String message = String.format("Wasn't expecting any (more) FETCHING URLs for %s but got %s", pld, fetchingUrl.getUrl());
            assertNotNull(message, numStillExpected);
            if (--numStillExpected > 0) {
                expectedUrlsPerPld.put(pld, numStillExpected);
            } else {
                expectedUrlsPerPld.remove(pld);
            }
            message = String.format("Expected status update for %s to FETCHING (not %s)", 
                                    fetchingUrl.getUrl(),
                                    fetchingUrl.getStatus());
            assertEquals(FetchStatus.FETCHING, fetchingUrl.getStatus());
        }
        if (!(expectedUrlsPerPld.isEmpty())) {
            String unfinishedPld = expectedUrlsPerPld.keySet().iterator().next();
            String message = 
                String.format(  "Expected status update to FETCHING for URLs from %d more PLDs (including %d from %s)",
                                expectedUrlsPerPld.size(),
                                expectedUrlsPerPld.get(unfinishedPld),
                                unfinishedPld);
            fail(message);
        }
    }

    /**
     * Ensure that each PLD in <code>inputUrls</code> resulted in exactly one FetchUrl output
     * @param inputUrls that were fed into (all partitions of) UrlDBFunction
     * @param fetchingUrls that (all partitions of) UrlDBFunction sent to its side channel
     */
    private void checkOutputUrls(List<CrawlStateUrl> inputUrls, List<FetchUrl> outputUrls) {
        Map<String, Integer> expectedUrlsPerPld = new HashMap<String, Integer>();
        for (CrawlStateUrl inputUrl : inputUrls) {
            expectedUrlsPerPld.put(inputUrl.getPld(), 1);
        }
        checkOutputUrls(expectedUrlsPerPld, outputUrls);
    }

    private void checkOutputUrls(Map<String, Integer> expectedUrlsPerPld, List<FetchUrl> outputUrls) {
        for (FetchUrl outputUrl : outputUrls) {
            String pld = outputUrl.getPld();
            Integer numStillExpected = expectedUrlsPerPld.get(pld);
            String message = String.format("Wasn't expecting any (more) output URLs for %s but got %s", pld, outputUrl.getUrl());
            assertNotNull(message, numStillExpected);
            if (--numStillExpected > 0) {
                expectedUrlsPerPld.put(pld, numStillExpected);
            } else {
                expectedUrlsPerPld.remove(pld);
            }
        }
        if (!(expectedUrlsPerPld.isEmpty())) {
            String unfinishedPld = expectedUrlsPerPld.keySet().iterator().next();
            String message = 
                String.format(  "Expected %d more URLs to be output from %d more PLDs (including %d from %s)",
                                expectedUrlsPerPld.size(),
                                expectedUrlsPerPld.get(unfinishedPld),
                                unfinishedPld);
            fail(message);
        }
    }

    /**
     * @see #getStatusUpdateUrls(int, List)
     */
    private List<CrawlStateUrl> getStatusUpdateUrls(int subTaskIndex) {
        return getStatusUpdateUrls(subTaskIndex, new ArrayList<CrawlStateUrl>());
    }
    
    /**
     * @param subTaskIndex of test harness partition
     * @param alreadySeen Tuples that we've already grabbed from the test harness
     * and should therefore be excluded from the result
     * @return All CrawlStateUrl Tuples that the <code>subTaskIndex</code> instance of
     * UrlDBFunction sent to its side channel since its test harness was constructed
     */
    private List<CrawlStateUrl> getStatusUpdateUrls(int subTaskIndex, List<CrawlStateUrl> alreadySeen) {
        ConcurrentLinkedQueue<StreamRecord<CrawlStateUrl>> recordQueue =
            _testHarnesses[subTaskIndex].getSideOutput(UrlDBFunction.STATUS_OUTPUT_TAG);
        Iterator<StreamRecord<CrawlStateUrl>> iterator = recordQueue.iterator();
        List<CrawlStateUrl> result = new ArrayList<CrawlStateUrl>();
        int numAlreadySeen = alreadySeen.size();
        while (iterator.hasNext()) {
            if (numAlreadySeen-- <= 0) {
                CrawlStateUrl url = iterator.next().getValue();
                result.add(url);
            }
        }
        return result;
    }
    
    /**
     * @see #getOutputUrls(int, List)
     */
    private List<FetchUrl> getOutputUrls(int subTaskIndex) {
        return getOutputUrls(subTaskIndex, new ArrayList<FetchUrl>());
    }

    /**
     * @param subTaskIndex of test harness partition
     * @param alreadySeen Tuples that we've already grabbed from the test harness
     * and should therefore be excluded from the result
     * @return All FetchUrl Tuples that the <code>subTaskIndex</code> instance of
     * UrlDBFunction has output since its test harness was constructed
     */
    @SuppressWarnings("unchecked")
    private List<FetchUrl> getOutputUrls(int subTaskIndex, List<FetchUrl> alreadySeen) {
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
            super(NUM_INPUT_DOMAINS);
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
