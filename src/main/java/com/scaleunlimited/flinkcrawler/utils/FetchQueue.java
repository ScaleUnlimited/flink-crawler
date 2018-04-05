package com.scaleunlimited.flinkcrawler.utils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public class FetchQueue implements Serializable {

    private int _maxQueueSize;
    private float _minFetchScore;
    
    private transient LinkedList<CrawlStateUrl> _fetchQueue;
    
    public FetchQueue(int maxQueueSize) {
        this(maxQueueSize, 0.0f);
    }

    public FetchQueue(int maxQueueSize, float minFetchScore) {
        _maxQueueSize = maxQueueSize;
        _minFetchScore = minFetchScore;
    }

    /**
     * Lifecycle management - called once we're deployed.
     */
    public void open() {
        _fetchQueue = new LinkedList<>();
    }

    public boolean isEmpty() {
        return _fetchQueue.isEmpty();
    }

    
    /**
     * Add a new URL to the queue. If there's room, we just add it, otherwise
     * we compare scores and only add it if it's got a higher score than the
     * lowest-scoring URL currently in the queue. In either of the "no space"
     * cases, we return back the URL that we're rejecting (either the URL
     * being passed in, or the lower-scoring URL we're removing from the
     * queue to make space for it)
     * 
     * @param url URL to be added
     * @return URL that we're rejecting or removing from the queue (or null
     * if there's enough space in the queue)
     */
    public CrawlStateUrl add(CrawlStateUrl url) {
        if (url.getScore() < _minFetchScore) {
            return url;
        } else if (url.getStatus() != FetchStatus.UNFETCHED) {
            // TODO refetch URL if fetch time is earlier than "now".
            return url;
        } else if (_fetchQueue.size() < _maxQueueSize) {
            _fetchQueue.add(url);
            sortQueue();
            
            return null;
        } else if (url.getScore() <= _fetchQueue.getLast().getScore()) {
            return url;
        } else {
            _fetchQueue.add(url);
            sortQueue();
            return _fetchQueue.removeLast();
        }
    }

    private void sortQueue() {
        _fetchQueue.sort(new Comparator<CrawlStateUrl>() {

            @Override
            public int compare(CrawlStateUrl o1, CrawlStateUrl o2) {
                float o1Score = o1.getScore();
                float o2Score = o2.getScore();
                
                // Do reverse order sorting, where highest score is
                // at the front of the list (so what pop() returns).
                if (o1Score > o2Score) {
                    return -1;
                } else if (o1Score < o2Score) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
    }

    public FetchUrl poll() {
        CrawlStateUrl url = _fetchQueue.poll();
        if (url == null) {
            return null;
        } else {
            return new FetchUrl(url, url.getScore());
        }
    }

    public int size() {
        return _fetchQueue.size();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_fetchQueue == null) ? 0 : _fetchQueue.hashCode());
        result = prime * result + _maxQueueSize;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FetchQueue other = (FetchQueue) obj;
        if (_fetchQueue == null) {
            if (other._fetchQueue != null)
                return false;
        } else if (!_fetchQueue.equals(other._fetchQueue))
            return false;
        if (_maxQueueSize != other._maxQueueSize)
            return false;
        return true;
    }

}
