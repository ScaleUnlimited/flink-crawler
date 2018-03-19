package com.scaleunlimited.flinkcrawler.webgraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;

@SuppressWarnings("serial")
public class SimpleWebGraph extends BaseWebGraph {

    private Map<String, List<String>> _graph;
    private BaseUrlNormalizer _normalizer;

    public SimpleWebGraph(BaseUrlNormalizer normalizer) {
        this(normalizer, new ArrayList<String>());
    }

    /**
     * Given a list of text lines with the web graph, convert this into a web graph data structure.
     * 
     * The format of lines in the file are: <page URL><tab><outline><tab><outlink>...<return>
     *
     * Where each page can have 0..n outlinks. If a page has no outlinks, it just contains <page URL><tab>
     * 
     * Empty lines, or lines beginning with '#' are igored.
     * 
     * @param nodes
     */
    public SimpleWebGraph(BaseUrlNormalizer normalizer, List<String> nodes) {
        _normalizer = normalizer;
        _graph = new HashMap<>(nodes.size());

        for (String node : nodes) {
            node = node.trim();
            if (node.isEmpty() || node.startsWith("#")) {
                continue;
            }

            String pieces[] = node.split("\t");
            String parent = pieces[0];
            String[] children = Arrays.copyOfRange(pieces, 1, pieces.length);

            add(parent, children);
        }
    }

    public SimpleWebGraph add(String parent, String... children) {
        parent = normalize(parent);
        normalize(children);

        if (_graph.containsKey(parent)) {
            throw new IllegalArgumentException("Duplicate node name found: " + parent);
        }

        _graph.put(parent, Arrays.asList(children));
        return this;
    }

    protected String normalize(String url) {
        if (!url.startsWith("http")) {
            url = "http://" + url;
        }

        return _normalizer.normalize(url);
    }

    private void normalize(String[] urls) {
        for (int i = 0; i < urls.length; i++) {
            urls[i] = normalize(urls[i]);
        }
    }

    @Override
    public Iterator<String> getChildren(String parent) {
        List<String> children = _graph.get(parent);
        if (children == null) {
            throw new RuntimeException("Can't call getChildren on missing page");
        }

        return children.iterator();
    }

    @Override
    public boolean hasPage(String url) {
        return _graph.containsKey(url);
    }
}
