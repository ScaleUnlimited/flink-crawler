package com.scaleunlimited.flinkcrawler.robots;

import com.scaleunlimited.flinkcrawler.fetcher.BaseFetcher;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;

@SuppressWarnings("serial")
public class SimpleRobotsParser extends BaseRobotsParser {

	SimpleRobotRulesParser _parser;
	
	public SimpleRobotsParser() {
		_parser = new SimpleRobotRulesParser();
	}

	@Override
	public BaseRobotRules parse(String domain, byte[] content, String contentType, String robotNames) {
		// TODO double-check this actually works.
		return _parser.parseContent(domain, content, contentType, robotNames);
	}
	
	

}
