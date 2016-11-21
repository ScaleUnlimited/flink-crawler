package com.scaleunlimited.flinkcrawler.robots;

import java.io.Serializable;

import crawlercommons.robots.BaseRobotRules;

/**
 * TODO who maintains the mapping from (case-sensitive) domain to rules?
 * And who decides when we need to re-fetch the rules?
 * 
 * TODO who handles sitemap processing? 
 *
 */
@SuppressWarnings("serial")
public abstract class BaseRobotsParser implements Serializable {

	public abstract BaseRobotRules parse(String domain, byte[] content, String contentType, String robotNames);
	
}
