package com.scaleunlimited.flinkcrawler.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// TODO Use a double for mass?

@SuppressWarnings("serial")
public class DomainRankFunction 
	extends BaseCoFlatMapFunction<	Tuple2<String, String>, Tuple2<String, Float>, 
									Tuple2<String, Float>> {
	private static final String FAKE_EPSILON_MASS_DOMAIN_PREFIX = "Epsilon-mass:";
	private static final int MAX_SOURCE_DOMAIN_UPDATE_SIZE = 2;
	private static final float OUTGOING_EPSILON_MASS_FRACTION = 0.15f;
	
	private static class DomainInfo {
		private float _mass;
		private Set<String> _targetDomains; // TODO Let this be null?
		
		public DomainInfo(String firstTargetDomain) {
			this(0.0f);
			addTargetDomain(firstTargetDomain);
		}
		
		public DomainInfo(float initialMass) {
			super();
			_mass = initialMass;
			_targetDomains = new HashSet<String>();
		}

		public float getMass() {
			return _mass;
		}

		public Set<String> getTargetDomains() {
			return _targetDomains;
		}

		public float addMass(float extraMass) {
			_mass += extraMass;
			return _mass;
		}
		
		public void addTargetDomain(String targetDomain) {
			_targetDomains.add(targetDomain);
		}
	}

	private Map<String, DomainInfo> _sourceDomainMap;
	private List<DomainInfo> _sourceDomainList;
	private int _sourceDomainIndex;
	private float _pendingEpsilonMass;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		_sourceDomainMap = new HashMap<String, DomainInfo>();
		_sourceDomainList = new ArrayList<DomainInfo>();
		_sourceDomainIndex = -1;
		_pendingEpsilonMass = 1.0f / getNumParitions();
	}

	@Override
	public void flatMap1(	Tuple2<String, String> domainLink,
							Collector<Tuple2<String, Float>> collector)
			throws Exception {
		
		// We have a new sourceDomain -> targetDomain link, so make sure
		// sourceDomain has an entry in both sourceDomainMap and sourceDomainList
		// and that this entry's target domain set includes targetDomain.
		String sourceDomain = domainLink.getField(0);
		String targetDomain = domainLink.getField(1);
		DomainInfo sourceDomainInfo = _sourceDomainMap.get(sourceDomain);
		if (sourceDomainInfo == null) {
			sourceDomainInfo = new DomainInfo(targetDomain);
			_sourceDomainMap.put(sourceDomain, sourceDomainInfo);
			_sourceDomainList.add(sourceDomainInfo);
		} else {
			sourceDomainInfo.addTargetDomain(targetDomain);
		}
		
		// We never generate any output Tuples from this map
	}

	@Override
	public void flatMap2(	Tuple2<String, Float> incomingMassTuple,
							Collector<Tuple2<String, Float>> collector)
			throws Exception {
		
		// If this is incoming epsilon mass, then add it to our unallocated
		// total and then update a few more source domains.
		if (isFakeEpsilonMassTuple(incomingMassTuple)) {
			float incomingEpsilonMass = incomingMassTuple.getField(1);
			_pendingEpsilonMass += incomingEpsilonMass;
			updateSomeSourceDomains(collector);
			
		// Otherwise, it's mass for one of our source domains, so add it in.
		} else {
			String sourceDomain = incomingMassTuple.getField(0);
			float extraSourceDomainMass = incomingMassTuple.getField(1);
			DomainInfo sourceDomainInfo =
				_sourceDomainMap.get(sourceDomain);
			if (sourceDomainInfo == null) {
				sourceDomainInfo = new DomainInfo(extraSourceDomainMass);
				_sourceDomainMap.put(sourceDomain, sourceDomainInfo);
				_sourceDomainList.add(sourceDomainInfo);
			} else {
				sourceDomainInfo.addMass(extraSourceDomainMass);
			}
		}
	}

	public boolean isFakeEpsilonMassTuple(Tuple2<String, Float> incomingMassTuple) {
		String sourceDomain = incomingMassTuple.getField(0);
		return isFakeEpsilonDomain(sourceDomain);
	}

	public boolean isFakeEpsilonDomain(String domain) {
		return domain.startsWith(FAKE_EPSILON_MASS_DOMAIN_PREFIX);
	}

	private void updateSomeSourceDomains(Collector<Tuple2<String, Float>> collector) {
		// TODO Synchronize access to _sourceDomainMap, etc.
		int numSourceDomains = _sourceDomainList.size();
		int numSourceDomainsToUpdate = 
			Math.min(numSourceDomains, MAX_SOURCE_DOMAIN_UPDATE_SIZE);
		for (int i = 0; i < numSourceDomainsToUpdate; i++) {
			updateNextSourceDomain(collector);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void updateNextSourceDomain(Collector<Tuple2<String, Float>> collector) {
		
		// Grab the next source domain (and move our index beyond it)
		DomainInfo sourceDomainInfo = 
			_sourceDomainList.get(_sourceDomainIndex++);
		int numSourceDomains = _sourceDomainList.size();
		if (_sourceDomainIndex >= numSourceDomains) {
			_sourceDomainIndex = 0;
		}
		
		// Add in its share of the pending epsilon mass
		float extraSourceDomainMass = _pendingEpsilonMass / numSourceDomains;
		_pendingEpsilonMass -= extraSourceDomainMass;
		sourceDomainInfo.addMass(extraSourceDomainMass);
		
		// Transfer most of its new total mass to its target domains
		Set<String> targetDomains = sourceDomainInfo.getTargetDomains();
		if	(	(targetDomains != null)
			&&	!(targetDomains.isEmpty())) {
			float extraMassPerTargetDomain =
					(	(	sourceDomainInfo.getMass() 
						* 	(1.0f - OUTGOING_EPSILON_MASS_FRACTION)) 
					/ 	targetDomains.size());
			for (String targetDomain : targetDomains) {
				
				// TODO If it's not in my partition, emit a Tuple instead
				DomainInfo targetDomainInfo = _sourceDomainMap.get(targetDomain);
				sourceDomainInfo.addMass(-extraSourceDomainMass);
				targetDomainInfo.addMass(extraMassPerTargetDomain);
			}
		}
		
		// Transfer the remainder to our output of one epsilon mass Tuple per 
		// partition
		int numPartitions = getNumParitions();
		float extraMassPerPartition =
			sourceDomainInfo.getMass() / numPartitions;
		for (int i = 0; i < numPartitions; i++) {
			sourceDomainInfo.addMass(-extraMassPerPartition);
			collector.collect(new Tuple2(	makeFakeEpsilonDomain(i), 
											extraMassPerPartition));
		}
	}

	private String makeFakeEpsilonDomain(int i) {
		// TODO We need to ensure that our partitioner will handle these keys
		// appropriately.
		return FAKE_EPSILON_MASS_DOMAIN_PREFIX + i;
	}

	private int getNumParitions() {
		
		// TODO Is this right?
		return _parallelism;
	}
}
