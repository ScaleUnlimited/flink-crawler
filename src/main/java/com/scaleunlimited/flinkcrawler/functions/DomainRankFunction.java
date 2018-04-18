package com.scaleunlimited.flinkcrawler.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class DomainRankFunction 
    extends BaseCoFlatMapFunction<Tuple2<String, String>, Tuple2<String, Double>, Tuple2<String, Double>> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(DomainRankFunction.class);

    private static final String FAKE_EPSILON_MASS_DOMAIN_PREFIX = "Epsilon-mass:";
    private static final double DEFAULT_OUTGOING_EPSILON_MASS_FRACTION = 0.15d;

    private static class DomainInfo {
        private double _mass;
        private Set<String> _targetDomains = null;
        private List<String> _targetDomainList = null;

        public DomainInfo(String firstTargetDomain) {
            this(0.0f);
            addTargetDomain(firstTargetDomain);
        }

        public DomainInfo(double initialMass) {
            super();
            _mass = initialMass;
        }

        public double getMass() {
            return _mass;
        }

        public String getTargetDomain(int index) {
            return _targetDomainList.get(index);
        }

        public Set<String> getTargetDomains() {
            return _targetDomains;
        }

        public double addMass(double extraMass) {
            _mass += extraMass;
            return _mass;
        }

        public void addTargetDomain(String targetDomain) {
            if (_targetDomains == null) {
                _targetDomains = new HashSet<String>();
                _targetDomainList = new ArrayList<String>();
            }
            if (_targetDomains.add(targetDomain)) {
                _targetDomainList.add(targetDomain);
            }
        }
    }

    private class ActiveMassDistribution {
        private int _targetDomainIndex = 0;
        private int _numTargetDomains;
        private int _partitionIndex = 0;
        private double _extraMassPerTargetDomain;
        private double _extraMassPerPartition;
        private DomainKeySelector _keySelector;
        private Partitioner<Integer> _partitioner;

        public ActiveMassDistribution() {
            super();
            int numSourceDomains = _sourceDomainList.size();
            if (numSourceDomains == 0) {
                throw new RuntimeException("Can't distribute mass since there are no source domains!");
            }
            if (_sourceDomainIndex >= numSourceDomains) {
                _sourceDomainIndex = 0;
            }
            Tuple2<String, DomainInfo> tuple = _sourceDomainList.get(_sourceDomainIndex);
            String sourceDomain = tuple.f0;
            DomainInfo sourceDomainInfo = tuple.f1;
            double epsilonMassToAdd = _pendingEpsilonMass / numSourceDomains;
            _pendingEpsilonMass -= epsilonMassToAdd;
            sourceDomainInfo.addMass(epsilonMassToAdd);
            double sourceMassToDistribute = sourceDomainInfo.getMass();
            if (sourceMassToDistribute < 0.000001d) {
                logIfDebug("%s has no significant mass to distribute", sourceDomain);
            }
            _numTargetDomains = sourceDomainInfo.getTargetDomains().size();
            _extraMassPerTargetDomain = (_numTargetDomains > 0) ? 
                    (   (   sourceMassToDistribute 
                        *   (1.0d - _outgoingEpsilonMassFraction)) 
                    /   _numTargetDomains)
                :   0.0d;
            int numPartitions = getNumPartitions();
            _extraMassPerPartition = 
                (   (   sourceMassToDistribute 
                    *   (   (_numTargetDomains > 0) ? 
                            _outgoingEpsilonMassFraction
                        :   1.0d)) 
                /   numPartitions);
            _keySelector = new DomainKeySelector();
            _partitioner = makePartitioner();
        }

        public Partitioner<Integer> makePartitioner() {
            return new HashPartitioner();
        }

        private boolean isCollectorBlocked() {
            return false;
        }

        private boolean isCollectorBlocked(int partition) {
            return false;
        }

        public boolean continueDistribution(Collector<Tuple2<String, Double>> collector) {

            // If the collector is blocked, just return true without doing
            // anything. We'll have to wait until at least our next invocation
            // to make any progress.
            //
            // TODO Dive in further and then call the partition-specific
            // isCollectorBlocked method instead?
            //
            if (isCollectorBlocked()) {
                return true;
            }

            // If this source domain had zero mass to distribute
            // (even after adding in its portin of the pending epsilon mass),
            // then we're already done with it.
            if  (   (_extraMassPerTargetDomain == 0.0d)
                &&  (_extraMassPerPartition == 0.0d)) {
                return false;
            }

            Tuple2<String, DomainInfo> tuple =
                _sourceDomainList.get(_sourceDomainIndex);
            DomainInfo sourceDomainInfo = tuple.f1;
            String sourceDomain = tuple.f0;

            // Most of the source domain's mass is distributed to its target
            // domains. Transfer the next target domain's portion unless we've
            // already handled them all.
            if (_targetDomainIndex < _numTargetDomains) {
                sourceDomainInfo.addMass(-_extraMassPerTargetDomain);
                String targetDomain = 
                    sourceDomainInfo.getTargetDomain(_targetDomainIndex++);

                // If the target domain is in my partition, we can just add the
                // mass to that element of the source domain map.
                if (isInMyPartition(targetDomain)) {
                    logIfDebug( "Distributing mass %f directly from source domain %s to target domain %s",
                                _extraMassPerTargetDomain, 
                                sourceDomain, 
                                targetDomain);
                    addSourceDomainMass(targetDomain, _extraMassPerTargetDomain);

                // Otherwise, output a Tuple to transfer the mass to the domain
                // which exists in some other partition.
                } else {
                    logIfDebug( "Distributing mass %f from source domain %s to target domain %s in partition %d",
                                _extraMassPerTargetDomain, 
                                sourceDomain, 
                                targetDomain,
                                getPartition(targetDomain));
                    Tuple2<String, Double> targetDomainTuple = 
                        new Tuple2<String, Double>( targetDomain,
                                                    _extraMassPerTargetDomain);
                    collector.collect(targetDomainTuple);
                }

            // We've already handled all of the target domains; the remainder
            // ("epsilon" mass) is distributed evenly among the partitions.
            // Output a Tuple with the next partition's portion unless we've
            // already handed them all as well.
            } else if (_partitionIndex < getNumPartitions()) {
                sourceDomainInfo.addMass(-_extraMassPerPartition);
                logIfDebug( "Distributing epsilon mass %f from source domain %s to partition %d",
                            _extraMassPerPartition, 
                            sourceDomain, 
                            _partitionIndex);
                Tuple2<String, Double> partitionTuple = 
                    new Tuple2<String, Double>( makeFakeEpsilonDomain(_partitionIndex++), 
                                                _extraMassPerPartition);
                collector.collect(partitionTuple);
                if (_partitionIndex >= getNumPartitions()) {
                    return false;
                }

                // We've already handled all of the epsilon mass too, so this source
                // domain's mass distribution is now complete.
            } else {
                return false;
            }

            // We're not done yet.
            return true;
        }

        private boolean isInMyPartition(String targetDomain) {
            return (getPartition(targetDomain) == _partition);
        }

        int getPartition(String targetDomain) {
            try {
                return _partitioner.partition(_keySelector.getKey(targetDomain), getNumPartitions());
            } catch (Exception e) {
                throw new RuntimeException(("Unable to get key for domain " + targetDomain), e);
            }
        }
    }

    // TODO Eventually we're going to want to use KeyedStreams, and Ken ran
    // into a problem where both 0 and 1 were getting assigned to the same
    // partition. How do we build a key that's guaranteed to be assigned to
    // the target partition?
    public static class DomainKeySelector 
        implements KeySelector<String, Integer> {

        @Override
        public Integer getKey(String domain) throws Exception {

            // We construct fake domains to help distribute epsilon mass evenly
            // among our key partitions. These take the following form:
            // "Epsilon-mass:<n>" where <n> is an int specifying the target
            // partition.
            if (domain.startsWith(FAKE_EPSILON_MASS_DOMAIN_PREFIX)) {
                String fields[] = domain.split(":", 3);
                return Integer.parseInt(fields[1]);
            }
            return domain.hashCode();
        }
    }

    public static class DomainLinkKeySelector 
        implements KeySelector<Tuple2<String, String>, Integer> {

        @Override
        public Integer getKey(Tuple2<String, String> domainLink) throws Exception {
            String sourceDomain = domainLink.f0;
            return sourceDomain.hashCode();
        }
    }

    public static class DomainMassKeySelector 
        implements KeySelector<Tuple2<String, Double>, Integer> {

        @Override
        public Integer getKey(Tuple2<String, Double> domainMass) throws Exception {
            String sourceDomain = domainMass.f0;
            return sourceDomain.hashCode();
        }
    }

    private Map<String, DomainInfo> _sourceDomainMap;
    private List<Tuple2<String, DomainInfo>> _sourceDomainList;
    private int _sourceDomainIndex;
    private double _pendingEpsilonMass;
    private double _outgoingEpsilonMassFraction = DEFAULT_OUTGOING_EPSILON_MASS_FRACTION;
    private ActiveMassDistribution _activeDistribution = null;

    public DomainRankFunction() {
        this(DEFAULT_OUTGOING_EPSILON_MASS_FRACTION);
    }

    public DomainRankFunction(double outgoingEpsilonMassFraction) {
        super();
        _outgoingEpsilonMassFraction = outgoingEpsilonMassFraction;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        _sourceDomainMap = new HashMap<String, DomainInfo>();
        _sourceDomainList = new ArrayList<Tuple2<String, DomainInfo>>();
        _sourceDomainIndex = 0;
        _pendingEpsilonMass = 1.0f / getNumPartitions();
    }

    @Override
    public void flatMap1(   Tuple2<String, String> domainLink, 
                            Collector<Tuple2<String, Double>> collector)
            throws Exception {

        String sourceDomain = domainLink.getField(0);
        String targetDomain = domainLink.getField(1);

        // We ignore all internal links.
        if (sourceDomain.equals(targetDomain)) {
            return;
        }

        logIfDebug( "Processing new domain link from %s to %s", 
                    sourceDomain, 
                    targetDomain);

        // We have a new sourceDomain -> targetDomain link, so make sure
        // sourceDomain has an entry in both sourceDomainMap and sourceDomainList
        // and that this entry's target domain set includes targetDomain.
        DomainInfo sourceDomainInfo = _sourceDomainMap.get(sourceDomain);
        if (sourceDomainInfo == null) {
            logIfDebug( "Adding new source domain %s with single link to %s", 
                        sourceDomain, 
                        targetDomain);
            sourceDomainInfo = new DomainInfo(targetDomain);
            _sourceDomainMap.put(sourceDomain, sourceDomainInfo);
            _sourceDomainList.add(new Tuple2<String, DomainInfo>(   sourceDomain,
                                                                    sourceDomainInfo));
        } else {
            sourceDomainInfo.addTargetDomain(targetDomain);
        }

        // We need to output something or flatMap2 will never get any input
        distributeMassIfPossible(collector);
    }

    @Override
    public void flatMap2(   Tuple2<String, Double> incomingMassTuple, 
                            Collector<Tuple2<String, Double>> collector)
            throws Exception {

        // If this is incoming epsilon mass, then add it to our unallocated
        // total and then try to distribute some mass.
        if (isFakeEpsilonMassTuple(incomingMassTuple)) {
            double incomingEpsilonMass = incomingMassTuple.getField(1);
            logIfDebug( "Distributing incoming epsilon mass %f to source domains", 
                        incomingEpsilonMass);
            _pendingEpsilonMass += incomingEpsilonMass;
            distributeMassIfPossible(collector);

            // Otherwise, it's mass for one of our source domains, so add it in.
            //
            // TODO Why not distribute some mass if possible for this case too?
            //
        } else {
            String sourceDomain = incomingMassTuple.getField(0);
            double extraSourceDomainMass = incomingMassTuple.getField(1);
            logIfDebug( "Adding incoming mass %f to source domain %s", 
                        extraSourceDomainMass, 
                        sourceDomain);
            addSourceDomainMass(sourceDomain, extraSourceDomainMass);
        }
    }

    public boolean isFakeEpsilonMassTuple(Tuple2<String, Double> incomingMassTuple) {
        String sourceDomain = incomingMassTuple.getField(0);
        return isFakeEpsilonDomain(sourceDomain);
    }

    public boolean isFakeEpsilonDomain(String domain) {
        return domain.startsWith(FAKE_EPSILON_MASS_DOMAIN_PREFIX);
    }

    private void addSourceDomainMass(   String sourceDomain, 
                                        double extraSourceDomainMass) {
        DomainInfo sourceDomainInfo = _sourceDomainMap.get(sourceDomain);
        if (sourceDomainInfo == null) {
            logIfDebug( "Adding new source domain %s with initial mass %f",
                        sourceDomain, 
                        extraSourceDomainMass);
            sourceDomainInfo = new DomainInfo(extraSourceDomainMass);
            _sourceDomainMap.put(sourceDomain, sourceDomainInfo);
            _sourceDomainList.add(new Tuple2<String, DomainInfo>(   sourceDomain, 
                                                                    sourceDomainInfo));
        } else {
            sourceDomainInfo.addMass(extraSourceDomainMass);
        }
    }

    private void distributeMassIfPossible(Collector<Tuple2<String, Double>> collector) {

        // If we're not in the middle of distributing one source domain's mass,
        // then start a new distribution for the next source domain.
        if (_activeDistribution == null) {
            _activeDistribution = new ActiveMassDistribution();
        }

        // Try to make some progress on the current distribution if possible,
        // and then toss it if that completed it.
        if (!(_activeDistribution.continueDistribution(collector))) {
            _activeDistribution = null;
            _sourceDomainIndex++;
        }

        // Log the current state of our source domain map & epsilon mass
        if (LOGGER.isDebugEnabled()) {
            for (String sourceDomain : _sourceDomainMap.keySet()) {
                logIfDebug( "Source domain '%s' now has a mass of %f",
                            sourceDomain, 
                            _sourceDomainMap.get(sourceDomain).getMass());
            }
            logIfDebug( "Total undistributed epsilon mass: %f", 
                        _pendingEpsilonMass);
        }
    }

    private String makeFakeEpsilonDomain(int targetPartition) {
        return FAKE_EPSILON_MASS_DOMAIN_PREFIX + targetPartition;
    }

    private int getNumPartitions() {
        return _parallelism;
    }

    private void logIfDebug(String format, Object... args) {
        logIfDebug(format, _partition, args);
    }

    private static void logIfDebug(String format, int partition, Object... args) {
        if (LOGGER.isDebugEnabled()) {
            Object fullArgs[] = new Object[args.length + 1];
            System.arraycopy(args, 0, fullArgs, 1, args.length);
            fullArgs[0] = partition;
            LOGGER.debug(String.format("[Partition %d] " + format, fullArgs));
        }
    }
}
