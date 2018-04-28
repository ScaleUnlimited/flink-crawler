package com.scaleunlimited.flinkcrawler.functions;

import java.util.HashSet;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class DomainRankFunctionTest {

    private static final int TEST_PARALLELISM = 2;
    public static HashSet<Tuple2<String, String>> TEST_DOMAIN_LINKS = new HashSet<Tuple2<String, String>>();
    static {
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-1", "domain-2"));
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-1", "domain-3"));
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-3", "domain-1"));
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-3", "domain-5"));
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-4", "domain-5"));
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-4", "domain-6"));
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-5", "domain-4"));
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-5", "domain-6"));
        TEST_DOMAIN_LINKS.add(new Tuple2("domain-6", "domain-4"));
    }

    @Test
    public void test() throws Throwable {

        // TODO Make compatible with Flink-1.5 and re-enable:
        
//        // TODO Do I need a LocalStreamEnvironmentWithAsyncExecution instead?
//        StreamExecutionEnvironment env = new LocalStreamEnvironment().setParallelism(TEST_PARALLELISM);
//
//        // TODO This should really send the links in one at a time delaying in
//        // between them to simulate a real crawling environment. However,
//        // the real problem is that when this stream finishes, it also prevents
//        // any Tuples from domainMassIteration from making it into
//        // DomainRankFunction.
//        DataStream<Tuple2<String, String>> domainLinks =
//            env.fromCollection(TEST_DOMAIN_LINKS)
//                .name("domain links")
//                .partitionCustom(new HashPartitioner(), new DomainRankFunction.DomainLinkKeySelector());
//
//        // TODO This empty stream is fairly ugly.
//        TupleTypeInfo<Tuple2<String, Double>> domainMassTypeInfo = 
//            new TupleTypeInfo<Tuple2<String, Double>>(  BasicTypeInfo.STRING_TYPE_INFO, 
//                                                        BasicTypeInfo.DOUBLE_TYPE_INFO);
//        DataStream<Tuple2<String, Double>> noInitialDomainMass = 
//            env.fromCollection( new HashSet<Tuple2<String, Double>>(), 
//                                domainMassTypeInfo)
//                .name("(no) initial domain mass");
//
//        IterativeStream<Tuple2<String, Double>> domainMassIteration = noInitialDomainMass.iterate();
//        domainMassIteration.setParallelism(TEST_PARALLELISM);
//
//        DataStream<Tuple2<String, Double>> partitionedDomainMass = 
//            domainMassIteration.partitionCustom(new HashPartitioner(), 
//                                                new DomainRankFunction.DomainMassKeySelector());
//
//        DataStream<Tuple2<String, Double>> transferredDomainMass = 
//            domainLinks.connect(partitionedDomainMass)
//                .flatMap(new DomainRankFunction(0.1d))
//                .name("DomainRankFunction");
//
//        domainMassIteration.closeWith(transferredDomainMass);
//
//        String dotAsString = FlinkUtils.planToDot(env.getExecutionPlan());
//        FileUtils.write(new File("mydot.dot"), dotAsString, "UTF-8");
//
//        env.execute();
    }
}
