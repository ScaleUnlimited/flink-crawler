package com.scaleunlimited.flinkcrawler.functions;

//import org.flinkspector.core.collection.ExpectedRecords;
//import org.flinkspector.core.quantify.MatchTuples;
//import org.flinkspector.core.quantify.OutputMatcher;
//import org.flinkspector.core.trigger.FinishAtCount;
//import org.flinkspector.dataset.DataSetTestBase;
import org.junit.After;
import org.junit.Before;

//import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
// TODO re-implement this without using flinkspector, which requires Java 8.
public class NormalizeUrlsFunctionTest /* extends DataSetTestBase */ {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

//	@SuppressWarnings("serial")
//	private DataSet<Tuple2<String, Float>> normalize(DataSet<Tuple2<String, Float>> inputSet) {
//		return inputSet.map(new MapFunction<Tuple2<String, Float>, Tuple2<String, Float>>() {
//
//			@Override
//			public Tuple2<String, Float> map(Tuple2<String, Float> arg0) throws Exception {
//				// TODO Auto-generated method stub
//				return null;
//			}
//		});
//	}
//	
//	@Test
//	public void test() throws Throwable {
//		/*
//		 * Define the input DataSet:
//		 * Get a DataSetBuilder with .createTestStreamWith(record).
//		 * Add data records to it and retrieve a DataSet,
//		 * by calling .complete().
//		 */
//		DataSet<Tuple2<String, Float>> testDataSet =
//			createTestDataSetWith(Tuple2.of(" http://www.foo.com/ ", 100.0f))
//				.emit(Tuple2.of("http://WWW.Foo.Com:80/page.html", 200.0f))
//				.emit(Tuple2.of("http://www.foo.com/foo.html#ref", 300.0f))
//				.close();
//		/*
//		 * Define the output you expect from the the transformation under test.
//		 * Add the tuples you want to see with .expect(record).
//		 */
//		ExpectedRecords<Tuple2<String, Float>> output = ExpectedRecords
//			.create(Tuple2.of("http://www.foo.com/", 100.0f))
//			.expect(Tuple2.of("http://www.foo.com/page.html", 200.0f))
//			.expect(Tuple2.of("http://www.foo.com/foo.html", 300.0f));
//		// refine your expectations by adding requirements
//		output.refine().only().inOrder(strict);
//
//		/*
//		 * Creates an OutputMatcher using AssertTuples.
//		 * AssertTuples builds an OutputMatcher working on Tuples.
//		 * You assign String identifiers to your Tuple,
//		 * and add hamcrest matchers testing the values.
//		 */
//		OutputMatcher<Tuple2<String, Float>> matcher =
//			//name the values in your tuple with keys:
//			new MatchTuples<Tuple2<String, Float>>("url", "estimated-score")
//				//add an assertion using a value and hamcrest matchers
//				.assertThat("url", isA(String.class))
//				.assertThat("estimated-score", greaterThanOrEqualTo(100.0f))
//				.assertThat("estimated-score", lessThanOrEqualTo(300.0f))
//				//express how many matchers must return true for your test to pass:
//				.anyOfThem()
//				//define how many records need to fulfill the
//				.onEachRecord();
//
//		/*
//		 * Use assertDataSet to map DataSet to an OutputMatcher.
//		 * ExpectedRecords extends OutputMatcher and thus can be used in this way.
//		 * Combine the created matchers with anyOf(), implicating that at least one of
//		 * the matchers must be positive.
//		 */
//		assertDataSet(normalize(testDataSet), anyOf(output, matcher), FinishAtCount.of(3));	}
//	}
}
