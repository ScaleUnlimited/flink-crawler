package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;

import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

public class NormalizeUrlsFunctionTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testFunction() throws MalformedURLException, Exception {
		
		// NOTE : In the case of rich functions we would also need to handle
		// parallelism in the open method.
		NormalizeUrlsFunction func = new NormalizeUrlsFunction();
		Collector<RawUrl> collector = Mockito.mock(Collector.class);
		func.flatMap(new RawUrl(" http://www.foo.com/foo.html#ref ", 10.0f), collector );
		
        Mockito.verify(collector).collect(Mockito.argThat(new MatchNormalizedUrl()));
	}

    private static class MatchNormalizedUrl implements ArgumentMatcher<RawUrl> {
       
		@Override
		public boolean matches(RawUrl normalizedUrl) {
			if (normalizedUrl.getUrl().equals("http://www.foo.com/foo.html") &&
					normalizedUrl.getScore() == 10.0f) {
				return true;
			}
			return false;
		}
    }
	
}
