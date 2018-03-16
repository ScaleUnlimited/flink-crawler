package com.scaleunlimited.flinkcrawler.urls;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SingleDomainUrlValidatorTest {

    @Test
    public void testSingleDomainUrlValidator() {
        SimpleUrlValidator validator = new SingleDomainUrlValidator("scaleunlimited.com");
        assertThat(validator.isValid("http://transpac.com")).isFalse();
        assertThat(validator.isValid("http://scaleunlimited.com")).isTrue();
    }

}
