/*
 * Copyright 2009-2018 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.scaleunlimited.flinkcrawler.urls;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SimpleUrlValidatorTest {

    @Test
    public void testValidate() {
        SimpleUrlValidator validator = new SimpleUrlValidator();

        assertTrue(validator.isValid("http://foo.com"));
        assertTrue(validator.isValid("http://www.foo.com"));
        assertTrue(validator.isValid("http://www.foo.com/"));
        assertTrue(validator.isValid("http://aws.foo.com/"));
        assertTrue(validator.isValid("https://aws.foo.com/"));

        assertFalse(validator.isValid("foo.com"));
        assertFalse(validator.isValid("www.foo.com"));
        assertFalse(validator.isValid("mailto://ken@foo.com"));
        assertFalse(
                validator.isValid("mailto:?Subject=http://info.foo.com/copyright/us/details.html"));
        assertFalse(validator.isValid("smtp://aws.foo.com/"));
        assertFalse(validator.isValid("ftp://aws.foo.com/"));
        assertFalse(validator.isValid("javascript:foobar()"));

        assertFalse(validator.isValid("feed://getbetterhealth.com/feed"));
        assertFalse(validator.isValid(
                "ttp://www.thehealthcareblog.com/the_health_care_blog/2009/07/healthcare-reform-lessons-from-mayo-clinic.html"));
    }

    @Test
    public void testSuffixBlacklist() {
        SimpleUrlValidator validator = new SimpleUrlValidator("pdf", "xml");

        assertTrue(validator.isValid("http://foo.com/page1"));
        assertTrue(validator.isValid("http://foo.com/page1.html"));

        assertFalse(validator.isValid("http://foo.com/page1.pdf"));
        assertFalse(validator.isValid("http://foo.com/page1.xml"));
    }

    @Test
    public void testUnencodedUrl() {
        SimpleUrlValidator validator = new SimpleUrlValidator();

        assertFalse(validator.isValid(
                "http://mail-archives.us.apache.org/mod_mbox/www-announce/201705.mbox/<CACRbFyjtT7QQGHUzTRdbJoySbJb7tt4BDk5-r-VRn0GB0Kgvag@mail.gmail.com>"));
        assertTrue(validator.isValid(
                "http://mail-archives.us.apache.org/mod_mbox/www-announce/201705.mbox/%3CCACRbFyjtT7QQGHUzTRdbJoySbJb7tt4BDk5-r-VRn0GB0Kgvag@mail.gmail.com%3E"));
    }

}
