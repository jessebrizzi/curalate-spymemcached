/**
 * Copyright (C) 2009-2011 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 * 
 * 
 * Portions Copyright (C) 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License"). You may not use this 
 * file except in compliance with the License. A copy of the License is located at
 *  http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file. This file is distributed on 
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or
 * implied. See the License for the specific language governing permissions and 
 * limitations under the License.
 */

package com.curalate.spy.memcached.spring;

import com.curalate.spy.memcached.TestConfig;
import junit.framework.Assert;
import junit.framework.TestCase;

import com.curalate.spy.memcached.ConnectionFactoryBuilder.Protocol;
import com.curalate.spy.memcached.DefaultHashAlgorithm;
import com.curalate.spy.memcached.FailureMode;
import com.curalate.spy.memcached.MemcachedClient;
import com.curalate.spy.memcached.transcoders.SerializingTranscoder;
import com.curalate.spy.memcached.transcoders.Transcoder;

import org.junit.Test;

/**
 * Test cases for the {@link MemcachedClientFactoryBean} implementation.
 *
 * @author Eran Harel
 */
public class MemcachedClientFactoryBeanTest extends TestCase {

  @Test
  public void testGetObject() throws Exception {
    final MemcachedClientFactoryBean factory = new MemcachedClientFactoryBean();
    factory.setDaemon(true);
    factory.setFailureMode(FailureMode.Cancel);
    factory.setHashAlg(DefaultHashAlgorithm.CRC_HASH);
    factory.setProtocol(Protocol.BINARY);
    factory.setServers(TestConfig.IPV4_ADDR + ":22211 " + TestConfig.IPV4_ADDR
        + ":22212");
    factory.setShouldOptimize(true);
    final Transcoder<Object> transcoder = new SerializingTranscoder();
    factory.setTranscoder(transcoder);

    final MemcachedClient memcachedClient =
        (MemcachedClient) factory.getObject();

    Assert.assertEquals("servers", 2,
        memcachedClient.getUnavailableServers().size());
    Assert.assertSame("transcoder", transcoder,
        memcachedClient.getTranscoder());
  }

  @Test
  public void testGetObjectType() {
    Assert.assertEquals("object type", MemcachedClient.class,
        new MemcachedClientFactoryBean().getObjectType());
  }
}
