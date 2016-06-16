/**
 * Copyright (C) 2006-2009 Dustin Sallings
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

package com.curalate.spy.memcached;

import junit.framework.TestCase;

/**
 * Test connection factory variations.
 */
public class ConnectionFactoryTest extends TestCase {

  // These tests are a little lame. They don't verify anything other than
  // that the code executes without failure.
  public void testBinaryEmptyCons() {
    new BinaryConnectionFactory();
  }

  public void testBinaryTwoIntCons() {
    new BinaryConnectionFactory(5, 5);
  }

  public void testBinaryAnIntAnotherIntAndAHashAlgorithmCons() {
    new BinaryConnectionFactory(ClientMode.Static, 5, 5, DefaultHashAlgorithm.FNV1_64_HASH);
  }

  public void testQueueSizes() {
    ConnectionFactory cf = new DefaultConnectionFactory(100, 1024);
    assertEquals(100, cf.createOperationQueue().remainingCapacity());
    assertEquals(Integer.MAX_VALUE, cf.createWriteOperationQueue()
        .remainingCapacity());
    assertEquals(Integer.MAX_VALUE, cf.createReadOperationQueue()
        .remainingCapacity());
  }
}
