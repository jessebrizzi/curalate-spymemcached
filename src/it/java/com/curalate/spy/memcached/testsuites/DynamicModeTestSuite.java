/**
 * Copyright (C) 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 *
 * Licensed under the Amazon Software License (the "License"). You may not use this 
 * file except in compliance with the License. A copy of the License is located at
 *  http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file. This file is distributed on 
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or
 * implied. See the License for the specific language governing permissions and 
 * limitations under the License. 
 */
package com.curalate.spy.memcached.testsuites;

import java.net.InetSocketAddress;
import java.util.List;

import com.curalate.spy.memcached.TimeoutNowriteTest;
import com.curalate.spy.memcached.AddrUtil;
import com.curalate.spy.memcached.AsciiCancellationTest;
import com.curalate.spy.memcached.AsciiClientTest;
import com.curalate.spy.memcached.AutoDiscoveryTest;
import com.curalate.spy.memcached.BinaryCancellationTest;
import com.curalate.spy.memcached.BinaryClientTest;
import com.curalate.spy.memcached.CASMutatorTest;
import com.curalate.spy.memcached.CancelFailureModeTest;
import com.curalate.spy.memcached.ClientMode;
import com.curalate.spy.memcached.LongClientTest;
import com.curalate.spy.memcached.MemcachedClient;
import com.curalate.spy.memcached.MemcachedClientConstructorTest;
import com.curalate.spy.memcached.ObserverTest;
import com.curalate.spy.memcached.QueueOverflowTest;
import com.curalate.spy.memcached.RedistributeFailureModeTest;
import com.curalate.spy.memcached.TestConfig;
import com.curalate.spy.memcached.TimeoutTest;
import com.curalate.spy.memcached.categories.StandardTests;
import com.curalate.spy.memcached.ops.ConfigurationType;
import com.curalate.spy.memcached.util.ConnectionUtil;
import com.curalate.spy.memcached.util.ConnectionUtilLinuxImpl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Categories.class)  
@IncludeCategory(StandardTests.class)
@SuiteClasses({
  AutoDiscoveryTest.class,
  AsciiClientTest.class,
  AsciiCancellationTest.class,
  BinaryClientTest.class,
  BinaryCancellationTest.class,
  CancelFailureModeTest.class,
  CASMutatorTest.class,
  LongClientTest.class,
  MemcachedClientConstructorTest.class,
  ObserverTest.class,
  QueueOverflowTest.class,
  RedistributeFailureModeTest.class,
  TimeoutNowriteTest.class,
  TimeoutTest.class
  })
public class DynamicModeTestSuite {

  public static ConnectionUtil connUtil = new ConnectionUtilLinuxImpl();

  /**
   * Start connections to local memcached ports. This is executed before all tests 
   * in the suite. 
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    System.out.println("Master Set Up.");
    TestConfig.initialize(ClientMode.Dynamic);
    startConnections();
    List<InetSocketAddress> addrs = AddrUtil.getAddresses(TestConfig.IPV4_ADDR
        + ":" + TestConfig.PORT_NUMBER);
    MemcachedClient client = new MemcachedClient(addrs);
    client.setConfig(addrs.get(0), ConfigurationType.CLUSTER, "1\n" + "localhost.localdomain|" + TestConfig.IPV4_ADDR + "|" + TestConfig.PORT_NUMBER);
  }


  /**
   * Kill connection to local memcached, finish tear down. This is executed after all
   * tests are completed in the suite. 
   */
  @AfterClass
  public static void tearDownClass() throws Exception {
    killConnections();
    System.out.println("Master Tear Down.");
  }

  /**
   * Starts local memcached servers in multiple ports.
   * @throws Exception 
   */
  private static void startConnections() throws Exception {
    connUtil.addLocalMemcachedServer(11200, 11201, 11211, 11212, 22211, 22212);
  }

  /**
   * Kills local memcached connections created for this test.
   */
  private static void killConnections() {
    connUtil.removeLocalMemcachedServer(TestConfig.MEMCACHED_NAME,11200, 11201, 11211, 11212, 22211, 22212);
  }

}
