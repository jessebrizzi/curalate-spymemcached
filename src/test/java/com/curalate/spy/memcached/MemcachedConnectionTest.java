/**
 * Copyright (C) 2006-2009 Dustin Sallings
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
 */

package com.curalate.spy.memcached;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.curalate.spy.memcached.protocol.binary.BinaryOperationFactory;
import junit.framework.TestCase;

/**
 * Test stuff that can be tested within a MemcachedConnection separately.
 */
public class MemcachedConnectionTest extends TestCase {

  public void testDebugBuffer() throws Exception {
    String input = "this is a test _";
    ByteBuffer bb = ByteBuffer.wrap(input.getBytes());
    String s = MemcachedConnection.dbgBuffer(bb, input.length());
    assertEquals("this is a test \\x5f", s);
  }

  public void testConnectionsStatus() throws Exception {
    ConnectionFactory factory = new DefaultConnectionFactory();
    List<InetSocketAddress> addresses =
      AddrUtil.getAddresses(UnitTestConfig.IPV4_ADDR + ":11211");
    Collection<ConnectionObserver> observers =
      new ArrayList<ConnectionObserver>();
    MemcachedConnection mcc = new MemcachedConnection(10240, factory, addresses,
      observers, FailureMode.Retry, new BinaryOperationFactory());
    assertNotNull(mcc.connectionsStatus());
  }

  public void testBelongsToCluster() throws Exception {
    ConnectionFactory factory = new DefaultConnectionFactory();
    Collection<ConnectionObserver> observers =
      new ArrayList<ConnectionObserver>();
    OperationFactory opfactory = new BinaryOperationFactory();

    MemcachedNode node = new MockMemcachedNode(
      new InetSocketAddress(UnitTestConfig.IPV4_ADDR, UnitTestConfig.PORT_NUMBER));
    MemcachedNode node2 = new MockMemcachedNode(
      new InetSocketAddress("invalidIpAddr", UnitTestConfig.PORT_NUMBER));

    List<InetSocketAddress> nodes = new ArrayList<InetSocketAddress>();
    nodes.add((InetSocketAddress)node.getSocketAddress());

    MemcachedConnection conn = new MemcachedConnection(
      100, factory, nodes, observers, FailureMode.Retry, opfactory);
    assertTrue(conn.belongsToCluster(node));
    assertFalse(conn.belongsToCluster(node2));
  }
}
