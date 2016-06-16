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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collection;

import com.curalate.spy.memcached.config.NodeEndPoint;
import com.curalate.spy.memcached.ops.Operation;

/**
 * A MockMemcachedNode.
 */
public class MockMemcachedNode implements MemcachedNode {
  private final InetSocketAddress socketAddress;
  private NodeEndPoint nodeEndPoint;
  
  public SocketAddress getSocketAddress() {
    return socketAddress;
  }
  
  public NodeEndPoint getNodeEndPoint(){
    return nodeEndPoint;
  }
  
  public void setNodeEndPoint(NodeEndPoint nodeEndPoint){
    this.nodeEndPoint = nodeEndPoint;
  }
  
  public MockMemcachedNode(InetSocketAddress socketAddress) {
    this.socketAddress = socketAddress;
  }
  
  public MockMemcachedNode(NodeEndPoint nodeEndPoint) {
    this.nodeEndPoint = nodeEndPoint;
    this.socketAddress = nodeEndPoint.getInetSocketAddress();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MockMemcachedNode that = (MockMemcachedNode) o;

    if (socketAddress != null ? !socketAddress.equals(that.socketAddress)
        : that.socketAddress != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return (socketAddress != null ? socketAddress.hashCode() : 0);
  }

  public void copyInputQueue() {
    // noop
  }

  public void setupResend() {
    // noop
  }

  public void fillWriteBuffer(boolean optimizeGets) {
    // noop
  }

  public void transitionWriteItem() {
    // noop
  }

  public Operation getCurrentReadOp() {
    return null;
  }

  public Operation removeCurrentReadOp() {
    return null;
  }

  public Operation getCurrentWriteOp() {
    return null;
  }

  public Operation removeCurrentWriteOp() {
    return null;
  }

  public boolean hasReadOp() {
    return false;
  }

  public boolean hasWriteOp() {
    return false;
  }

  public void addOp(Operation op) {
    // noop
  }

  public void insertOp(Operation op) {
    // noop
  }

  public int getSelectionOps() {
    return 0;
  }

  public ByteBuffer getRbuf() {
    return null;
  }

  public ByteBuffer getWbuf() {
    return null;
  }

  public boolean isActive() {
    return false;
  }

  public void reconnecting() {
    // noop
  }

  public void connected() {
    // noop
  }

  public int getReconnectCount() {
    return 0;
  }

  public void registerChannel(SocketChannel ch, SelectionKey selectionKey) {
    // noop
  }

  public void setChannel(SocketChannel to) {
    // noop
  }

  public SocketChannel getChannel() {
    return null;
  }

  public void setSk(SelectionKey to) {
    // noop
  }

  public SelectionKey getSk() {
    return null;
  }

  public int getBytesRemainingToWrite() {
    return 0;
  }

  public int writeSome() throws IOException {
    return 0;
  }

  public void fixupOps() {
    // noop
  }

  public Collection<Operation> destroyInputQueue() {
    return null;
  }

  public void authComplete() {
    // noop
  }

  public void setupForAuth() {
    // noop
  }

  public int getContinuousTimeout() {
    return 0;
  }

  public void setContinuousTimeout(boolean timedOut) {
    // noop
  }

  public boolean isAuthenticated() {
    return true;
  }

  public long lastReadDelta() {
    return 0;
  }

  public void completedRead() {
    // noop
  }

  @Override
  public MemcachedConnection getConnection() {
    return null;
  }

  @Override
  public void setConnection(MemcachedConnection connection) {

  }

}
