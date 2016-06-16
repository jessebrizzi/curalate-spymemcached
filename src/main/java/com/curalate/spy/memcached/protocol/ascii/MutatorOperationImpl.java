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

package com.curalate.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import com.curalate.spy.memcached.KeyUtil;
import com.curalate.spy.memcached.ops.Mutator;
import com.curalate.spy.memcached.ops.OperationCallback;
import com.curalate.spy.memcached.ops.OperationState;
import com.curalate.spy.memcached.ops.StatusCode;
import com.curalate.spy.memcached.protocol.BaseOperationImpl;
import com.curalate.spy.memcached.ops.MutatorOperation;
import com.curalate.spy.memcached.ops.OperationStatus;

/**
 * Operation for mutating integers inside of memcached.
 */
final class MutatorOperationImpl extends OperationImpl implements
    MutatorOperation {

  public static final int OVERHEAD = 32;

  private static final OperationStatus NOT_FOUND = new OperationStatus(false,
      "NOT_FOUND", StatusCode.ERR_NOT_FOUND);

  private final Mutator mutator;
  private final String key;
  private final long amount;

  public MutatorOperationImpl(Mutator m, String k, long amt,
      OperationCallback c) {
    super(c);
    mutator = m;
    key = k;
    amount = amt;
  }

  @Override
  public void handleLine(String line) {
    getLogger().debug("Result:  %s", line);
    OperationStatus found = null;
    if (line.equals("NOT_FOUND")) {
      found = NOT_FOUND;
    } else {
      found = new OperationStatus(true, line, StatusCode.SUCCESS);
    }
    getCallback().receivedStatus(found);
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    int size = KeyUtil.getKeyBytes(key).length + OVERHEAD;
    ByteBuffer b = ByteBuffer.allocate(size);
    setArguments(b, mutator.name(), key, amount);
    b.flip();
    setBuffer(b);
  }

  @Override
  protected void wasCancelled() {
    // XXX: Replace this comment with why the hell I did this.
    getCallback().receivedStatus(BaseOperationImpl.CANCELLED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public long getBy() {
    return amount;
  }

  public long getDefault() {
    return -1;
  }

  public int getExpiration() {
    return -1;
  }

  public Mutator getType() {
    return mutator;
  }

  @Override
  public String toString() {
    return "Cmd: " + mutator.name() + " Key: " + key + " Amount: " + amount;
  }
}
