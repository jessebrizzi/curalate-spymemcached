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

package com.curalate.spy.memcached.protocol.binary;

import com.curalate.spy.memcached.ops.GetsOperation;
import com.curalate.spy.memcached.ops.ObserveOperation;
import com.curalate.spy.memcached.ops.BaseOperationFactory;
import com.curalate.spy.memcached.ops.CASOperation;
import com.curalate.spy.memcached.ops.ConcatenationOperation;
import com.curalate.spy.memcached.ops.ConcatenationType;
import com.curalate.spy.memcached.ops.ConfigurationType;
import com.curalate.spy.memcached.ops.DeleteOperation;
import com.curalate.spy.memcached.ops.FlushOperation;
import com.curalate.spy.memcached.ops.GetAndTouchOperation;
import com.curalate.spy.memcached.ops.GetConfigOperation;
import com.curalate.spy.memcached.ops.GetOperation;
import com.curalate.spy.memcached.ops.GetOperation.Callback;
import com.curalate.spy.memcached.ops.DeleteConfigOperation;
import com.curalate.spy.memcached.ops.GetlOperation;
import com.curalate.spy.memcached.ops.KeyedOperation;
import com.curalate.spy.memcached.ops.MultiGetOperationCallback;
import com.curalate.spy.memcached.ops.MultiGetsOperationCallback;
import com.curalate.spy.memcached.ops.MultiReplicaGetOperationCallback;
import com.curalate.spy.memcached.ops.Mutator;
import com.curalate.spy.memcached.ops.MutatorOperation;
import com.curalate.spy.memcached.ops.NoopOperation;
import com.curalate.spy.memcached.ops.Operation;
import com.curalate.spy.memcached.ops.OperationCallback;
import com.curalate.spy.memcached.ops.ReplicaGetOperation;
import com.curalate.spy.memcached.ops.ReplicaGetsOperation;
import com.curalate.spy.memcached.ops.SASLAuthOperation;
import com.curalate.spy.memcached.ops.SASLMechsOperation;
import com.curalate.spy.memcached.ops.SASLStepOperation;
import com.curalate.spy.memcached.ops.SetConfigOperation;
import com.curalate.spy.memcached.ops.StatsOperation;
import com.curalate.spy.memcached.ops.StoreOperation;
import com.curalate.spy.memcached.ops.StoreType;
import com.curalate.spy.memcached.ops.TapOperation;
import com.curalate.spy.memcached.ops.UnlockOperation;
import com.curalate.spy.memcached.ops.VersionOperation;
import com.curalate.spy.memcached.tapmessage.RequestMessage;
import com.curalate.spy.memcached.tapmessage.TapOpcode;

import javax.security.auth.callback.CallbackHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Factory for binary operations.
 */
public class BinaryOperationFactory extends BaseOperationFactory {

  public DeleteOperation
  delete(String key, DeleteOperation.Callback operationCallback) {
    return new DeleteOperationImpl(key, operationCallback);
  }

  public DeleteOperation delete(String key, long cas,
    DeleteOperation.Callback operationCallback) {
    return new DeleteOperationImpl(key, cas, operationCallback);
  }

  public UnlockOperation unlock(String key, long casId,
          OperationCallback cb) {
    return new UnlockOperationImpl(key, casId, cb);
  }
  public ObserveOperation observe(String key, long casId, int index,
                                  ObserveOperation.Callback cb) {
    return new ObserveOperationImpl(key, casId, index, cb);
  }
  public FlushOperation flush(int delay, OperationCallback cb) {
    return new FlushOperationImpl(cb);
  }

  public GetAndTouchOperation getAndTouch(String key, int expiration,
      GetAndTouchOperation.Callback cb) {
    return new GetAndTouchOperationImpl(key, expiration, cb);
  }

  public GetOperation get(String key, Callback callback) {
    return new GetOperationImpl(key, callback);
  }

  public ReplicaGetOperation replicaGet(String key, int index,
    ReplicaGetOperation.Callback callback) {
    return new ReplicaGetOperationImpl(key, index, callback);
  }

  public ReplicaGetsOperation replicaGets(String key, int index,
    ReplicaGetsOperation.Callback callback) {
    return new ReplicaGetsOperationImpl(key, index, callback);
  }

  public GetOperation get(Collection<String> value, Callback cb) {
    return new MultiGetOperationImpl(value, cb);
  }

  public GetlOperation getl(String key, int exp, GetlOperation.Callback cb) {
    return new GetlOperationImpl(key, exp, cb);
  }

  public GetsOperation gets(String key, GetsOperation.Callback cb) {
    return new GetsOperationImpl(key, cb);
  }

  public StatsOperation keyStats(String key, StatsOperation.Callback cb) {
    return new KeyStatsOperationImpl(key, cb);
  }

  public GetConfigOperation getConfig(ConfigurationType type, GetConfigOperation.Callback callback) {
    return new GetConfigOperationImpl(type, callback);
  }
  
  public SetConfigOperation setConfig(ConfigurationType type, int flags, byte[] data, OperationCallback cb){
    return new SetConfigOperationImpl(type, flags, data, cb);
  }
  
  public DeleteConfigOperation deleteConfig(ConfigurationType type, OperationCallback cb) {
    return new DeleteConfigOperationImpl(type, cb);
  }

  public MutatorOperation mutate(Mutator m, String key, long by, long def,
      int exp, OperationCallback cb) {
    return new MutatorOperationImpl(m, key, by, def, exp, cb);
  }

  public StatsOperation stats(String arg,
      StatsOperation.Callback cb) {
    return new StatsOperationImpl(arg, cb);
  }

  public StoreOperation store(StoreType storeType, String key, int flags,
      int exp, byte[] data, StoreOperation.Callback cb) {
    return new StoreOperationImpl(storeType, key, flags, exp, data, 0, cb);
  }

  public KeyedOperation touch(String key, int expiration,
      OperationCallback cb) {
    return new TouchOperationImpl(key, expiration, cb);
  }

  public VersionOperation version(OperationCallback cb) {
    return new VersionOperationImpl(cb);
  }

  public NoopOperation noop(OperationCallback cb) {
    return new NoopOperationImpl(cb);
  }

  public CASOperation cas(StoreType type, String key, long casId, int flags,
      int exp, byte[] data, StoreOperation.Callback cb) {
    return new StoreOperationImpl(type, key, flags, exp, data, casId, cb);
  }

  public ConcatenationOperation cat(ConcatenationType catType, long casId,
      String key, byte[] data, OperationCallback cb) {
    return new ConcatenationOperationImpl(catType, key, data, casId, cb);
  }

  @Override
  protected Collection<? extends Operation> cloneGet(KeyedOperation op) {
    Collection<Operation> rv = new ArrayList<Operation>();
    GetOperation.Callback getCb = null;
    GetsOperation.Callback getsCb = null;
    ReplicaGetOperation.Callback replicaGetCb = null;
    if (op.getCallback() instanceof GetOperation.Callback) {
      getCb =
          new MultiGetOperationCallback(op.getCallback(), op.getKeys().size());
    } else if(op.getCallback() instanceof ReplicaGetOperation.Callback) {
      replicaGetCb =
       new MultiReplicaGetOperationCallback(op.getCallback(), op.getKeys().size());
    } else {
      getsCb =
          new MultiGetsOperationCallback(op.getCallback(), op.getKeys().size());
    }
    for (String k : op.getKeys()) {
      if(getCb != null) {
        rv.add(get(k, getCb));
      } else if(getsCb != null) {
        rv.add(gets(k, getsCb));
      } else {
        rv.add(replicaGet(k, ((ReplicaGetOperationImpl)op).getReplicaIndex() ,replicaGetCb));
      }
    }
    return rv;
  }

  public SASLAuthOperation saslAuth(String[] mech, String serverName,
      Map<String, ?> props, CallbackHandler cbh, OperationCallback cb) {
    return new SASLAuthOperationImpl(mech, serverName, props, cbh, cb);
  }

  public SASLMechsOperation saslMechs(OperationCallback cb) {
    return new SASLMechsOperationImpl(cb);
  }

  public SASLStepOperation saslStep(String[] mech, byte[] challenge,
      String serverName, Map<String, ?> props, CallbackHandler cbh,
      OperationCallback cb) {
    return new SASLStepOperationImpl(mech, challenge, serverName, props, cbh,
        cb);
  }

  public TapOperation tapBackfill(String id, long date, OperationCallback cb) {
    return new TapBackfillOperationImpl(id, date, cb);
  }

  public TapOperation tapCustom(String id, RequestMessage message,
      OperationCallback cb) {
    return new TapCustomOperationImpl(id, message, cb);
  }

  public TapOperation
  tapAck(TapOpcode opcode, int opaque, OperationCallback cb) {
    return new TapAckOperationImpl(opcode, opaque, cb);
  }

  public TapOperation tapDump(String id, OperationCallback cb) {
    return new TapDumpOperationImpl(id, cb);
  }
}
