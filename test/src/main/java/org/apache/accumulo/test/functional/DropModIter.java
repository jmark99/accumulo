/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropModIter extends SkippingIterator {

  private static final Logger log = LoggerFactory.getLogger(DropModIter.class);

  private int mod;
  private int drop;

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    log.info(">>>> tableId: {}", env.getTableId());
    log.info(">>>> scope:   {}", env.getIteratorScope().toString());
    PluginEnvironment pluginEnv = env.getPluginEnv();
    PluginEnvironment.Configuration configuration = pluginEnv.getConfiguration();
    log.info(">>>> configuration: {}", configuration.toString());
    PluginEnvironment.Configuration configuration1 = pluginEnv.getConfiguration(env.getTableId());
    log.info(">>>> configuration1: {}", configuration1.toString());
    try {
      log.info(">>>> tableName: {}", pluginEnv.getTableName(env.getTableId()));
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.mod = Integer.parseInt(options.get("mod"));
    this.drop = Integer.parseInt(options.get("drop"));
  }

  @Override
  protected void consume() throws IOException {
    while (getSource().hasTop()
        && Integer.parseInt(getSource().getTopKey().getRow().toString()) % mod == drop) {
      getSource().next();
    }
  }

}
