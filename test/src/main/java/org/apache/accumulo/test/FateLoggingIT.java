/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;


import java.io.File;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateLoggingIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(FateLoggingIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(MiniClusterConfigurationCallback.NO_CALLBACK);
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testTest() {
    log.info("**** this is a log msg");
    log.info(">>>> " + SharedMiniClusterBase.getCluster().getFileSystem().getWorkingDirectory().getName());
    SharedMiniClusterBase.getClientInfo().getProperties().forEach((k,v) -> log.info("k: " + v));
  }

  @Test
  public void createTable()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
    }
  }

//  @Test
//  public void createTable1()
//      throws TableExistsException, AccumuloSecurityException, AccumuloException {
//    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
//      ClientContext context = new ClientContext(getClientProps());
//      String tableName = getUniqueNames(1)[0];
//      //client.tableOperations().create(tableName);
//      TableOperationsImpl tops = new TableOperationsImpl(context);
//      tops.doFateOperation(FateOperation.TABLE_CREATE, );
//    }
//  }

  //@Test
  public void createTableWithSplits()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("ccccc"));
      splits.add(new Text("aaaaa"));
      splits.add(new Text("ddddd"));
      splits.add(new Text("abcde"));
      splits.add(new Text("bbbbb"));
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      client.tableOperations().create(tableName, ntc);
    }
  }

  @Test(expected = AccumuloException.class)
  public void createTableInNonExistentNamespace()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = "not_a_namespace." + getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
    }
  }

  @Test
  public void createTableInsufficientPermissions()
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      log.info(">>>> whoami1: {}", client.whoami());
      client.securityOperations().createLocalUser("user1", new PasswordToken("user1pwd"));
      log.info(">>>> whoami2: {}", client.whoami());
      client.tableOperations().create(tableName);
    }
  }

}
