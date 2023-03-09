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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(AdminIT.class);

  private static String rootPath;

  private static class AdminITConfigCallback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Only one tserver to avoid race conditions on ZK propagation (auths and configuration)
      cfg.setNumTservers(1);
      // Set the min span to 0 so we will definitely get all the traces back. See ACCUMULO-4365
      Map<String,String> siteConf = cfg.getSiteConfig();
      cfg.setSiteConfig(siteConf);
    }
  }

  @BeforeAll
  public static void setupMiniCluster() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new AdminITConfigCallback());
    rootPath = getMiniClusterDir().getAbsolutePath();

    String userDir = System.getProperty("user.dir");

    // history file is updated in $HOME
    System.setProperty("HOME", rootPath);
    System.setProperty("hadoop.tmp.dir", userDir + "/target/hadoop-tmp");
  }

  @AfterAll
  public static void tearDownAfterAll() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testUsage() throws IOException, InterruptedException {
    log.info("testing usage...");
    MiniAccumuloClusterImpl.ProcessInfo p = getCluster().exec(Admin.class);
    assertEquals(0, p.getProcess().waitFor());
    var result = p.readStdOut();
    assertTrue(result.contains("Usage: accumulo admin [options]"),
        "Did not see expected Usage message");

    p = getCluster().exec(Admin.class, "-?");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    assertTrue(result.contains("Usage: accumulo admin [options]"),
        "Did not see expected Usage message");

    p = getCluster().exec(Admin.class, "--help");
    assertEquals(0, p.getProcess().waitFor());
    result = p.readStdOut();
    assertTrue(result.contains("Usage: accumulo admin [options]"),
        "Did not see expected Usage message");

    p = getCluster().exec(Admin.class, "--heelp");
    assertEquals(1, p.getProcess().waitFor());
    result = p.readStdOut();
    log.info(">>>> result: {}", result);
    assertTrue(result.contains("MissingCommandException"), "Expected to see Usage error");
  }

}
