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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//SharedMiniClusterBase
public class AdminIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(AdminIT.class);

  @Override
  protected Duration defaultTimeout() {
    log.info("set DefaultTimeout...");
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    log.info("configure...");
    cfg.setSiteConfig(Collections.singletonMap(Property.TABLE_FILE_BLOCK_SIZE.getKey(), "1234567"));
  }

  // private static String rootPath;

  @TempDir
  private static File tempDir;

  // private static class AdminITConfigCallback implements MiniClusterConfigurationCallback {
  // @Override
  // public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
  // // Only one tserver to avoid race conditions on ZK propagation (auths and configuration)
  // cfg.setNumTservers(1);
  // // Set the min span to 0, so we will definitely get all the traces back. See ACCUMULO-4365
  // Map<String,String> siteConf = cfg.getSiteConfig();
  // cfg.setSiteConfig(siteConf);
  // }
  // }

  // @BeforeAll
  // public static void setupMiniCluster() throws Exception {
  // SharedMiniClusterBase.startMiniClusterWithConfig(new AdminITConfigCallback());
  // rootPath = getMiniClusterDir().getAbsolutePath();
  //
  // String userDir = System.getProperty("user.dir");
  //
  // // history file is updated in $HOME
  // System.setProperty("HOME", rootPath);
  // System.setProperty("hadoop.tmp.dir", userDir + "/target/hadoop-tmp");
  // }

  // @AfterAll
  // public static void tearDownAfterAll() {
  // SharedMiniClusterBase.stopMiniCluster();
  // }

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

    p = getCluster().exec(Admin.class, "-help");
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

  /**
   *
   * Usage: accumulo admin [options] [command] [command options] Options: -auths, --auths the
   * authorizations to use when reading or writing Default: 'empty string' -c, --config-file Read
   * the given client config file. If omitted, the classpath will be searched for file named
   * accumulo-client.properties -f, --force force the given server to stop by removing its lock
   * Default: false -h, -?, --help, -help
   *
   * --password connection password (can be specified as '<password>', 'pass:<password>',
   * 'file:<local file containing the password>' or 'env:<variable containing the pass>') --trace
   * turn on distributed tracing Default: false -u, --user Connection user -o Overrides property in
   * accumulo-client.properties. Expected format: -o <key>=<value> Default: []
   */
  @Test
  public void testAdminOptions() {
    log.info("testAdminOptions...");
  }

  /**
   * changeSecret Changes the unique secret given to the instance that all servers must know. Usage:
   * changeSecret
   */
  @Test
  public void testChangeSecret() {
    log.info("testChangeSecret...");
  }

  /**
   * checkTablets print tablets that are offline in online tables Usage: checkTablets [options]
   * Options: --fixFiles Remove dangling file pointers Default: false -t, --table Table to check, if
   * not set checks all tables
   */
  @Test
  public void testCheckTablets() {
    log.info("testCheckTablets...");
  }

  /**
   * deleteZooInstance Deletes specific instance name or id from zookeeper or cleans up all old
   * instances. Usage: deleteZooInstance [options] Options: -c, --clean Cleans Zookeeper by deleting
   * all old instances. This will not delete the instance pointed to by the local
   * accumulo.properties file Default: false -i, --instance the instance name or id to delete
   * --password The system secret, if different than instance.secret in accumulo.properties
   */
  @Test
  public void testDeleteZooInstance() {
    log.info("testDeleteZooInstance...");
  }

  /**
   * dumpConfig print out non-default configuration settings Usage: dumpConfig [options] Options:
   * -a, --all print the system and all table configurations Default: false -d, --directory
   * directory to place config files -n, --namespaces print the namespace configuration Default:
   * false -s, --system print the system configuration Default: false -t, --tables print per-table
   * configuration Default: [] -u, --users print users and their authorizations and permissions
   * Default: false
   */
  @Test
  public void testDumpConfig() throws Exception {
    log.info("testDumpConfig...");
    File folder = new File(tempDir, testName() + "/");
    assertTrue(folder.isDirectory() || folder.mkdir(), "failed to create dir: " + folder);
    File siteFileBackup = new File(folder, "accumulo.properties.bak");
    assertFalse(siteFileBackup.exists());
    assertEquals(0, exec(Admin.class, "dumpConfig", "-a", "-d", folder.getPath()).waitFor());
    assertTrue(siteFileBackup.exists());
    String site = FunctionalTestUtils.readAll(new FileInputStream(siteFileBackup));
    assertTrue(site.contains(Property.TABLE_FILE_BLOCK_SIZE.getKey()));
    assertTrue(site.contains("1234567"));
    String meta = FunctionalTestUtils
        .readAll(new FileInputStream(new File(folder, MetadataTable.NAME + ".cfg")));
    assertTrue(meta.contains(Property.TABLE_FILE_REPLICATION.getKey()));
    String systemPerm =
        FunctionalTestUtils.readAll(new FileInputStream(new File(folder, "root_user.cfg")));
    assertTrue(systemPerm.contains("grant System.ALTER_USER -s -u root"));
    assertTrue(systemPerm.contains("grant Table.READ -t " + MetadataTable.NAME + " -u root"));
    assertFalse(systemPerm.contains("grant Table.DROP -t " + MetadataTable.NAME + " -u root"));
  }

  /**
   * fate Operations performed on the Manager FaTE system. Usage: fate [options] [<txId>...]
   * Options: -c, --cancel <txId>... Cancel new or submitted FaTE transactions Default: false -d,
   * --delete <txId>... Delete locks associated with transactions (Requires Manager to be down)
   * Default: false -f, --fail <txId>... Transition FaTE transaction status to FAILED_IN_PROGRESS
   * (requires Manager to be down) Default: false -j, --json Print transactions in json Default:
   * false -p, --print, -print, -l, --list, -list [<txId>...] Print information about FaTE
   * transactions. Print only the 'txId's specified or print all transactions if empty. Use -s to
   * only print certain states. Default: false -s, --state 'state'... Print transactions in the
   * state(s) {NEW, IN_PROGRESS, FAILED_IN_PROGRESS, FAILED, SUCCESSFUL} Default: [] --summary Print
   * a summary of all FaTE transactions Default: false
   */
  @Test
  public void testFateSummaryCommandWithSlowCompaction() throws Exception {
    log.info("testFateSummaryCommanWithSlowCompaction...");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String namespace = "ns1";
      final String table = namespace + "." + getUniqueNames(1)[0];
      client.namespaceOperations().create(namespace);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("h"));
      splits.add(new Text("m"));
      splits.add(new Text("r"));
      splits.add(new Text("w"));
      IteratorSetting is = new IteratorSetting(1, SlowIterator.class);
      is.addOption("sleepTime", "10000");

      NewTableConfiguration cfg = new NewTableConfiguration();
      cfg.withSplits(splits);
      cfg.attachIterator(is, EnumSet.of(IteratorUtil.IteratorScope.majc));
      client.tableOperations().create(table, cfg);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);
      client.tableOperations().flush(table);

      // validate blank report, compactions have not started yet
      MiniAccumuloClusterImpl.ProcessInfo p = getCluster().exec(Admin.class, "fate", "--summary",
          "-j", "-s", "NEW", "-s", "IN_PROGRESS", "-s", "FAILED");
      assertEquals(0, p.getProcess().waitFor());
      String result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      FateSummaryReport report = FateSummaryReport.fromJson(result);
      assertNotNull(report);
      assertNotEquals(0, report.getReportTime());
      Set<String> expected = new HashSet<>();
      expected.add("FAILED");
      expected.add("IN_PROGRESS");
      expected.add("NEW");
      assertEquals(expected, report.getStatusFilterNames());
      assertEquals(Map.of(), report.getStatusCounts());
      assertEquals(Map.of(), report.getStepCounts());
      assertEquals(Map.of(), report.getCmdCounts());

      // create Fate transactions
      client.tableOperations().compact(table, null, null, false, false);
      client.tableOperations().compact(table, null, null, false, false);

      // validate no filters
      p = getCluster().exec(Admin.class, "fate", "--summary", "-j");
      assertEquals(0, p.getProcess().waitFor());
      result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      report = FateSummaryReport.fromJson(result);
      assertNotNull(report);
      assertNotEquals(0, report.getReportTime());
      assertEquals(Set.of(), report.getStatusFilterNames());
      assertFalse(report.getStatusCounts().isEmpty());
      assertFalse(report.getStepCounts().isEmpty());
      assertFalse(report.getCmdCounts().isEmpty());
      assertEquals(2, report.getFateDetails().size());
      ArrayList<String> txns = new ArrayList<>();
      report.getFateDetails().forEach((d) -> txns.add(d.getTxnId()));
      assertEquals(2, txns.size());

      // validate tx ids
      p = getCluster().exec(Admin.class, "fate", txns.get(0), txns.get(1), "--summary", "-j");
      assertEquals(0, p.getProcess().waitFor());
      result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      report = FateSummaryReport.fromJson(result);
      assertNotNull(report);
      assertNotEquals(0, report.getReportTime());
      assertEquals(Set.of(), report.getStatusFilterNames());
      assertFalse(report.getStatusCounts().isEmpty());
      assertFalse(report.getStepCounts().isEmpty());
      assertFalse(report.getCmdCounts().isEmpty());
      assertEquals(2, report.getFateDetails().size());

      // validate filter by including only FAILED transactions, should be none
      p = getCluster().exec(Admin.class, "fate", "--summary", "-j", "-s", "FAILED");
      assertEquals(0, p.getProcess().waitFor());
      result = p.readStdOut();
      result = result.substring(result.indexOf("{"), result.lastIndexOf("}") + 1);
      report = FateSummaryReport.fromJson(result);
      assertNotNull(report);
      assertNotEquals(0, report.getReportTime());
      assertEquals(Set.of("FAILED"), report.getStatusFilterNames());
      assertFalse(report.getStatusCounts().isEmpty());
      assertFalse(report.getStepCounts().isEmpty());
      assertFalse(report.getCmdCounts().isEmpty());
      assertEquals(0, report.getFateDetails().size());
    }
  }

  /**
   * listInstances list Accumulo instances in zookeeper Usage: listInstances [options] Options:
   * --print-all print information for all instances, not just those with names Default: false
   * --print-errors display errors while listing instances Default: false
   */
  @Test
  public void testListInstances() {
    log.info("testListInstances...");
  }

  /**
   * locks List or delete Tablet Server locks. Default with no arguments is to list the locks.
   * Usage: locks [options] Options: -delete specify a tablet server lock to delete
   */
  @Test
  public void testLocks() {
    log.info("testLocks...");
  }

  /**
   * ping Ping tablet servers. If no arguments, pings all. Usage: ping {'host' ... }
   */
  @Test
  public void testPing() {
    log.info("testPing...");
  }

  /**
   * restoreZoo Restore Zookeeper data from a file. Usage: restoreZoo [options] Options: --file
   * --overwrite Default: false
   */
  @Test
  public void testRestoreZoo() {
    log.info("testRestoreZoo...");
  }

  /**
   * randomizeVolumes Randomizing tablet directories is deprecated and now does nothing. Accumulo
   * now always calls the volume chooser for each file created by a tablet, so its no longer
   * necessary. Usage: randomizeVolumes [options] Options: -t table to update
   */
  @Test
  public void testRandomizeVolumes() {
    log.info("testRandomizeVolumes...");
  }

  /**
   * stop stop the tablet server on the given hosts Usage: stop 'host' {<host> ... }
   */
  @Test
  public void testStop() {
    log.info("testStop...");
  }

  /**
   * stopAll stop all tablet servers and the manager Usage: stopAll
   */
  @Test
  public void testStopAll() {
    log.info("testStopAll...");
  }

  /**
   * stopManager stop the manager Usage: stopManager
   */
  @Test
  public void testStopManager() {
    log.info("testStopManager...");
  }

  /**
   * stopMaster stop the master (DEPRECATED -- use stopManager instead) Usage: stopMaster
   */
  @Test
  public void testStopMaster() {
    log.info("testStopMaster...");
  }

  /**
   * verifyTabletAssigns Verify all Tablets are assigned to tablet servers Usage:
   * verifyTabletAssigns [options] Options: -v, --verbose verbose mode (prints locations of tablets)
   * Default: false
   */
  @Test
  public void testVerifyAssigns() {
    log.info("testVerifyAssigns...");
  }

  /**
   * volumes Accumulo volume utility Usage: volumes [options] Options: -l, --list list volumes
   * currently in use Default: false
   */
  @Test
  public void testVolumes() {
    log.info("testVolumes...");
  }

}
