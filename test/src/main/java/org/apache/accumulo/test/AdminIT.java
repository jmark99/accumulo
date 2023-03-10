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


//Usage: accumulo admin [options] [command] [command options]
//  Options:
//    -auths, --auths
//      the authorizations to use when reading or writing
//      Default: <empty string>
//    -c, --config-file
//      Read the given client config file. If omitted, the classpath will be
//      searched for file named accumulo-client.properties
//    -f, --force
//      force the given server to stop by removing its lock
//      Default: false
//    -h, -?, --help, -help
//
//    --password
//      connection password (can be specified as '<password>',
//      'pass:<password>', 'file:<local file containing the password>' or
//      'env:<variable containing the pass>')
//    --trace
//      turn on distributed tracing
//      Default: false
//    -u, --user
//      Connection user
//    -o
//      Overrides property in accumulo-client.properties. Expected format: -o
//      <key>=<value>
//      Default: []
  
//  Commands:
//    changeSecret      Changes the unique secret given to the instance that all
//            servers must know.
//      Usage: changeSecret
//
//    checkTablets      print tablets that are offline in online tables
//      Usage: checkTablets [options]
//        Options:
//          --fixFiles
//            Remove dangling file pointers
//            Default: false
//          -t, --table
//            Table to check, if not set checks all tables
//
//    deleteZooInstance      Deletes specific instance name or id from zookeeper
//            or cleans up all old instances.
//      Usage: deleteZooInstance [options]
//        Options:
//          -c, --clean
//            Cleans Zookeeper by deleting all old instances. This will not
//            delete the instance pointed to by the local accumulo.properties
//            file
//            Default: false
//          -i, --instance
//            the instance name or id to delete
//          --password
//            The system secret, if different than instance.secret in
//            accumulo.properties
//
//    dumpConfig      print out non-default configuration settings
//      Usage: dumpConfig [options]
//        Options:
//          -a, --all
//            print the system and all table configurations
//            Default: false
//          -d, --directory
//            directory to place config files
//          -n, --namespaces
//            print the namespace configuration
//            Default: false
//          -s, --system
//            print the system configuration
//            Default: false
//          -t, --tables
//            print per-table configuration
//            Default: []
//          -u, --users
//            print users and their authorizations and permissions
//            Default: false
//
//    fate      Operations performed on the Manager FaTE system.
//      Usage: fate [options] [<txId>...]
//        Options:
//          -c, --cancel
//            <txId>... Cancel new or submitted FaTE transactions
//            Default: false
//          -d, --delete
//            <txId>... Delete locks associated with transactions (Requires
//            Manager to be down)
//            Default: false
//          -f, --fail
//            <txId>... Transition FaTE transaction status to FAILED_IN_PROGRESS
//            (requires Manager to be down)
//            Default: false
//          -j, --json
//            Print transactions in json
//            Default: false
//          -p, --print, -print, -l, --list, -list
//            [<txId>...] Print information about FaTE transactions. Print only
//            the 'txId's specified or print all transactions if empty. Use -s
//            to only print certain states.
//            Default: false
//          -s, --state
//            <state>... Print transactions in the state(s) {NEW, IN_PROGRESS,
//            FAILED_IN_PROGRESS, FAILED, SUCCESSFUL}
//            Default: []
//          --summary
//            Print a summary of all FaTE transactions
//            Default: false
//
//    listInstances      list Accumulo instances in zookeeper
//      Usage: listInstances [options]
//        Options:
//          --print-all
//            print information for all instances, not just those with names
//            Default: false
//          --print-errors
//            display errors while listing instances
//            Default: false
//
//    locks      List or delete Tablet Server locks. Default with no arguments
//            is to list the locks.
//      Usage: locks [options]
//        Options:
//          -delete
//            specify a tablet server lock to delete
//
//    ping      Ping tablet servers.  If no arguments, pings all.
//      Usage: ping {<host> ... }
//
//    restoreZoo      Restore Zookeeper data from a file.
//      Usage: restoreZoo [options]
//        Options:
//          --file
//
//          --overwrite
//            Default: false
//
//    randomizeVolumes      Randomizing tablet directories is deprecated and now
//            does nothing. Accumulo now always calls the volume chooser for
//            each file created by a tablet, so its no longer necessary.
//      Usage: randomizeVolumes [options]
//        Options:
//        * -t
//            table to update
//
//    stop      stop the tablet server on the given hosts
//      Usage: stop <host> {<host> ... }
//
//    stopAll      stop all tablet servers and the manager
//      Usage: stopAll
//
//    stopManager      stop the manager
//      Usage: stopManager
//
//    stopMaster      stop the master (DEPRECATED -- use stopManager instead)
//      Usage: stopMaster
//
//    verifyTabletAssigns      Verify all Tablets are assigned to tablet servers
//      Usage: verifyTabletAssigns [options]
//        Options:
//          -v, --verbose
//            verbose mode (prints locations of tablets)
//            Default: false
//
//    volumes      Accumulo volume utility
//      Usage: volumes [options]
//        Options:
//          -l, --list
//            list volumes currently in use
//            Default: false

}
