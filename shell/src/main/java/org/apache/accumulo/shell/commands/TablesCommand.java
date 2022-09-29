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
package org.apache.accumulo.shell.commands;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.MapUtils;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public class TablesCommand extends Command {

  private Option tableIdOption;
  private Option tableTimeTypeOption;
  private Option sortByTableIdOption;
  private Option disablePaginationOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, IOException, NamespaceNotFoundException {

    final String namespace = cl.hasOption(OptUtil.namespaceOpt().getOpt())
        ? OptUtil.getNamespaceOpt(cl, shellState) : null;
    Map<String,String> tables = shellState.getAccumuloClient().tableOperations().tableIdMap();

    // filter only specified namespace
    tables = Maps.filterKeys(tables, tableName -> namespace == null
        || TableNameUtil.qualify(tableName).getFirst().equals(namespace));

    final boolean sortByTableId = cl.hasOption(sortByTableIdOption.getOpt());
    tables = new TreeMap<>((sortByTableId ? MapUtils.invertMap(tables) : tables));

    Iterator<String> it = Iterators.transform(tables.entrySet().iterator(), entry -> {
      String tableName = String.valueOf(sortByTableId ? entry.getValue() : entry.getKey());
      String tableId = String.valueOf(sortByTableId ? entry.getKey() : entry.getValue());
      String output = String.format("%-20s", tableName);

      if (namespace != null) {
        tableName = TableNameUtil.qualify(tableName).getSecond();
        output = String.format("%-20s", tableName);
      }

      if (cl.getOptions().length > 0) {
        output = output + " ==> ";
      }
      if (cl.hasOption(tableIdOption.getOpt())) {
        output = output + String.format("%9s", tableId);
      }
      if (cl.hasOption(tableTimeTypeOption.getOpt())) {
        TimeType timeType;
        try {
          timeType = shellState.getAccumuloClient().tableOperations().getTimeType(entry.getKey());
        } catch (TableNotFoundException e) {
          throw new IllegalStateException("Failed to retrieve TimeType value for " + tableName);
        }
        output = output + String.format("  [%s]", timeType);
      }
      return output + "\n";
    });

    shellState.printLines(it, !cl.hasOption(disablePaginationOpt.getOpt()));
    return 0;
  }

  @Override
  public String description() {
    return "displays a list of all existing tables";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    tableIdOption =
        new Option("l", "list-ids", false, "display internal table ids along with the table name");
    o.addOption(tableIdOption);
    tableTimeTypeOption =
        new Option("t", "timetype", false, "display the TimeType value along with the table name");
    o.addOption(tableTimeTypeOption);
    sortByTableIdOption = new Option("s", "sort-ids", false, "with -l: sort output by table ids");
    o.addOption(sortByTableIdOption);
    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    o.addOption(disablePaginationOpt);
    o.addOption(OptUtil.namespaceOpt("name of namespace to list only its tables"));
    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }
}
