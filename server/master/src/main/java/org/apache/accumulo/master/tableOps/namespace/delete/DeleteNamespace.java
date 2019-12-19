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
package org.apache.accumulo.master.tableOps.namespace.delete;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.fate.FateTxId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.FateLogger;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;

public class DeleteNamespace extends MasterRepo implements FateLogger {

  private static final long serialVersionUID = 1L;

  private NamespaceId namespaceId;

  public DeleteNamespace(NamespaceId namespaceId) {
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveNamespace(environment, namespaceId, tid, true, true, TableOperation.DELETE);
  }

  @Override
  public Repo<Master> call(long tid, Master environment) {
    environment.getEventCoordinator().event("deleting namespace %s ", namespaceId);
    return new NamespaceCleanUp(namespaceId);
  }

  @Override
  public void undo(long tid, Master environment) {
    Utils.unreserveNamespace(environment, namespaceId, tid, true);
    fLogger.info("{}:\tUndo-ing delete namespace operation", FateTxId.formatTid(tid));
    fLogger.info("{}:END fate transaction", FateTxId.formatTid(tid), tid);
  }

}
