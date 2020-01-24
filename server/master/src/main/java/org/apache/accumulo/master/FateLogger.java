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
package org.apache.accumulo.master;

import org.apache.accumulo.fate.FateTxId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface FateLogger {
  Logger FateLogger = LoggerFactory.getLogger(FateLogger.class);

  default void FateBegin(long tid) {
    FateLogger.info("{}: BEGIN Fate Transaction", FateTxId.formatTid(tid));
  }

  default void FateInfo(long tid, String msg) {
    FateLogger.info("{}:   {}", FateTxId.formatTid(tid), msg);
  }

  default void FateWarn(long tid, String msg) {
    FateLogger.warn("{}:   {}", FateTxId.formatTid(tid), msg);
  }

  default void FateEnd(long tid) {
    FateLogger.info("{}: END Fate Transaction", FateTxId.formatTid(tid));
    FateLogger.info("");
  }

  default void FateEnd(long tid, String msg) {
    FateLogger.info("{}:   {}", FateTxId.formatTid(tid), msg);
    FateLogger.info("{}: END Fate Transaction", FateTxId.formatTid(tid));
  }


  default void FateGoal(long tid, String msg) {
    FateLogger.info("{}: Goal: {}", FateTxId.formatTid(tid), msg);
    FateLogger.info("{}: Status:", FateTxId.formatTid(tid));
  }

  default void FatePermissionError(long tid) {
    FateLogger.error("{}:   Insufficient permissions to perform operation", FateTxId.formatTid(tid));
    FateEnd(tid);
  }

  default void FateError(long tid, String msg) {
    FateLogger.error("{}:   {}", FateTxId.formatTid(tid), msg);
    FateEnd(tid);
  }
}
