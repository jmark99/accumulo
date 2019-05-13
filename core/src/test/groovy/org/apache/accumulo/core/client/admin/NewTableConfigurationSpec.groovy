/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.admin;

import org.apache.hadoop.io.Text

import spock.lang.Specification
import spock.lang.Shared

class NewTableConfigurationSpec extends Specification {

    @Shared
    SortedSet<Text> splits

    def setupSpec() {
        splits = new TreeSet<>();
        splits.add(new Text("ccccc"));
        splits.add(new Text("aaaaa"));
        splits.add(new Text("ddddd"));
        splits.add(new Text("abcde"));
        splits.add(new Text("bbbbb"));
    }


    def "create a table in offline mode"() {

        given: "a NewTableConfiguration object"
        NewTableConfiguration ntc = new NewTableConfiguration();

        when: "a table is created using createOffline()"
        ntc = new NewTableConfiguration().createOffline()

        then: "retrieving the initial state should return InitialTableState.OFFLINE"
        ntc.getInitialTableState() == InitialTableState.OFFLINE
    }


    def "create a table using default configuration"() {

        given: "a NewTableConfiguration object"
        NewTableConfiguration ntc = new NewTableConfiguration()

        when: "a table is created with the default configuration"
        ntc = new NewTableConfiguration()

        then: "retrieving the initial state should return InitialTableState.ONLINE"
        ntc.getInitialTableState() == InitialTableState.ONLINE
    }


    def "default table contains no splits"() {

        given: "a NewTableConfiguration object"
        NewTableConfiguration ntc = new NewTableConfiguration()

        when: "retrieving a list a splits from the table"
        Collection<Text> splits = ntc.getSplits()

        then: "the splits collection is empty"
        splits.isEmpty()
    }


    def "verify splits are in sorted order"() {

        given: "a table with provided splits at creation"
        NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits)

        and: "a set of splits retrieved from the table"
        Collection<Text> ntcSplits = ntc.getSplits()

        and: "an iterator from the provided splits"
        Iterator<Text> splitIt = splits.iterator()

        and: "an iterator from the retrieved splits"
        Iterator<Text> ntcIt = ntcSplits.iterator()

        when: "the two collections are compared"
        List<Text> retrievedList = new ArrayList(ntcSplits)
        List<Text> originalList = new ArrayList(splits)

        println("****** retrievedList: " + retrievedList)
        println("****** originalList:  " + originalList)

        then: "their order is identical"
            retrievedList == originalList
    }

    def "I should fail"() {
        expect:
        1 + 1 == 3
    }
}
