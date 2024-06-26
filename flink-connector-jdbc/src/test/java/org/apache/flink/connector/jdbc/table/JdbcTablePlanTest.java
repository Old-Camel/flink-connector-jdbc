/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.rules.TestName;

/** Plan tests for JDBC connector, for example, testing projection push down. */
public class JdbcTablePlanTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    private TestInfo testInfo;

    @BeforeEach
    public void setup(TestInfo testInfo) {
        this.testInfo = testInfo;
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE jdbc ("
                                + "id BIGINT,"
                                + "timestamp6_col TIMESTAMP(6),"
                                + "timestamp9_col TIMESTAMP(9),"
                                + "time_col TIME,"
                                + "real_col FLOAT,"
                                + "double_col DOUBLE,"
                                + "decimal_col DECIMAL(10, 4)"
                                + ") WITH ("
                                + "  'connector'='jdbc',"
                                + "  'url'='jdbc:derby:memory:test',"
                                + "  'table-name'='test_table'"
                                + ")");
    }

    @Test
    public void testProjectionPushDown() {
        util.verifyExecPlan("SELECT decimal_col, timestamp9_col, id FROM jdbc");
    }

    @Test
    public void testLimitPushDown() {
        util.verifyExecPlan("SELECT id, time_col FROM jdbc LIMIT 3");
    }

    @Test
    public void testFilterPushdown() {
        util.verifyExecPlan(
                "SELECT id, time_col, real_col FROM jdbc WHERE id = 900001 AND time_col <> TIME '11:11:11' OR double_col >= -1000.23");
    }

    /**
     * Get the test method name, in order to adapt to {@link TableTestBase} that has not migrated to
     * Junit5. Remove it when dropping support of Flink 1.18.
     */
    public TestName name() {
        return new TestName() {
            @Override
            public String getMethodName() {
                return testInfo.getTestMethod().get().getName();
            }
        };
    }
}
