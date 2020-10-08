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

package org.apache.flink.table.examples.java.connectors.kinesis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * An end-to-end test demonstrating how Flink Kinesis sources and sinks can be used from the Table API.
 */
public class KinesisConnectorTest {

	private static final String CREATE_KINESIS_TABLE_JSON = "" +
		"CREATE TABLE `%1$s` (\n" +
		"  `event_time` TIMESTAMP(3) NOT NULL,\n" +
		"  `name` VARCHAR(32) NOT NULL,\n" +
		"  `age` BIGINT NOT NULL,\n" +
		"  `office` VARCHAR(255) NOT NULL,\n" +
		"  `role` VARCHAR(4) NOT NULL,\n" +
		"  `arrival_time` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,\n" +
		"  `shard_id` VARCHAR(128) NOT NULL METADATA FROM 'shard-id' VIRTUAL,\n" +
		"  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' second\n" +
		") PARTITIONED BY (office, role) WITH (\n" +
		"  'connector' = 'kinesis',\n" +
		"  'stream' = '%1$s',\n" +
		"  'aws.region' = 'us-east-2',\n" +
		"  'scan.stream.initpos' = 'LATEST',\n" +
		"  'sink.partitioner-field-delimiter' = ';',\n" +
		"  'sink.producer.collection-max-count' = '100',\n" +
		"  'format' = 'json',\n" +
		"  'json.timestamp-format.standard' = 'ISO-8601'\n" +
		")";

	private static final String CREATE_KINESIS_TABLE_AVRO = "" +
		"CREATE TABLE `%1$s` (\n" +
		"  `event_time` TIMESTAMP(3) NOT NULL,\n" +
		"  `name` VARCHAR(32) NOT NULL,\n" +
		"  `age` BIGINT NOT NULL,\n" +
		"  `office` VARCHAR(255) NOT NULL,\n" +
		"  `role` VARCHAR(4) NOT NULL,\n" +
		"  `arrival_time` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,\n" +
		"  `shard_id` VARCHAR(128) NOT NULL METADATA FROM 'shard-id' VIRTUAL,\n" +
		"  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' second\n\n" +
		") PARTITIONED BY (office, role) WITH (\n" +
		"  'connector' = 'kinesis',\n" +
		"  'stream' = '%1$s',\n" +
		"  'aws.region' = 'us-east-2',\n" +
		"  'scan.stream.initpos' = 'LATEST',\n" +
		"  'sink.partitioner-field-delimiter' = ';',\n" +
		"  'sink.producer.collection-max-count' = '100',\n" +
		"  'format' = 'avro'\n" +
		")";

	private static final String CREATE_PRINT_TABLE = "" +
		"CREATE TABLE `%1$s` (\n" +
		"  `event_time` TIMESTAMP(3),\n" +
		"  `name` VARCHAR(32),\n" +
		"  `age` BIGINT,\n" +
		"  `office` VARCHAR(255),\n" +
		"  `role` VARCHAR(4)\n" +
		") WITH (\n" +
		"  'connector' = 'print',\n" +
		"  'print-identifier' = '%1$s'\n" +
		")";

	private static final String CREATE_PRINT_TABLE_AGG = "" +
		"CREATE TABLE `%1$s` (\n" +
		"  `start_time` TIMESTAMP(3),\n" +
		"  `distinct_names` BIGINT\n" +
		") WITH (\n" +
		"  'connector' = 'print',\n" +
		"  'print-identifier' = '%1$s'\n" +
		")";

	private static final String COPY_STREAM_DYNAMIC_PARTITION = "" +
		"INSERT INTO `%2$s`\n" +
		"SELECT\n" +
		"  `event_time`,\n" +
		"  `name`,\n" +
		"  `age`,\n" +
		"  `office`,\n" +
		"  `role`\n" +
		"FROM\n" +
		"  `%1$s`";

	private static final String COPY_STREAM_STATIC_PARTITION = "" +
		"INSERT INTO `%2$s` PARTITION (office='BER12',role='SDE2')\n" +
		"SELECT\n" +
		"  `event_time`,\n" +
		"  `name`,\n" +
		"  `age`\n" +
		"FROM\n" +
		"  `%1$s`";

	private static final String COPY_STREAM_MIXED_PARTITION = "" +
		"INSERT INTO `%2$s` PARTITION (role='SDE2') \n" +
		"SELECT\n" +
		"  `event_time`,\n" +
		"  `name`,\n" +
		"  `age`,\n" +
		"  `office`\n" +
		"FROM\n" +
		"  `%1$s`";

	private static final String CREATE_AGGREGATE_VIEW = "" +
		"CREATE VIEW `%2$s` AS \n" +
		"SELECT \n" +
		"  TUMBLE_START(`event_time`, INTERVAL '5' SECOND) AS `start_time`,\n" +
		"  COUNT(DISTINCT `name`) AS `distinct_names`\n" +
		"FROM " +
		"  `%1$s`\n" +
		"GROUP BY\n" +
		"  TUMBLE(`event_time`, INTERVAL '5' SECOND)";

	final StreamExecutionEnvironment senv;
	final StreamTableEnvironment stenv;
	final StatementSet stset;

	private KinesisConnectorTest() {
		final Configuration config = new Configuration();
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build();

		senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		stenv = StreamTableEnvironment.create(senv, settings);
		stset = stenv.createStatementSet();

		senv.setParallelism(2);
	}

	private void executeSql(String query, Object... args) {
		stenv.executeSql(String.format(query, args));
	}

	private void addInsertSql(String query, Object... args) {
		stset.addInsertSql(String.format(query, args));
	}

	private void execute() {
		stset.execute();
	}

	public static void main(String[] args) {
		String workload = args.length != 1 ? "" : args[0];

		switch (workload) {
			case "workload1":
				workload1();
				break;
			case "workload2":
				workload2();
				break;
			case "workload3":
				workload3();
				break;
			default:
				System.err.println("Usage: <program> [workload1|wordkload2]");
				System.exit(-1);
				break;
		}
	}

	public static void workload1() {
		final String sourceStream = "mm-10139-source";
		final String targetStream = "mm-10139-target";
		final String sourceStdout = "source";
		final String targetStdout = "target";

		KinesisConnectorTest program = new KinesisConnectorTest();

		program.executeSql(CREATE_KINESIS_TABLE_JSON, sourceStream);
		program.executeSql(CREATE_KINESIS_TABLE_AVRO, targetStream);
		program.executeSql(CREATE_PRINT_TABLE, sourceStdout);
		program.executeSql(CREATE_PRINT_TABLE, targetStdout);

		program.addInsertSql(COPY_STREAM_MIXED_PARTITION, sourceStream, targetStream);
//		program.addInsertSql(COPY_STREAM_DYNAMIC_PARTITION, sourceStream, sourceStdout);
		program.addInsertSql(COPY_STREAM_DYNAMIC_PARTITION, targetStream, targetStdout);

		program.execute();
	}

	public static void workload2() {
		final String sourceStream = "mm-10139-source";
		final String targetStream = "target-view";
		final String targetStdout = "target";

		KinesisConnectorTest program = new KinesisConnectorTest();

		program.executeSql(CREATE_KINESIS_TABLE_JSON, sourceStream);
		program.executeSql(CREATE_PRINT_TABLE_AGG, targetStdout);
		program.executeSql(CREATE_AGGREGATE_VIEW, sourceStream, targetStream);
		program.addInsertSql(COPY_STREAM_MIXED_PARTITION, targetStream, targetStdout);

		program.execute();
	}

	public static void workload3() {
		final String sourceStream = "mm-10139-source";
		final String sourceStdout = "source";

		KinesisConnectorTest program = new KinesisConnectorTest();

		program.executeSql(CREATE_KINESIS_TABLE_JSON, sourceStream);
		program.executeSql(CREATE_PRINT_TABLE, sourceStdout);

		program.addInsertSql(COPY_STREAM_DYNAMIC_PARTITION, sourceStream, sourceStdout);

		program.execute();
	}
}
