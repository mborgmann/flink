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



package org.apache.flink.examples.java.array;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.examples.java.array.util.BandSequentialFile;
import org.apache.flink.examples.java.array.util.Line;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.List;

public class Histogram {

	static String bsqFileToProcess = "227064_000202_BLA_SR";
	static String outputPath = "/home/moritz/Projekte/Studienarbeit/Flink/result.csv";


	public static void main(String[] args) throws Exception {

		System.out.println("Starting Histogram: " + new Date());
		long startTime = System.currentTimeMillis();


		// Reading a file
		BandSequentialFile bsq = new BandSequentialFile(bsqFileToProcess);
		// List<Line> bsqAsLines = bsq.getBandAsLines(3);
		List<Line> bsqAsLines = bsq.getBsqAsLines();
		System.out.println("Number of Lines to process: " + bsqAsLines.size());

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Line> lines = env.fromCollection(bsqAsLines);

		DataSet<Tuple4<String, Integer, Integer, Integer>> counts =
				lines
						.flatMap(new Counter())
						.groupBy(0, 1, 2)
						.sum(3);

		counts.writeAsCsv(outputPath, "\n", "\t", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		// counts.print();


		// execute program
		// env.execute("Histogram");

		System.out.println("Consumed Time: " + ((System.currentTimeMillis() - startTime) / 1000));
		System.out.println("Finished Histogram " + new Date());
	}

	public static final class Counter implements FlatMapFunction<Line, Tuple4<String, Integer, Integer, Integer>> {

		int bucketSize = 25;

		@Override
		public void flatMap(Line value, Collector<Tuple4<String, Integer, Integer, Integer>> out) throws Exception {

			for (short pixel : value.getLineData()) {
				if (pixel == -9999) {
					continue;
				}
				if (pixel < 0) {
					out.collect(new Tuple4<String, Integer, Integer, Integer>(
							value.getFileName(),
							value.getBand(),
							valueToBucket(0),
							1));
					continue;
				}
				if (pixel > 10000) {
					continue;
				}
				out.collect(new Tuple4<String, Integer, Integer, Integer>(
						value.getFileName(),
						value.getBand(),
						valueToBucket(pixel),
						1));
			}
		}

		private int valueToBucket(int value) {

			int bucketNumber = value / bucketSize;
			return (bucketNumber * bucketSize);

		}
	}

}
