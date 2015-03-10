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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.examples.java.array.util.BsqReader;
import org.apache.flink.examples.java.array.util.Line;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Histogram {

	public static void main(String[] args) throws Exception {

		String inputPath;
		String outputPath;

		if (args.length == 2) {
			inputPath = args[0];
			outputPath = args[1];
		} else {
			System.err.println("Usage: WordCount <text path> <result path>");
			return;
		}

		long startTime = System.currentTimeMillis();

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Line> lines = env.readFile(new BsqReader(), inputPath);

		// Filename, Band, Bucket, PixelsPerBucket
		DataSet<Tuple4<String, Integer, Integer, Integer>> counts =
				lines
						.flatMap(new Counter())
						.groupBy(0, 1, 2)
						.sum(3);

		// Filename, Bucket, Band1, Band2, Band3, Band4, Band5, Band6
		DataSet<Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> output =
				counts
						.map(new ToCSVMapper())
						.groupBy(0, 1)
						.reduce(new ToCSVReducer());

		output.writeAsCsv(outputPath, "\n", "\t", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// execute program
		env.execute("Histogram");
		System.out.println("Consumed Time: " + ((System.currentTimeMillis() - startTime) / 1000));
	}


	/**
	 * Merges all records of the same bucket and sourcefile
	 */
	public static final class ToCSVReducer implements ReduceFunction<Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {

		private static final Logger LOG = LoggerFactory.getLogger(ToCSVReducer.class);

		@Override
		public Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value1, Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value2) throws Exception {

			// Due to grouping, fileName und bucket should be the same

			String fileName;
			int bucket;

			int[] values = new int[6];

			if (!value1.f0.equals(value2.f0)) LOG.error("Found inequal filenames: " + value1.f0 + " - " + value2.f0);
			fileName = value1.f0;
			if (!value1.f1.equals(value2.f1)) LOG.error("Found inequal buckets: " + value1.f1 + " - " + value2.f1);
			bucket = value1.f1;
			values[0] = value1.f2 > 0 ? value1.f2 : value2.f2;
			values[1] = value1.f3 > 0 ? value1.f3 : value2.f3;
			values[2] = value1.f4 > 0 ? value1.f4 : value2.f4;
			values[3] = value1.f5 > 0 ? value1.f5 : value2.f5;
			values[4] = value1.f6 > 0 ? value1.f6 : value2.f6;
			values[5] = value1.f7 > 0 ? value1.f7 : value2.f7;

			return new Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
					fileName,
					bucket,
					values[0],
					values[1],
					values[2],
					values[3],
					values[4],
					values[5]
			);
		}
	}

	/**
	 * Maps the band-information to a single column per band (static to 6 bands)
	 */
	public static final class ToCSVMapper implements MapFunction<Tuple4<String, Integer, Integer, Integer>, Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {

		@Override
		public Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple4<String, Integer, Integer, Integer> value) throws Exception {

			// Band, Bucket, band1, band2, band3, band4, band5, band6
			Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out
					= new Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
					(String) value.getField(0),
					(Integer) value.getField(2),
					0,
					0,
					0,
					0,
					0,
					0
			);

			out.setField(value.getField(3), (Integer)value.getField(1)+2);

			return out;
		}

	}


	/**
	 * Maps all pixelvalues of a band to the corresponding buckets
	 * Created Tuples are Filename, Band, Bucket, PixelsInBucket
	 */
	public static final class Counter implements FlatMapFunction<Line, Tuple4<String, Integer, Integer, Integer>> {

		int bucketSize = 25;

		@Override
		public void flatMap(Line value, Collector<Tuple4<String, Integer, Integer, Integer>> out) throws Exception {

			for (short pixel : value.getLineData()) {
				if (pixel == -9999) {
					continue;
				}
				if (pixel < 0) {
					// Not sure if this could be really applied to the given data
					/*
					out.collect(new Tuple4<String, Integer, Integer, Integer>(
							value.getFileName(),
							value.getBand(),
							valueToBucket(0),
							1));
					*/
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
