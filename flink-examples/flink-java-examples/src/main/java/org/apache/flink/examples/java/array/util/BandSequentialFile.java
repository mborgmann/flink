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


package org.apache.flink.examples.java.array.util;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BandSequentialFile {

	final String directory = "/media/moritz/flink/Data/";

	// Metadata
	private int numberOfBands;
	private int numberOfSamples;
	private int numberOfLines;
	private String fileName;

	// Data
	private List[] bands;

	public BandSequentialFile(String fileName) throws IOException {
		System.out.println(fileName + " - Start Reading");

		// Get Fileparams
		this.fileName = fileName;

		// Check for hdr file and load params
		File hdrFile = new File(this.directory + this.fileName + ".hdr");
		FileReader fileReader = new FileReader(hdrFile);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line;
		while ((line = bufferedReader.readLine()) != null) {

			if (line.startsWith("samples")) {
				this.numberOfSamples = Integer.parseInt(line.replaceAll("[\\D]", ""));
				System.out.println(fileName + " - Samples: " + this.numberOfSamples);
			}

			if (line.startsWith("lines")) {
				this.numberOfLines = Integer.parseInt(line.replaceAll("[\\D]", ""));
				System.out.println(fileName + " - Lines: " + this.numberOfLines);
			}

			if (line.startsWith("bands")) {
				this.numberOfBands = Integer.parseInt(line.replaceAll("[\\D]", ""));
				System.out.println(fileName + " - Bands: " + this.numberOfBands);
			}

			// TODO Bandnames could be important

		}
		fileReader.close();

		this.bands = new List[this.numberOfBands];

		// Open Stream
		File file = new File(this.directory + this.fileName + ".bsq");

		int pixelsPerBand = (int)file.length() / this.numberOfBands / 2;

		readDataToObject(file);

		System.out.println(this.fileName + " - FINISHED");
	}

	public List<Line> getBandAsLines(int bandNumber) {
		return this.bands[bandNumber];
	}

	public List<Line> getBsqAsLines() {
		List<Line> lines = new ArrayList<Line>(this.numberOfBands * this.numberOfLines);

		for (int i = 0; i < 2; i++) {
			lines.addAll(this.bands[i]);
		}

		return lines;
	}

	private List<Line> shortArrayToLineList(short[] shortArray, int bandNumber) {

		List<Line> lines = new ArrayList<Line>(this.numberOfLines);

		for (int i = 0; i < this.numberOfLines; i++) {
			int from = i * this.numberOfSamples;
			int to = from + this.numberOfSamples; // final index, exclusive
			short[] lineData = Arrays.copyOfRange(shortArray, from, to);
			Line newLine = new Line(this.fileName, bandNumber, i, lineData);
			lines.add(newLine);
		}

		return lines;

	}

	private void readDataToObject(File file) throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream(file));

		for (int i = 0; i < this.numberOfBands; i++) {
			System.out.println(this.fileName + " - Reading Band " + (i+1));
			byte[] currentBand = new byte[this.numberOfLines * this.numberOfSamples * 2];

			dis.readFully(currentBand, 0, currentBand.length);

			short[] bandAsShorts = new short[this.numberOfLines * this.numberOfSamples];

			ShortBuffer shortBuffer =
					ByteBuffer.wrap(currentBand).order(ByteOrder.LITTLE_ENDIAN).
							asShortBuffer().get(bandAsShorts);

			this.bands[i] = shortArrayToLineList(bandAsShorts, i);
		}
		dis.close();

	}

}
