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


import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BandSequentialFile {

	// Metadata
	private int numberOfBands = 6;
	private int path;
	private int row;
	private int numberOfSamples = 8002;         // TODO Get dynamically
	private int numberOfLines = 7232;           // TODO Get dynamically
	private String fileName;

	// Actual Data
	private short[][] bsqAsShortArray;


	public BandSequentialFile(String fileName) throws IOException {
		System.out.println("Start reading File: " + fileName);

		// TODO Calc Fileparams
		this.fileName = fileName;
		// TODO this.path = ;
		// TODO this.row = ;

		// Open Stream
		File file = new File(this.fileName);

		int pixelsPerBand = (int)file.length() / this.numberOfBands / 2;
		System.out.println("Pixel per Band: " + pixelsPerBand);

		createShortArray(pixelsPerBand, this.numberOfBands);

		System.out.println(this.bsqAsShortArray[2][48000000]);

		readDataToObject(file);


		System.out.println("Finished reading File: " + file);
	}

	public List<Line> bandToLineList(int bandNumber) {

		System.out.println("Preparing Lines from: " + bandNumber);

		List<Line> lines = new ArrayList<Line>(this.numberOfLines);

		for (int i = 0; i < 3000; i++) {
			int from = i * this.numberOfSamples;
			int to = from + numberOfSamples; // final index, exclusive
			short[] lineData = Arrays.copyOfRange(this.bsqAsShortArray[bandNumber], from, to);
			Line newLine = new Line(this.path, this.row, bandNumber, i, lineData);
			lines.add(newLine);
		}

		System.out.println("Created Lines: " + lines.size());

		return lines;
	}



	private void createShortArray(int pixelsPerBand, int numberOfBands) {
		this.bsqAsShortArray = new short[numberOfBands][pixelsPerBand];
	}

	private void readDataToObject(File file) throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream(file));

		for (int i = 0; i < this.bsqAsShortArray.length; i++) {

			System.out.println("Reading Band: " + i);
			byte[] currentBand = new byte[this.bsqAsShortArray[i].length * 2];

			dis.readFully(currentBand, 0, currentBand.length);

			ShortBuffer shortBuffer =
					ByteBuffer.wrap(currentBand).order(ByteOrder.LITTLE_ENDIAN).
							asShortBuffer().get(bsqAsShortArray[i]);
		}

		dis.close();
	}

}
