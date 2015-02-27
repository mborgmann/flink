package org.apache.flink.examples.java.array.util;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BsqReader extends FileInputFormat<Line> {

	private static final Logger LOG = LoggerFactory.getLogger(BsqReader.class);

	protected static final String DEFLATE_SUFFIX = ".bsq";
	private int numberOfSamples;
	private int numberOfLines;
	private int numberOfBands;

	private int currentBand = 0;
	private int currentLineOfBand = 0;

	public void setFilePath(String filePath) {
		// Do parent stuff
		super.setFilePath(filePath);

		// Load HDR Data
		this.loadHDRData();
	}

	public void setFilePath(Path filePath) {
		// Do parent stuff
		super.setFilePath(filePath);

		// Load HDR Data
		this.loadHDRData();
	}


	public void configure(Configuration parameters) {
		super.configure(parameters);

		// This way we get each band in a split
		// ATTENTION: This assumes there are always six band per file
		this.numSplits = 6;
	}


	private void loadHDRData() {
		// throw new IllegalArgumentException("File path may not be null.");

		String hdrFilePath = this.filePath.toString().replace(".bsq", ".hdr");

		try {
			File hdrFile = new File(hdrFilePath);
			FileReader fileReader = new FileReader(hdrFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {

				if (line.startsWith("samples")) {
					this.numberOfSamples = Integer.parseInt(line.replaceAll("[\\D]", ""));
					LOG.info("Loading " + this.filePath.getName() + " - Samples: " + this.numberOfSamples);
				}

				if (line.startsWith("lines")) {
					this.numberOfLines = Integer.parseInt(line.replaceAll("[\\D]", ""));
					LOG.info("Loading " + this.filePath.getName() + " - Lines: " + this.numberOfLines);
				}

				if (line.startsWith("bands")) {
					this.numberOfBands = Integer.parseInt(line.replaceAll("[\\D]", ""));
					LOG.info("Loading " + this.filePath.getName() + " - Bands: " + this.numberOfBands);
				}

				// TODO Bandnames could be important

			}
			fileReader.close();
		} catch (IOException e) {
			throw new IllegalArgumentException("HDR File could not be found.");
		}

	}

	protected boolean acceptFile(FileStatus fileStatus) {
		final String name = fileStatus.getPath().getName();
		return !name.startsWith("_") && !name.startsWith(".") && name.endsWith(DEFLATE_SUFFIX);
	}


	@Override
	public boolean reachedEnd() throws IOException {

		if (this.currentLineOfBand >= this.numberOfLines) {
			LOG.info("Loading " + this.filePath.getName() + " - Finished Band: " + this.currentBand);
			return true;
		}

		return false;
	}

	@Override
	public Line nextRecord(Line reuse) throws IOException {

		this.currentBand = getBandNumber();

		// Last Line of Band reached
		if (this.currentLineOfBand >= this.numberOfLines) {
			LOG.info("Loading " + this.filePath.getName() + " - Finished Band: " + this.currentBand);

			return null;
		}

		short[] lineData = new short[this.numberOfSamples];
		byte[]	lineDataAsBytes = new byte[lineData.length*2];

		if (this.currentLineOfBand % 100 == 0) {
			LOG.info("Loading " + this.filePath.getName() + " - Band: " + this.currentBand +
					" - Line: " + this.currentLineOfBand + " / " + this.numberOfLines);
		}

		this.stream.read(lineDataAsBytes);

		ByteBuffer.wrap(lineDataAsBytes).order(ByteOrder.LITTLE_ENDIAN).
				asShortBuffer().get(lineData);

		// Create Line
		reuse = new Line(
				this.filePath.getName(),
				this.currentBand,
				this.currentLineOfBand,
				lineData);


		// Set Pointer to next Line
		this.currentLineOfBand++;
		return reuse;
	}

	private int getBandNumber() {
		return (int) (this.splitStart / this.splitLength);
	}
}