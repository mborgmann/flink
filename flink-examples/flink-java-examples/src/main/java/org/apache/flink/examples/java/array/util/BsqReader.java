package org.apache.flink.examples.java.array.util;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileStatus;
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

	private boolean hdrLoaded = false;
	private boolean splitDataProcessed = false;

	private int numberOfSamples;
	private int numberOfLines;
	private int numberOfBands;

	private int currentPixel;
	private int lastPixelToRead;

	private void loadHDRData() {

		if (this.hdrLoaded) {
			return;
		}

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
			throw new IllegalArgumentException("HDR File could not be found. File: " + this.filePath.getName());
		}

		this.hdrLoaded = true;
	}


	private void loadSplitData() {
		// When reading first pixel no values are set yet
		if (!this.splitDataProcessed) {
			this.currentPixel = (int)(this.splitStart / 2);
			this.lastPixelToRead = (int)((this.splitStart + this.splitLength) / 2) - 1;

			LOG.info("First Position: " + this.getCurrentBand() + " - " + this.getCurrentLine());

			this.splitDataProcessed = true;
		}
	}

	/**
	 * @param bandNumber The band number
	 * @param lineNumber The line corresponding to the band
	 * @return The absolut pixel in bsq
	 */
	private int getLastPixelInLine(int bandNumber, int lineNumber) {
		return getFirstPixelOfBand(bandNumber) + ((lineNumber+1) * this.numberOfSamples) - 1;
	}

	// verifiziert
	private int getBandNumberInFile(int pixel) {
		return pixel / (this.numberOfLines * this.numberOfSamples);
	}

	/**
	 * @param bandNumber The band number
	 * @param pixel The absolut pixel
	 * @return which line the pixel is in, corresponding to the band
	 */
	private int getLineNumberInBand(int bandNumber, int pixel) {
		int positionInBand = pixel - this.getFirstPixelOfBand(bandNumber);
		return positionInBand / this.numberOfSamples;
	}

	// verifiziert
	private int getFirstPixelOfBand(int bandNumber) {
		return bandNumber * this.numberOfLines * this.numberOfSamples;
	}

	private int getFirstPixelOfLineInBand(int bandNumber, int lineNumber) {
		return getFirstPixelOfBand(bandNumber) + lineNumber * this.numberOfSamples;
	}

	/**
	 * @return The first pixel that should be read into the current line
	 */
	private int getCurrentLineStart() {

		int currentBand = this.getBandNumberInFile(this.currentPixel);
		int currentLine = this.getLineNumberInBand(currentBand, this.currentPixel);

		int firstPixelOfLine = this.getFirstPixelOfLineInBand(currentBand, currentLine);

		return this.currentPixel > firstPixelOfLine ? this.currentPixel : firstPixelOfLine;
	}

	/**
	 * @return The last pixel that should be read into the current line
	 */
	private int getCurrentLineEnd() {
		int lastPixel = this.getLastPixelInLine(
				this.getCurrentBand(),
				this.getLineNumberInBand(this.getCurrentBand(), this.currentPixel));

		return lastPixel < this.lastPixelToRead ? lastPixel : this.lastPixelToRead;
	}

	/**
	 * @return The current band that the current line belongs to
	 */
	private int getCurrentBand() {
		return this.getBandNumberInFile(this.currentPixel);
	}

	/**
	 * @return The current line relative to band
	 */
	private int getCurrentLine() {
		return this.getLineNumberInBand(this.getCurrentBand(), this.currentPixel);
	}




	protected boolean acceptFile(FileStatus fileStatus) {
		final String name = fileStatus.getPath().getName();
		return !name.startsWith("_") && !name.startsWith(".") && name.endsWith(".bsq");
	}


	@Override
	public boolean reachedEnd() throws IOException {

		this.loadHDRData();
		this.loadSplitData();

		if (this.currentPixel > this.lastPixelToRead) {
			LOG.info("Finished read this Input. Last Position: " + this.getCurrentBand() + " - " + this.getCurrentLine());
			return true;
		};

		return false;
	}

	@Override
	public Line nextRecord(Line reuse) throws IOException {

		// Check for hdrData
		this.loadHDRData();
		this.loadSplitData();

		// Collect params for line
		int startingPixel = this.getCurrentLineStart();
		int endingPixel = this.getCurrentLineEnd();
		int bandNumber = this.getCurrentBand();
		int lineNumber = this.getCurrentLine();
		String fileName = this.filePath.getName();
		int pixelsToRead = endingPixel - startingPixel + 1;

		if (pixelsToRead < 1) {
			LOG.error("No Pixels to read! Last Position: " + this.getCurrentBand() + " - " + this.getCurrentLine());
			return null;
		}

		short[] lineData = new short[pixelsToRead];

		byte[] lineDataAsBytes = new byte[lineData.length*2];

		if (lineNumber % 100 == 0) {
			LOG.info("Loading " + fileName + " - Band: " + bandNumber +
					" - Line: " + lineNumber + " / " + this.numberOfLines);
		}

		this.stream.read(lineDataAsBytes);

		ByteBuffer.wrap(lineDataAsBytes).order(ByteOrder.LITTLE_ENDIAN).
				asShortBuffer().get(lineData);

		// Create Line
		reuse = new Line(
				fileName,
				bandNumber,
				lineNumber,
				lineData);

		// Set Pointer to next Pixel that was not processed
		this.currentPixel = endingPixel + 1;

		return reuse;
	}

}