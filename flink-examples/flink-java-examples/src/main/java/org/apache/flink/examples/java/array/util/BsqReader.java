package org.apache.flink.examples.java.array.util;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BsqReader extends FileInputFormat<Line> {

	private Logger LOG = LoggerFactory.getLogger(BsqReader.class);

	private boolean hdrLoaded = false;
	private boolean splitDataProcessed = false;
	private int splitNumber;

	private int startingLine;						// Debugging
	private int startingBand;						// Debugging

	private String bsqFileName;
	private int numberOfSamples;
	private int numberOfLines;
	private int numberOfBands;

	private int currentPixel;
	private int lastPixelToRead;

	private boolean loadHDRData() throws IOException {

		if (!this.bsqFileName.toString().endsWith(".bsq")) {
			throw new IOException("Not an bsq File: " + this.bsqFileName.toString());
		}

		Path hdrFilePath;
		if (this.filePath.toString().endsWith(".bsq")) {
			// Case single file to process
			hdrFilePath = new Path(this.filePath.toString().replace(".bsq", ".hdr"));
		} else {
			// Case Directory to process
			hdrFilePath = new Path(this.filePath + "/" + this.bsqFileName.replace(".bsq", ".hdr"));
		}

		System.out.println("Input " + this.splitNumber + " - Processing hdr-File: " + hdrFilePath.toString());

		try {

			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdrFilePath.getFileSystem().open(hdrFilePath)));

			String line;
			while ((line = bufferedReader.readLine()) != null) {

				if (line.startsWith("samples")) {
					this.numberOfSamples = Integer.parseInt(line.replaceAll("[\\D]", ""));
					System.out.println(this.splitNumber + " - Loading " + this.bsqFileName + " - Samples: " + this.numberOfSamples);
				}

				if (line.startsWith("lines")) {
					this.numberOfLines = Integer.parseInt(line.replaceAll("[\\D]", ""));
					System.out.println(this.splitNumber + " - Loading " + this.bsqFileName + " - Lines: " + this.numberOfLines);
				}

				if (line.startsWith("bands")) {
					this.numberOfBands = Integer.parseInt(line.replaceAll("[\\D]", ""));
					System.out.println(this.splitNumber + " - Loading " + this.bsqFileName + " - Bands: " + this.numberOfBands);
				}

			}
			bufferedReader.close();
		} catch (IOException e) {
			throw new IllegalArgumentException("HDR File could not be found. File: " + hdrFilePath);
		}

		this.hdrLoaded = true;
		return true;
	}


	private void loadSplitData() {

		/**
		 * TODO This is just a workaround
		 *
		 * Determining if we are starting in the middle of byte, if so, we skip half a pixel
		 */
		if (this.splitStart % 2 != 0) {
			this.splitStart++;
			try {
				this.stream.seek(this.splitStart);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// When reading first pixel no values are set yet
		this.currentPixel = (int)(this.splitStart / 2);
		this.lastPixelToRead = (int)((this.splitStart + this.splitLength) / 2) - 1;

		// Debugging
		System.out.println(this.splitNumber + " - First Position: " + this.getCurrentBand() + " - " + this.getCurrentLine());
		this.startingBand = this.getCurrentBand();
		this.startingLine = this.getCurrentLine();

		this.splitDataProcessed = true;
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


	private float getPercentageDone() {
		float pixelsToProcess = (this.splitLength / 2);
		float pixelsDone =  (this.currentPixel - this.splitStart / 2);

		return 100*(pixelsDone / pixelsToProcess);
	}


	protected boolean acceptFile(FileStatus fileStatus) {
		final String name = fileStatus.getPath().getName();
		return !name.startsWith("_") && !name.startsWith(".") && name.endsWith(".bsq");
	}


	@Override
	public boolean reachedEnd() throws IOException {

		if (this.currentPixel > this.lastPixelToRead) {
			LOG.error("Finished Input: " + this.splitNumber + " - From File: " + this.bsqFileName.toString() + " - Stream Start-Length: " + this.splitStart + " - " + this.splitLength);
			LOG.error("Finished Input: " + this.splitNumber + " - From File: " + this.bsqFileName.toString() + " - Current Pixel - Last Pixel: " + this.currentPixel + " - " + this.lastPixelToRead);
			LOG.error("Finished Input: " + this.splitNumber + " - From File: " + this.bsqFileName.toString() + " - First Position: " + this.startingBand + " - " + this.startingLine);
			LOG.error("Finished Input: " + this.splitNumber + " - From File: " + this.bsqFileName.toString() + " - Last Position: " + this.getCurrentBand() + " - " + this.getCurrentLine());
			return true;
		};

		return false;
	}

	@Override
	public Line nextRecord(Line reuse) throws IOException {


		// Collect params for line
		int startingPixel = this.getCurrentLineStart();
		int endingPixel = this.getCurrentLineEnd();
		int bandNumber = this.getCurrentBand();
		int lineNumber = this.getCurrentLine();
		String fileName = this.bsqFileName.toString();
		int pixelsToRead = endingPixel - startingPixel + 1;

		if (pixelsToRead < 1) {
			LOG.error(this.splitNumber + "No Pixels to read! Start Position: " + this.startingBand + " - " + this.startingLine);
			LOG.error(this.splitNumber + "No Pixels to read! Last Position: " + this.getCurrentBand() + " - " + this.getCurrentLine());
			this.currentPixel = endingPixel + 1;
			return null;
		}


		short[] lineData = new short[pixelsToRead];

		byte[] lineDataAsBytes = new byte[lineData.length*2];

		if (lineNumber % 100 == 0) {
			LOG.info(this.splitNumber + " " + String.format("%.2f", this.getPercentageDone()) + " "
					+ fileName + " - Band: " + bandNumber +
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


	public void open(FileInputSplit fileSplit) throws IOException {
		super.open(fileSplit);

		this.bsqFileName = fileSplit.getPath().getName();
		System.out.println("|||||||||||| Opening input split " + fileSplit.getPath() + " - " + fileSplit.getSplitNumber() + " [" + this.splitStart + "," + this.splitLength + "]");

		this.splitNumber = fileSplit.getSplitNumber();

		// Check for hdrData
		this.loadHDRData();
		this.loadSplitData();
	}

}