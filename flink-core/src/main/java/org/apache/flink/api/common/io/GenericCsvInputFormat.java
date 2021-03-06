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


package org.apache.flink.api.common.io;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;
import org.apache.flink.types.parser.StringValueParser;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.ArrayList;


public abstract class GenericCsvInputFormat<OT> extends DelimitedInputFormat<OT> {
	
	private static final long serialVersionUID = 1L;
	
	private static final Class<?>[] EMPTY_TYPES = new Class[0];
	
	private static final boolean[] EMPTY_INCLUDED = new boolean[0];
	
	private static final byte[] DEFAULT_FIELD_DELIMITER = new byte[] {','};

	// --------------------------------------------------------------------------------------------
	//  Variables for internal operation.
	//  They are all transient, because we do not want them so be serialized 
	// --------------------------------------------------------------------------------------------

	private transient FieldParser<?>[] fieldParsers;
	
	
	// --------------------------------------------------------------------------------------------
	//  The configuration parameters. Configured on the instance and serialized to be shipped.
	// --------------------------------------------------------------------------------------------
	
	private Class<?>[] fieldTypes = EMPTY_TYPES;
	
	private boolean[] fieldIncluded = EMPTY_INCLUDED;
		
	private byte[] fieldDelim = DEFAULT_FIELD_DELIMITER;
	
	private boolean lenient;
	
	private boolean skipFirstLineAsHeader;

	private boolean quotedStringParsing = false;

	private byte quoteCharacter;
	
	
	// --------------------------------------------------------------------------------------------
	//  Constructors and getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------
	
	protected GenericCsvInputFormat() {
		super();
	}
	
	protected GenericCsvInputFormat(Path filePath) {
		super(filePath);
	}
	
	// --------------------------------------------------------------------------------------------

	public int getNumberOfFieldsTotal() {
		return this.fieldIncluded.length;
	}
	
	public int getNumberOfNonNullFields() {
		return this.fieldTypes.length;
	}

	public byte[] getFieldDelimiter() {
		return fieldDelim;
	}

	public void setFieldDelimiter(byte[] delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}

		this.fieldDelim = delimiter;
	}

	public void setFieldDelimiter(char delimiter) {
		setFieldDelimiter(String.valueOf(delimiter));
	}

	public void setFieldDelimiter(String delimiter) {
		this.fieldDelim = delimiter.getBytes(Charsets.UTF_8);
	}

	public boolean isLenient() {
		return lenient;
	}

	public void setLenient(boolean lenient) {
		this.lenient = lenient;
	}
	
	public boolean isSkippingFirstLineAsHeader() {
		return skipFirstLineAsHeader;
	}

	public void setSkipFirstLineAsHeader(boolean skipFirstLine) {
		this.skipFirstLineAsHeader = skipFirstLine;
	}

	public void enableQuotedStringParsing(char quoteCharacter) {
		quotedStringParsing = true;
		this.quoteCharacter = (byte)quoteCharacter;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected FieldParser<?>[] getFieldParsers() {
		return this.fieldParsers;
	}
	
	protected Class<?>[] getGenericFieldTypes() {
		// check if we are dense, i.e., we read all fields
		if (this.fieldIncluded.length == this.fieldTypes.length) {
			return this.fieldTypes;
		}
		else {
			// sparse type array which we made dense for internal book keeping.
			// create a sparse copy to return
			Class<?>[] types = new Class<?>[this.fieldIncluded.length];
			
			for (int i = 0, k = 0; i < this.fieldIncluded.length; i++) {
				if (this.fieldIncluded[i]) {
					types[i] = this.fieldTypes[k++];
				}
			}
			
			return types;
		}
	}
	
	
	protected void setFieldTypesGeneric(Class<?> ... fieldTypes) {
		if (fieldTypes == null) {
			throw new IllegalArgumentException("Field types must not be null.");
		}
		
		this.fieldIncluded = new boolean[fieldTypes.length];
		ArrayList<Class<?>> types = new ArrayList<Class<?>>();
		
		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<?> type = fieldTypes[i];
			
			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[i] = true;
			}
		}
		
		Class<?>[] denseTypeArray = (Class<?>[]) types.toArray(new Class[types.size()]);
		this.fieldTypes = denseTypeArray;
	}
	
	protected void setFieldsGeneric(int[] sourceFieldIndices, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(sourceFieldIndices);
		Preconditions.checkNotNull(fieldTypes);
		Preconditions.checkArgument(sourceFieldIndices.length == fieldTypes.length,
			"Number of field indices and field types must match.");

		for (int i : sourceFieldIndices) {
			if (i < 0) {
				throw new IllegalArgumentException("Field indices must not be smaller than zero.");
			}
		}

		int largestFieldIndex = Ints.max(sourceFieldIndices);
		this.fieldIncluded = new boolean[largestFieldIndex + 1];
		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<?> type = fieldTypes[i];

			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName()
						+ "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[sourceFieldIndices[i]] = true;
			}
		}

		Class<?>[] denseTypeArray = (Class<?>[]) types.toArray(new Class[types.size()]);
		this.fieldTypes = denseTypeArray;
	}
	
	protected void setFieldsGeneric(boolean[] includedMask, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(includedMask);
		Preconditions.checkNotNull(fieldTypes);

		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if types are valid for included fields
		int typeIndex = 0;
		for (int i = 0; i < includedMask.length; i++) {

			if (includedMask[i]) {
				if (typeIndex > fieldTypes.length - 1) {
					throw new IllegalArgumentException("Missing type for included field " + i + ".");
				}
				Class<?> type = fieldTypes[typeIndex++];

				if (type == null) {
					throw new IllegalArgumentException("Type for included field " + i + " should not be null.");
				} else {
					// check if we support parsers for this type
					if (FieldParser.getParserForType(type) == null) {
						throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
					}
					types.add(type);
				}
			}
		}

		Class<?>[] denseTypeArray = (Class<?>[]) types.toArray(new Class[types.size()]);
		this.fieldTypes = denseTypeArray;
		this.fieldIncluded = includedMask;
	}

	// --------------------------------------------------------------------------------------------
	//  Runtime methods
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		
		// instantiate the parsers
		@SuppressWarnings("unchecked")
		FieldParser<?>[] parsers = new FieldParser[fieldTypes.length];
		
		for (int i = 0; i < fieldTypes.length; i++) {
			if (fieldTypes[i] != null) {
				Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldTypes[i]);
				if (parserType == null) {
					throw new RuntimeException("No parser available for type '" + fieldTypes[i].getName() + "'.");
				}
				

				@SuppressWarnings("unchecked")
				FieldParser<?> p = (FieldParser<?>) InstantiationUtil.instantiate(parserType, FieldParser.class);

				if (this.quotedStringParsing) {
					if (p instanceof StringParser) {
						((StringParser)p).enableQuotedStringParsing(this.quoteCharacter);
					} else if (p instanceof StringValueParser) {
						((StringValueParser)p).enableQuotedStringParsing(this.quoteCharacter);
					}
				}

				parsers[i] = p;
			}
		}
		this.fieldParsers = parsers;
		
		// skip the first line, if we are at the beginning of a file and have the option set
		if (this.skipFirstLineAsHeader && this.splitStart == 0) {
			readLine(); // read and ignore
		}
	}
	
	protected boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes) throws ParseException {
		
		boolean[] fieldIncluded = this.fieldIncluded;
		
		int startPos = offset;
		final int limit = offset + numBytes;
		
		for (int field = 0, output = 0; field < fieldIncluded.length; field++) {
			
			// check valid start position
			if (startPos >= limit) {
				if (lenient) {
					return false;
				} else {
					throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
				}
			}
			
			if (fieldIncluded[field]) {
				// parse field
				FieldParser<Object> parser = (FieldParser<Object>) this.fieldParsers[output];
				Object reuse = holders[output];
				startPos = parser.parseField(bytes, startPos, limit, this.fieldDelim, reuse);
				holders[output] = parser.getLastResult();
				
				// check parse result
				if (startPos < 0) {
					// no good
					if (lenient) {
						return false;
					} else {
						String lineAsString = new String(bytes, offset, numBytes);
						throw new ParseException("Line could not be parsed: '" + lineAsString + "'\n"
								+ "ParserError " + parser.getErrorState() + " \n"
								+ "Expect field types: "+fieldTypesToString() + " \n"
								+ "in file: " + filePath);
					}
				}
				output++;
			}
			else {
				// skip field
				startPos = skipFields(bytes, startPos, limit, this.fieldDelim);
				if (startPos < 0) {
					if (!lenient) {
						String lineAsString = new String(bytes, offset, numBytes);
						throw new ParseException("Line could not be parsed: '" + lineAsString+"'\n"
								+ "Expect field types: "+fieldTypesToString()+" \n"
								+ "in file: "+filePath);
					}
				}
			}
		}
		return true;
	}
	
	private String fieldTypesToString() {
		StringBuilder string = new StringBuilder();
		string.append(this.fieldTypes[0].toString());

		for (int i = 1; i < this.fieldTypes.length; i++) {
			string.append(", ").append(this.fieldTypes[i]);
		}
		
		return string.toString();
	}

	protected int skipFields(byte[] bytes, int startPos, int limit, byte[] delim) {

		int i = startPos;
		byte current;

		final int delimLimit = limit - delim.length + 1;

		if(quotedStringParsing == true && bytes[i] == quoteCharacter) {

			// quoted string parsing enabled and field is quoted
			// search for ending quote character
			while(i < limit && bytes[i] != quoteCharacter) {
				i++;
			}
			i++;

			if (i == limit) {
				// we are at the end of the record
				return limit;
			} else if ( i < delimLimit && FieldParser.delimiterNext(bytes, i, delim)) {
				// we are not at the end, check if delimiter comes next
				return i + delim.length;
			} else {
				// delimiter did not follow end quote. Error...
				return -1;
			}
		} else {
			// field is not quoted
			while(i < delimLimit && !FieldParser.delimiterNext(bytes, i, delim)) {
				i++;
			}

			if (i >= delimLimit) {
				// no delimiter found. We are at the end of the record
				return limit;
			} else {
				// delimiter found.
				return i + delim.length;
			}
		}
	}
}
