/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.roaringbitmap.RoaringBitmap;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Base class for transform function providing the default implementation for all data types.
 */
public abstract class BaseTransformFunction implements TransformFunction {
  protected static final TransformResultMetadata INT_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.INT, true, false);
  protected static final TransformResultMetadata LONG_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.LONG, true, false);
  protected static final TransformResultMetadata FLOAT_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.FLOAT, true, false);
  protected static final TransformResultMetadata DOUBLE_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.DOUBLE, true, false);
  protected static final TransformResultMetadata BIG_DECIMAL_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BIG_DECIMAL, true, false);
  protected static final TransformResultMetadata BOOLEAN_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BOOLEAN, true, false);
  protected static final TransformResultMetadata TIMESTAMP_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.TIMESTAMP, true, false);
  protected static final TransformResultMetadata STRING_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.STRING, true, false);
  protected static final TransformResultMetadata JSON_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.JSON, true, false);
  protected static final TransformResultMetadata BYTES_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BYTES, true, false);

  protected static final TransformResultMetadata INT_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.INT, false, false);
  protected static final TransformResultMetadata LONG_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.LONG, false, false);
  protected static final TransformResultMetadata FLOAT_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.FLOAT, false, false);
  protected static final TransformResultMetadata DOUBLE_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.DOUBLE, false, false);
  // TODO: Support MV BIG_DECIMAL
  protected static final TransformResultMetadata BIG_DECIMAL_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BIG_DECIMAL, false, false);
  protected static final TransformResultMetadata BOOLEAN_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BOOLEAN, false, false);
  protected static final TransformResultMetadata TIMESTAMP_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.TIMESTAMP, false, false);
  protected static final TransformResultMetadata STRING_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.STRING, false, false);
  protected static final TransformResultMetadata JSON_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.JSON, false, false);

  // These buffers are used to hold the result for different result types. When the subclass overrides a method, it can
  // reuse the buffer for that method. E.g. if transformToIntValuesSV is overridden, the result can be written into
  // _intValuesSV.
  protected int[] _intValuesSV;
  protected long[] _longValuesSV;
  protected float[] _floatValuesSV;
  protected double[] _doubleValuesSV;
  protected BigDecimal[] _bigDecimalValuesSV;
  protected String[] _stringValuesSV;
  protected byte[][] _bytesValuesSV;
  protected int[][] _intValuesMV;
  protected long[][] _longValuesMV;
  protected float[][] _floatValuesMV;
  protected double[][] _doubleValuesMV;
  protected String[][] _stringValuesMV;
  protected byte[][][] _bytesValuesMV;

  protected List<TransformFunction> _arguments;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    _arguments = arguments;
  }

  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] transformToDictIdsSV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pair<RoaringBitmap, int[]> transformToDictIdsSVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToDictIdsSV(projectionBlock));
  }

  @Override
  public int[][] transformToDictIdsMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException("transformToDictIdsMV is not supported for current transform function");
  }

  @Override
  public Pair<RoaringBitmap, int[][]> transformToDictIdsMVWithNull(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException(
        "transformToDictIdsMVWithNull is not supported for current transform function");
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[length];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readIntValues(dictIds, length, _intValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _intValuesSV = transformToIntValuesSV(projectionBlock);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _intValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _intValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _intValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _intValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _intValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as INT", resultDataType));
      }
    }
    return _intValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, int[]> transformToIntValuesSVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToIntValuesSV(projectionBlock));
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_longValuesSV == null) {
      _longValuesSV = new long[length];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readLongValues(dictIds, length, _longValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _longValuesSV = transformToLongValuesSV(projectionBlock);
          break;
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _longValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _longValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _longValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _longValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _longValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as LONG", resultDataType));
      }
    }
    return _longValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, long[]> transformToLongValuesSVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToLongValuesSV(projectionBlock));
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[length];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readFloatValues(dictIds, length, _floatValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _floatValuesSV = transformToFloatValuesSV(projectionBlock);
          break;
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _floatValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _floatValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _floatValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _floatValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _floatValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as FLOAT", resultDataType));
      }
    }
    return _floatValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, float[]> transformToFloatValuesSVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToFloatValuesSV(projectionBlock));
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[length];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readDoubleValues(dictIds, length, _doubleValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _doubleValuesSV = transformToDoubleValuesSV(projectionBlock);
          break;
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _doubleValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _doubleValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _doubleValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _doubleValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _doubleValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as DOUBLE", resultDataType));
      }
    }
    return _doubleValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, double[]> transformToDoubleValuesSVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToDoubleValuesSV(projectionBlock));
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_bigDecimalValuesSV == null) {
      _bigDecimalValuesSV = new BigDecimal[length];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readBigDecimalValues(dictIds, length, _bigDecimalValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _bigDecimalValuesSV = transformToBigDecimalValuesSV(projectionBlock);
          break;
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _bigDecimalValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _bigDecimalValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _bigDecimalValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _bigDecimalValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _bigDecimalValuesSV, length);
          break;
        case BYTES:
          byte[][] bytesValues = transformToBytesValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bytesValues, _bigDecimalValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as BIG_DECIMAL", resultDataType));
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, BigDecimal[]> transformToBigDecimalValuesSVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToBigDecimalValuesSV(projectionBlock));
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[length];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readStringValues(dictIds, length, _stringValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _stringValuesSV = transformToStringValuesSV(projectionBlock);
          break;
        case INT:
          int[] intValues = transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _stringValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _stringValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _stringValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _stringValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _stringValuesSV, length);
          break;
        case BYTES:
          byte[][] bytesValues = transformToBytesValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bytesValues, _stringValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as STRING", resultDataType));
      }
    }
    return _stringValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, String[]> transformToStringValuesSVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToStringValuesSV(projectionBlock));
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_bytesValuesSV == null) {
      _bytesValuesSV = new byte[length][];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(projectionBlock);
      dictionary.readBytesValues(dictIds, length, _bytesValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _bytesValuesSV = transformToBytesValuesSV(projectionBlock);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _bytesValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _bytesValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as BYTES", resultDataType));
      }
    }
    return _bytesValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, byte[][]> transformToBytesValuesSVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToBytesValuesSV(projectionBlock));
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_intValuesMV == null) {
      _intValuesMV = new int[length][];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        int[] intValues = new int[numValues];
        dictionary.readIntValues(dictIds, numValues, intValues);
        _intValuesMV[i] = intValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _intValuesMV = transformToIntValuesMV(projectionBlock);
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(projectionBlock);
          ArrayCopyUtils.copy(longValuesMV, _intValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(projectionBlock);
          ArrayCopyUtils.copy(floatValuesMV, _intValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _intValuesMV, length);
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
          ArrayCopyUtils.copy(stringValuesMV, _intValuesMV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as INT", resultDataType));
      }
    }
    return _intValuesMV;
  }

  @Override
  public Pair<RoaringBitmap, int[][]> transformToIntValuesMVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToIntValuesMV(projectionBlock));
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_longValuesMV == null) {
      _longValuesMV = new long[length][];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        long[] longValues = new long[numValues];
        dictionary.readLongValues(dictIds, numValues, longValues);
        _longValuesMV[i] = longValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _longValuesMV = transformToLongValuesMV(projectionBlock);
          break;
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(projectionBlock);
          ArrayCopyUtils.copy(intValuesMV, _longValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(projectionBlock);
          ArrayCopyUtils.copy(floatValuesMV, _longValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _longValuesMV, length);
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
          ArrayCopyUtils.copy(stringValuesMV, _longValuesMV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as LONG", resultDataType));
      }
    }
    return _longValuesMV;
  }

  @Override
  public Pair<RoaringBitmap, long[][]> transformToLongValuesMVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToLongValuesMV(projectionBlock));
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_floatValuesMV == null) {
      _floatValuesMV = new float[length][];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        float[] floatValues = new float[numValues];
        dictionary.readFloatValues(dictIds, numValues, floatValues);
        _floatValuesMV[i] = floatValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _floatValuesMV = transformToFloatValuesMV(projectionBlock);
          break;
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(projectionBlock);
          ArrayCopyUtils.copy(intValuesMV, _floatValuesMV, length);
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(projectionBlock);
          ArrayCopyUtils.copy(longValuesMV, _floatValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _floatValuesMV, length);
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
          ArrayCopyUtils.copy(stringValuesMV, _floatValuesMV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as FLOAT", resultDataType));
      }
    }
    return _floatValuesMV;
  }

  @Override
  public Pair<RoaringBitmap, float[][]> transformToFloatValuesMVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToFloatValuesMV(projectionBlock));
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_doubleValuesMV == null) {
      _doubleValuesMV = new double[length][];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        double[] doubleValues = new double[numValues];
        dictionary.readDoubleValues(dictIds, numValues, doubleValues);
        _doubleValuesMV[i] = doubleValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case UNKNOWN:
          _doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          break;
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(projectionBlock);
          ArrayCopyUtils.copy(intValuesMV, _doubleValuesMV, length);
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(projectionBlock);
          ArrayCopyUtils.copy(longValuesMV, _doubleValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(projectionBlock);
          ArrayCopyUtils.copy(floatValuesMV, _doubleValuesMV, length);
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
          ArrayCopyUtils.copy(stringValuesMV, _doubleValuesMV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as DOUBLE", resultDataType));
      }
    }
    return _doubleValuesMV;
  }

  public Pair<RoaringBitmap, double[][]> transformToDoubleValuesMVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToDoubleValuesMV(projectionBlock));
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_stringValuesMV == null) {
      _stringValuesMV = new String[length][];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        String[] stringValues = new String[numValues];
        dictionary.readStringValues(dictIds, numValues, stringValues);
        _stringValuesMV[i] = stringValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType) {
        case UNKNOWN:
          _stringValuesMV = transformToStringValuesMV(projectionBlock);
          break;
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(projectionBlock);
          ArrayCopyUtils.copy(intValuesMV, _stringValuesMV, length);
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(projectionBlock);
          ArrayCopyUtils.copy(longValuesMV, _stringValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(projectionBlock);
          ArrayCopyUtils.copy(floatValuesMV, _stringValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(projectionBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _stringValuesMV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as STRING", resultDataType));
      }
    }
    return _stringValuesMV;
  }

  public Pair<RoaringBitmap, String[][]> transformToStringValuesMVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToStringValuesMV(projectionBlock));
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_bytesValuesMV == null) {
      _bytesValuesMV = new byte[length][][];
    }
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(projectionBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        byte[][] bytesValues = new byte[numValues][];
        dictionary.readBytesValues(dictIds, numValues, bytesValues);
        _bytesValuesMV[i] = bytesValues;
      }
    } else {
      Preconditions.checkState(getResultMetadata().getDataType().getStoredType() == DataType.STRING
          || getResultMetadata().getDataType().getStoredType() == DataType.UNKNOWN);
      String[][] stringValuesMV = transformToStringValuesMV(projectionBlock);
      ArrayCopyUtils.copy(stringValuesMV, _bytesValuesMV, length);
    }
    return _bytesValuesMV;
  }

  public Pair<RoaringBitmap, byte[][][]> transformToBytesValuesMVWithNull(ProjectionBlock projectionBlock) {
    return ImmutablePair.of(getNullBitmap(projectionBlock), transformToBytesValuesMV(projectionBlock));
  }

  @Override
  public RoaringBitmap getNullBitmap(ProjectionBlock projectionBlock) {
    RoaringBitmap bitmap = new RoaringBitmap();
    for (TransformFunction arg : _arguments) {
      RoaringBitmap argBitmap = arg.getNullBitmap(projectionBlock);
      if (argBitmap != null) {
        bitmap.or(argBitmap);
      }
    }
    return bitmap;
  }
}
