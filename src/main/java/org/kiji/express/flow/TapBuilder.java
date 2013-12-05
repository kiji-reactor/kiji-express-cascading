package org.kiji.express.flow;

import java.util.Map;

import cascading.tap.Tap;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.express.flow.ColumnInputSpecBuilders.ColumnInputSpecBuilder;
import org.kiji.express.flow.ColumnOutputSpecBuilders.ColumnOutputSpecBuilder;
import org.kiji.schema.KijiURI;

/** Builder for Cascading {@link cascading.tap.Tap}s using Kiji inputs and outputs. */
@ApiAudience.Public
@ApiStability.Experimental
public final class TapBuilder {

  /**
   * Create a new empty TapBuilder.
   *
   * @return a new empty TapBuilder.
   */
  public static TapBuilder create() {
    return new TapBuilder(null);
  }

  /**
   * Create a copy of the given TapBuilder.
   *
   * @param toCopy other TapBuilder to copy.
   * @return a copy of the given TapBuilder.
   */
  public static TapBuilder copy(
      final TapBuilder toCopy
  ) {
    return new TapBuilder(toCopy);
  }

  private KijiURI mTableURI = null;
  private TimeRange mTimeRange = null;
  private String mTimestampField = null;
  private Map<String, ColumnInputSpec> mInputColumns = Maps.newHashMap();
  private Map<String, ColumnOutputSpec> mOutputColumns = Maps.newHashMap();

  /**
   * Private constructor, use {@link #create()} or {@link #copy(TapBuilder)}.
   *
   * @param toCopy another TapBuilder to copy, or null to create a new empty builder.
   */
  private TapBuilder(
      final TapBuilder toCopy
  ) {
    if (null != toCopy) {
      mTableURI = toCopy.mTableURI;
      mTimeRange = toCopy.mTimeRange;
      mTimestampField = toCopy.mTimestampField;
      mInputColumns = toCopy.mInputColumns;
      mOutputColumns = toCopy.mOutputColumns;
    }
  }

  /**
   * Configure the tap to read from and write to the Kiji table with the given KijiURI.
   *
   * @param tableURI KijiURI of the table from which to read and write.
   * @return this.
   */
  public TapBuilder withTableURI(
      final KijiURI tableURI
  ) {
    Preconditions.checkNotNull(tableURI, "Table URI may not be null.");
    Preconditions.checkArgument(null != tableURI.getTable(),
        "Table URI must include a table name, found: " + tableURI);
    Preconditions.checkState(null == mTableURI, "Table URI already set to: " + mTableURI);
    mTableURI = tableURI;
    return this;
  }

  /**
   * Get the configured TableURI from this builder or null if none has been set.
   *
   * @return the configured TableURI from this builder or null if none has been set.
   */
  public KijiURI getTableURI() {
    return mTableURI;
  }

  /**
   * Configure the tap to read values from timestamps in the given TimeRange.
   *
   * @param timeRange TimeRange from which to read values.
   * @return this.
   */
  public TapBuilder withTimeRange(
      final TimeRange timeRange
  ) {
    Preconditions.checkNotNull(timeRange, "Time range may not be null.");
    Preconditions.checkState(null == mTimeRange, "Time range already set to: " + mTimeRange);
    mTimeRange = timeRange;
    return this;
  }

  /**
   * Configure the tap to read values from all timestamps before the given end time.
   *
   * @param endTime time in milliseconds since the unix epoch before which values may be read.
   *     (inclusive)
   * @return this.
   */
  public TapBuilder withTimeBefore(
      final long endTime
  ) {
    Preconditions.checkState(null == mTimeRange, "Time range already set to: " + mTimeRange);
    mTimeRange = Before$.MODULE$.apply(endTime);
    return this;
  }

  /**
   * Configure the tap to read values from all timestamps after the given start time.
   *
   * @param startTime time in milliseconds since the unix epoch after which values may be read.
   *     (exclusive)
   * @return this.
   */
  public TapBuilder withTimeAfter(
      final long startTime
  ) {
    Preconditions.checkState(null == mTimeRange, "Time range already set to: " + mTimeRange);
    mTimeRange = After$.MODULE$.apply(startTime);
    return this;
  }

  /**
   * Configure the tap to read values from all timestamps between the given start and end times.
   * The start time is included and the end time is excluded.
   *
   * @param startTime time in milliseconds since the unix epoch after which values may be read.
   *     (inclusive)
   * @param endTime time in milliseconds since the unix epoch before which values may be read.
   *     (exclusive)
   * @return
   */
  public TapBuilder withTimeBetween(
      final long startTime,
      final long endTime
  ) {
    Preconditions.checkState(null == mTimeRange, "Time range already set to: " + mTimeRange);
    mTimeRange = Between$.MODULE$.apply(startTime, endTime);
    return this;
  }

  /**
   * Configure the tap to read values from all timestamps.
   *
   * @return this.
   */
  public TapBuilder withAllTime() {
    Preconditions.checkState(null == mTimeRange, "Time range already set to: " + mTimeRange);
    mTimeRange = All$.MODULE$;
    return this;
  }

  /**
   * Configure the tap to read values from the given timestamp only.
   *
   * @param exactTime time in milliseconds since the unix epoch from which to read values.
   * @return this.
   */
  public TapBuilder withExactTime(
      final long exactTime
  ) {
    Preconditions.checkState(null == mTimeRange, "Time range already set to: " + mTimeRange);
    mTimeRange = At$.MODULE$.apply(exactTime);
    return this;
  }

  /**
   * Get the time range from which input values may be read.
   *
   * @return the time range from which input values may be read.
   */
  public TimeRange getTimeRange() {
    return mTimeRange;
  }

  /**
   * Configure the tap to write values to the timestamp found in the given field.
   *
   * @param timestampField name of the field from which to read the timestamp.
   * @return this.
   */
  public TapBuilder withTimestampField(
      final String timestampField
  ) {
    Preconditions.checkNotNull(timestampField, "Timestamp field may not be null.");
    Preconditions.checkState(null == mTimestampField,
        "Timestamp field already set to: " + mTimestampField);
    mTimestampField = timestampField;
    return this;
  }

  /**
   * Get the timestamp field configured in this builder or null if none has been set.
   *
   * @return the timestamp field configured in this builder or null if none has been set.
   */
  public String getTimestampField() {
    return mTimestampField;
  }

  /**
   * Configure the tap to include the given input column specifications.
   *
   * @param inputColumns mapping from field name to input spec.
   * @return this.
   */
  public TapBuilder withInputColumns(
      final Map<String, ColumnInputSpec> inputColumns
  ) {
    Preconditions.checkNotNull(inputColumns, "Input columns may not be null.");
    Preconditions.checkState(null == mInputColumns,
        "Input columns already set to: " + mInputColumns);
    mInputColumns = inputColumns;
    return this;
  }

  /**
   * Add new input column specifications to this builder. Field names must not have been set
   * previously.
   *
   * @param inputColumns mapping from field name to input spec.
   * @return this.
   */
  public TapBuilder addInputColumns(
      final Map<String, ColumnInputSpec> inputColumns
  ) {
    Preconditions.checkNotNull(inputColumns, "Input columns may not be null.");
    if (null == mInputColumns) {
      mInputColumns = inputColumns;
    } else {
      for (Map.Entry<String, ColumnInputSpec> inputEntry : inputColumns.entrySet()) {
        final String field = inputEntry.getKey();
        final ColumnInputSpec spec = inputEntry.getValue();
        final ColumnInputSpec overwritten = mInputColumns.put(field, spec);
        Preconditions.checkState(overwritten == null,
            "Field: %s already mapped to input column: %s", field, overwritten);
      }
    }
    return this;
  }

  /**
   * Configure the tap to include the given input column specifications.
   *
   * @param inputColumnBuilders mapping from field name to input spec builder.
   * @return this.
   */
  public TapBuilder withInputColumnBuilders(
      final Map<String, ColumnInputSpecBuilder> inputColumnBuilders
  ) {
    Preconditions.checkNotNull(inputColumnBuilders, "Input column builders may not be null.");
    Preconditions.checkState(null == mInputColumns,
        "Input columns already set to: " + mInputColumns);
    final Map<String, ColumnInputSpec> inputColumns = Maps.newHashMap();
    for (Map.Entry<String, ColumnInputSpecBuilder> builderEntry : inputColumnBuilders.entrySet()) {
      inputColumns.put(builderEntry.getKey(), builderEntry.getValue().build());
    }
    mInputColumns = inputColumns;
    return this;
  }

  /**
   * Add new input column specifications to this builder. Field names must not have been set
   * previously.
   *
   * @param inputColumnBuilders mapping from field name to input spec builder.
   * @return this.
   */
  public TapBuilder addInputColumnBuilders(
      final Map<String, ColumnInputSpecBuilder> inputColumnBuilders
  ) {
    Preconditions.checkNotNull(inputColumnBuilders, "Input column builders may not be null.");
    if (null == mInputColumns) {
      final Map<String, ColumnInputSpec> inputColumns = Maps.newHashMap();
      for (Map.Entry<String, ColumnInputSpecBuilder> builderEntry
          : inputColumnBuilders.entrySet()) {
        inputColumns.put(builderEntry.getKey(), builderEntry.getValue().build());
      }
      mInputColumns = inputColumns;
    } else {
      for (Map.Entry<String, ColumnInputSpecBuilder> inputEntry : inputColumnBuilders.entrySet()) {
        final String field = inputEntry.getKey();
        final ColumnInputSpec spec = inputEntry.getValue().build();
        final ColumnInputSpec overwritten = mInputColumns.put(field, spec);
        Preconditions.checkState(overwritten == null,
            "Field: %s already mapped to input column: %s", field, overwritten);
      }
    }
    return this;
  }

  /**
   * Get the input column specifications or null if none have been set.
   *
   * @return the input column specifications or null if none have been set.
   */
  public Map<String, ColumnInputSpec> getInputColumns() {
    return mInputColumns;
  }

  /**
   * Configure the tap to include the given output column specifications.
   *
   * @param outputColumns mapping from field name to output spec.
   * @return this.
   */
  public TapBuilder withOutputColumns(
      final Map<String, ColumnOutputSpec> outputColumns
  ) {
    Preconditions.checkNotNull(outputColumns, "Output columns may not be null.");
    Preconditions.checkState(null == mOutputColumns,
        "Output columns already set to: " + mOutputColumns);
    mOutputColumns = outputColumns;
    return this;
  }

  /**
   * Add new output column specifications to this builder. Field names must not have been set
   * previously.
   *
   * @param outputColumns mapping from field name to output spec.
   * @return this.
   */
  public TapBuilder addOutputColumns(
      final Map<String, ColumnOutputSpec> outputColumns
  ) {
    Preconditions.checkNotNull(outputColumns, "Input columns may not be null.");
    if (null == mOutputColumns) {
      mOutputColumns = outputColumns;
    } else {
      for (Map.Entry<String, ColumnOutputSpec> outputEntry : outputColumns.entrySet()) {
        final String field = outputEntry.getKey();
        final ColumnOutputSpec spec = outputEntry.getValue();
        final ColumnOutputSpec overwritten = mOutputColumns.put(field, spec);
        Preconditions.checkState(overwritten == null,
            "Field: %s already mapped to output column: %s", field, overwritten);
      }
    }
    return this;
  }

  /**
   * Configure the tap to include the given output column specifications.
   *
   * @param outputColumnBuilders mapping from field name to output spec builder.
   * @return this.
   */
  public TapBuilder withOutputColumnBuilders(
      final Map<String, ColumnOutputSpecBuilder> outputColumnBuilders
  ) {
    Preconditions.checkNotNull(outputColumnBuilders, "Output column builders may not be null.");
    Preconditions.checkState(null == mOutputColumns,
        "Output columns already set to: " + mOutputColumns);
    final Map<String, ColumnOutputSpec> outputColumns = Maps.newHashMap();
    for (Map.Entry<String, ColumnOutputSpecBuilder> builderEntry
        : outputColumnBuilders.entrySet()) {
      outputColumns.put(builderEntry.getKey(), builderEntry.getValue().build());
    }
    mOutputColumns = outputColumns;
    return this;
  }

  /**
   * Add new output column specifications to this builder. Field names must not have been set
   * previously.
   *
   * @param outputColumnBuilders mapping from field name to output spec builder.
   * @return this.
   */
  public TapBuilder addOutputColumnBuilders(
      final Map<String, ColumnOutputSpecBuilder> outputColumnBuilders
  ) {
    Preconditions.checkNotNull(outputColumnBuilders, "Output column builders may not be null.");
    if (null == mOutputColumns) {
      final Map<String, ColumnOutputSpec> outputColumns = Maps.newHashMap();
      for (Map.Entry<String, ColumnOutputSpecBuilder> builderEntry
          : outputColumnBuilders.entrySet()) {
        outputColumns.put(builderEntry.getKey(), builderEntry.getValue().build());
      }
      mOutputColumns = outputColumns;
    } else {
      for (Map.Entry<String, ColumnOutputSpecBuilder> outputEntry
          : outputColumnBuilders.entrySet()) {
        final String field = outputEntry.getKey();
        final ColumnOutputSpec spec = outputEntry.getValue().build();
        final ColumnOutputSpec overwritten = mOutputColumns.put(field, spec);
        Preconditions.checkState(overwritten == null,
            "Field: %s already mapped to output column: %s", field, overwritten);
      }
    }
    return this;
  }

  /**
   * Get the output column specifications from this builder, or null if none have been set.
   *
   * @return the output column specifications from this builder, or null if none have been set.
   */
  public Map<String, ColumnOutputSpec> getOutputColumns() {
    return mOutputColumns;
  }

  /**
   * Build a Cascading {@link cascading.tap.Tap} from the values stored in this builder.
   *
   * @return a Cascading {@link cascading.tap.Tap} from the values stored in this builder.
   */
  public Tap<?, ?, ?> build() {
    return KijiSource$.MODULE$.makeTap(
        mTableURI.toString(), mTimeRange, mTimestampField, mInputColumns, mOutputColumns);
  }
}
