package org.kiji.express.flow;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.filter.KijiColumnFilter;

@ApiAudience.Public
@ApiStability.Experimental
public class ColumnInputSpecBuilders {

  /** Dummy super-class for column input spec builders. */
  public static abstract class ColumnInputSpecBuilder {
    /**
     * Build a new ColumnInputSpec from the values stored in this builder.
     *
     * @return a new ColumnInputSpec from the values stored in this builder.
     */
    public abstract ColumnInputSpec build();
  }

  /** Builder for {@link QualifiedColumnInputSpec}. */
  public static final class QualifiedColumnInputSpecBuilder extends ColumnInputSpecBuilder {
    /**
     * Initializes a new empty QualifiedColumnInputSpecBuilder.
     *
     * @return a new empty QualifiedColumnInputSpecBuilder.
     */
    public static QualifiedColumnInputSpecBuilder create() {
      return new QualifiedColumnInputSpecBuilder(null);
    }

    /**
     * Initializes a new QualifiedColumnInputSpecBuilder as a copy of the given builder.
     *
     * @param toCopy builder to copy.
     * @return a new QualifiedColumnInputSpecBuilder as a copy of the given builder.
     */
    public static QualifiedColumnInputSpecBuilder copy(
        final QualifiedColumnInputSpecBuilder toCopy
    ) {
      return new QualifiedColumnInputSpecBuilder(toCopy);
    }

    private KijiColumnName mColumn = null;
    private SchemaSpec mSchemaSpec = null;
    private Integer mMaxVersions = null;
    private ColumnFilterSpec mColumnFilterSpec = null;
    private PagingSpec mPagingSpec = null;

    /**
     * Private constructor. Use {@link #create()}
     *
     * @param toCopy builder to copy or null to make an empty builder.
     */
    private QualifiedColumnInputSpecBuilder(
      final QualifiedColumnInputSpecBuilder toCopy
    ) {
      if (null != toCopy) {
        mColumn = toCopy.mColumn;
        mSchemaSpec = toCopy.mSchemaSpec;
      }
    }

    /**
     * Configure the input spec to read from the given Kiji column.
     *
     * @param column name of the qualified column from which to read.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withQualifiedColumn(
        final KijiColumnName column
    ) {
      Preconditions.checkNotNull(column, "Input column may not be null.");
      Preconditions.checkArgument(null != column.getQualifier(),
          "Input column must be fully qualified, found: " + column);
      Preconditions.checkState(null == mColumn,
          "Input column already set to: " + mColumn);
      mColumn = column;
      return this;
    }

    /**
     * Configure the input spec to read from the given family and qualifier.
     *
     * @param family Kiji column family of the target qualified column.
     * @param qualifier Kiji column qualifier of the target qualified column.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withQualifiedColumn(
        final String family,
        final String qualifier
    ) {
      Preconditions.checkNotNull(family, "Input family may not be null.");
      Preconditions.checkNotNull(qualifier, "Input qualifier may not be null.");
      Preconditions.checkState(null == mColumn, "Input column already set to: " + mColumn);
      mColumn = new KijiColumnName(family, qualifier);
      return this;
    }

    /**
     * Get the specified column or null if none has been set.
     *
     * @return the specified column or null if none has been set.
     */
    public KijiColumnName getQualifiedColumn() {
      return mColumn;
    }

    /**
     * Configure the input spec to infer the reader schema from the value.
     * If the value is an Avro value its Schema can be retrieved directly. If the value is a
     * primitive type the Schema can be inferred from the type of the primitive. If the value is a
     * parametrized type the Schema cannot be inferred.
     *
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withAvroWriterSchemaGeneric() {
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Writer$.MODULE$;
      return this;
    }


    /**
     * Configure the input spec to read using the default reader schema for this column stored in
     * the table layout.
     *
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withAvroDefaultReaderSchema() {
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.DefaultReader$.MODULE$;
      return this;
    }

    /**
     * Configure the input spec to read using the given generic Avro Schema.
     *
     * @param schema Schema to use as the reader Schema.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withAvroSchemaGeneric(
        final Schema schema
    ) {
      Preconditions.checkNotNull(schema, "Schema may not be null.");
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Generic$.MODULE$.apply(schema);
      return this;
    }

    /**
     * Configure the input spec to read using the Schema of the given SpecificRecord subclass.
     *
     * @param specificRecordClass class of SpecificRecord from which to get the writer Schema.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withAvroSchemaSpecific(
        final Class<? extends SpecificRecord> specificRecordClass
    ) {
      Preconditions.checkNotNull(specificRecordClass, "Specific record class may not be null.");
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Specific$.MODULE$.apply(specificRecordClass);
      return this;
    }

    /**
     * Configure the input spec to read using the given SchemaSpec.
     *
     * @param schemaSpec Specification of the Schema with which to read.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withSchemaSpec(
        final SchemaSpec schemaSpec
    ) {
      Preconditions.checkNotNull(schemaSpec, "Schema spec may not be null.");
      Preconditions.checkState(null == mSchemaSpec,
          "Schema spec is already set to: " + mSchemaSpec);
      mSchemaSpec = schemaSpec;
      return this;
    }

    /**
     * Get the specified value of the SchemaSpec from this builder, or null if one has not been set.
     *
     * @return the specified value of the SchemaSpec from this builder, or null if one has not been
     *     set.
     */
    public SchemaSpec getSchemaSpec() {
      return mSchemaSpec;
    }

    /**
     * Configure the input spec to return the maximum number of versions requested.
     *
     * @param maxVersions The maximum number of versions per column qualifier to return.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withMaxVersions(
        final int maxVersions
    ) {
      Preconditions.checkArgument(maxVersions > 0,
          "Maximum number of versions must be strictly positive, but got: %d",
          maxVersions);
      Preconditions.checkState(mMaxVersions == null,
          "Cannot set max versions to %d, max versions already set to %d.",
          maxVersions, mMaxVersions);
      mMaxVersions = maxVersions;
      return this;
    }

    /**
     * Get the max versions or null if it has not been set.
     *
     * @return the max versions.
     */
    public Integer getMaxVersions() {
      return mMaxVersions;
    }

    /**
     * Configure the input spec to return data with the requested filter specification.
     * Null ColumnFilterSpec means that no filter will be applied.
     *
     * @param columnFilterSpec is the Scala specification of the column filters.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withColumnFilterSpec(
        final ColumnFilterSpec columnFilterSpec
    ) {
      Preconditions.checkState(mColumnFilterSpec == null,
          "Cannot set ColumnFilterSpec to %d, ColumnFilterSpec already set to %d.",
          columnFilterSpec, mColumnFilterSpec);
      mColumnFilterSpec = columnFilterSpec;
      return this;
    }

    /**
     * Configure the input spec to return data with the requested filter specification.
     * Null ColumnFilterSpec means that no filter will be applied.
     *
     * @param kijiColumnFilter is the filter to values returned.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withColumnFilterSpec(
        final KijiColumnFilter kijiColumnFilter
    ) {
      Preconditions.checkState(mColumnFilterSpec == null,
          "Cannot set ColumnFilterSpec to KijiColumnFilter %d, ColumnFilterSpec already set to %d.",
          kijiColumnFilter, mColumnFilterSpec);
      mColumnFilterSpec = ColumnFilterSpec.
          KijiColumnFilterColumnFilterSpec$.MODULE$.apply(kijiColumnFilter);
      return this;
    }

    /**
     * Get the column filter spec or null if it has not been set.
     *
     * @return the column filter spec.
     */
    public ColumnFilterSpec getColumnFilterSpec() {
      return mColumnFilterSpec;
    }

    /**
     * Configure the input spec to return data with the specified page size.
     *
     * @param pagingSpec the Scala paging specification.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withPagingSpec(
        final PagingSpec pagingSpec
    ) {
      Preconditions.checkState(mPagingSpec == null,
          "Cannot set PagingSpec to %d, PagingSpec already set to %d.",
          pagingSpec, mPagingSpec);
      mPagingSpec = pagingSpec;
      return this;
    }

    /**
     * Configure the input spec to turn off paging.
     *
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withPagingOff() {
      Preconditions.checkState(mPagingSpec == null,
          "Cannot set PagingSpec since it is already set to %d.",
          mPagingSpec);
      mPagingSpec = PagingSpec.Off$.MODULE$;
      return this;
    }

    /**
     * Configure the input spec to return data with the specified page size.
     *
     * @param count of the cells per page.
     * @return this.
     */
    public QualifiedColumnInputSpecBuilder withPagingCellCount(final int count) {
      Preconditions.checkState(mPagingSpec == null,
          "Cannot set PagingSpec since it is already set to %d.",
          mPagingSpec);
      mPagingSpec = PagingSpec.Cells$.MODULE$.apply(count);
      return this;
    }

    /**
     * Get the paging spec or null if it has not been set.
     *
     * @return the paging spec.
     */
    public PagingSpec getPagingSpec() {
      return mPagingSpec;
    }

    /**
     * Build a new QualifiedColumnInputSpec from the values stored in this builder.
     *
     * @return a new QualifiedColumnInputSpec from the values stored in this builder.
     */
    public QualifiedColumnInputSpec build() {
      final KijiColumnName column = Preconditions.checkNotNull(mColumn,
          "Input column may not be null.");
      // TODO(shashir): This construct method needs to be implemented in ColumnInputSpec.
      return QualifiedColumnInputSpec.construct(
          mColumn,
          mSchemaSpec,
          mMaxVersions,
          mColumnFilterSpec,
          mPagingSpec);
    }
  }

  
  /** Builder for {@link ColumnFamilyInputSpec}. */
  public static final class ColumnFamilyInputSpecBuilder extends ColumnInputSpecBuilder {
    /**
     * Initializes a new empty ColumnFamilyInputSpecBuilder.
     *
     * @return a new empty ColumnFamilyInputSpecBuilder.
     */
    public static ColumnFamilyInputSpecBuilder create() {
      return new ColumnFamilyInputSpecBuilder(null);
    }

    /**
     * Initializes a new ColumnFamilyInputSpecBuilder as a copy of the given builder.
     *
     * @param toCopy builder to copy.
     * @return a new ColumnFamilyInputSpecBuilder as a copy of the given builder.
     */
    public static ColumnFamilyInputSpecBuilder copy(
        final ColumnFamilyInputSpecBuilder toCopy
    ) {
      return new ColumnFamilyInputSpecBuilder(toCopy);
    }

    private KijiColumnName mColumn = null;
    private SchemaSpec mSchemaSpec = null;
    private Integer mMaxVersions = null;
    private ColumnFilterSpec mColumnFilterSpec = null;
    private PagingSpec mPagingSpec = null;

    /**
     * Private constructor. Use {@link #create()}
     *
     * @param toCopy builder to copy or null to make an empty builder.
     */
    private ColumnFamilyInputSpecBuilder(
      final ColumnFamilyInputSpecBuilder toCopy
    ) {
      if (null != toCopy) {
        mColumn = toCopy.mColumn;
        mSchemaSpec = toCopy.mSchemaSpec;
      }
    }

    /**
     * Configure the input spec to read from the given Kiji column family.
     *
     * @param column name of the family from which to read.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withColumnFamily(
        final KijiColumnName column
    ) {
      Preconditions.checkNotNull(column, "Input column may not be null.");
      Preconditions.checkArgument(!column.isFullyQualified(),
          "Input column can not be fully qualified, found: " + column);
      Preconditions.checkState(null == mColumn,
          "Input column already set to: " + mColumn);
      mColumn = column;
      return this;
    }

    /**
     * Configure the input spec to read from the given family.
     *
     * @param family Kiji column family of the target column.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withColumnFamily(
        final String family
    ) {
      Preconditions.checkNotNull(family, "Input family may not be null.");
      Preconditions.checkArgument(!family.contains(":"), "Family name may not contain colon.");
      Preconditions.checkState(null == mColumn, "Input column already set to: " + mColumn);
      mColumn = new KijiColumnName(family);
      return this;
    }

    /**
     * Get the specified column family or null if none has been set.
     *
     * @return the specified column family or null if none has been set.
     */
    public KijiColumnName getColumnFamily() {
      return mColumn;
    }

    /**
     * Configure the input spec to infer the reader schema from the value.
     * If the value is an Avro value its Schema can be retrieved directly. If the value is a
     * primitive type the Schema can be inferred from the type of the primitive. If the value is a
     * parametrized type the Schema cannot be inferred.
     *
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withAvroWriterSchemaGeneric() {
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Writer$.MODULE$;
      return this;
    }


    /**
     * Configure the input spec to read using the default reader schema for this column stored in
     * the table layout.
     *
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withAvroDefaultReaderSchema() {
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.DefaultReader$.MODULE$;
      return this;
    }

    /**
     * Configure the input spec to read using the given generic Avro Schema.
     *
     * @param schema Schema to use as the reader Schema.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withAvroSchemaGeneric(
        final Schema schema
    ) {
      Preconditions.checkNotNull(schema, "Schema may not be null.");
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Generic$.MODULE$.apply(schema);
      return this;
    }

    /**
     * Configure the input spec to read using the Schema of the given SpecificRecord subclass.
     *
     * @param specificRecordClass class of SpecificRecord from which to get the reader Schema.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withAvroSchemaSpecific(
        final Class<? extends SpecificRecord> specificRecordClass
    ) {
      Preconditions.checkNotNull(specificRecordClass, "Specific record class may not be null.");
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Specific$.MODULE$.apply(specificRecordClass);
      return this;
    }

    /**
     * Configure the input spec to read using the given SchemaSpec.
     *
     * @param schemaSpec Specification of the Schema with which to read.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withSchemaSpec(
        final SchemaSpec schemaSpec
    ) {
      Preconditions.checkNotNull(schemaSpec, "Schema spec may not be null.");
      Preconditions.checkState(null == mSchemaSpec,
          "Schema spec is already set to: " + mSchemaSpec);
      mSchemaSpec = schemaSpec;
      return this;
    }

    /**
     * Get the specified value of the SchemaSpec from this builder, or null if one has not been set.
     *
     * @return the specified value of the SchemaSpec from this builder, or null if one has not been
     *     set.
     */
    public SchemaSpec getSchemaSpec() {
      return mSchemaSpec;
    }

    /**
     * Configure the input spec to return the maximum number of versions requested.
     *
     * @param maxVersions The maximum number of versions per column to return.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withMaxVersions(
        final int maxVersions
    ) {
      Preconditions.checkArgument(maxVersions > 0,
          "Maximum number of versions must be strictly positive, but got: %d",
          maxVersions);
      Preconditions.checkState(mMaxVersions == null,
          "Cannot set max versions to %d, max versions already set to %d.",
          maxVersions, mMaxVersions);
      mMaxVersions = maxVersions;
      return this;
    }

    /**
     * Get the max versions or null if it has not been set.
     *
     * @return the max versions.
     */
    public Integer getMaxVersions() {
      return mMaxVersions;
    }

    /**
     * Configure the input spec to return data with the requested filter specification.
     * Null ColumnFilterSpec means that no filter will be applied.
     *
     * @param columnFilterSpec is the Scala specification of the column filters.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withColumnFilterSpec(
        final ColumnFilterSpec columnFilterSpec
    ) {
      Preconditions.checkState(mColumnFilterSpec == null,
          "Cannot set ColumnFilterSpec to %d, ColumnFilterSpec already set to %d.",
          columnFilterSpec, mColumnFilterSpec);
      mColumnFilterSpec = columnFilterSpec;
      return this;
    }

    /**
     * Configure the input spec to return data with the requested filter specification.
     * Null ColumnFilterSpec means that no filter will be applied.
     *
     * @param kijiColumnFilter is the filter to values returned.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withColumnFilterSpec(
        final KijiColumnFilter kijiColumnFilter
    ) {
      Preconditions.checkState(mColumnFilterSpec == null,
          "Cannot set ColumnFilterSpec to KijiColumnFilter %d, ColumnFilterSpec already set to %d.",
          kijiColumnFilter, mColumnFilterSpec);
      mColumnFilterSpec = ColumnFilterSpec.
          KijiColumnFilterColumnFilterSpec$.MODULE$.apply(kijiColumnFilter);
      return this;
    }

    /**
     * Get the column filter spec or null if it has not been set.
     *
     * @return the column filter spec.
     */
    public ColumnFilterSpec getColumnFilterSpec() {
      return mColumnFilterSpec;
    }

    /**
     * Configure the input spec to return data with the specified page size.
     *
     * @param pagingSpec the Scala paging specification.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withPagingSpec(
        final PagingSpec pagingSpec
    ) {
      Preconditions.checkState(mPagingSpec == null,
          "Cannot set PagingSpec to %d, PagingSpec already set to %d.",
          pagingSpec, mPagingSpec);
      mPagingSpec = pagingSpec;
      return this;
    }

    /**
     * Configure the input spec to turn off paging.
     *
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withPagingOff() {
      Preconditions.checkState(mPagingSpec == null,
          "Cannot set PagingSpec since it is already set to %d.",
          mPagingSpec);
      mPagingSpec = PagingSpec.Off$.MODULE$;
      return this;
    }

    /**
     * Configure the input spec to return data with the specified page size.
     *
     * @param count of the cells per page.
     * @return this.
     */
    public ColumnFamilyInputSpecBuilder withPagingCellCount(final int count) {
      Preconditions.checkState(mPagingSpec == null,
          "Cannot set PagingSpec since it is already set to %d.",
          mPagingSpec);
      mPagingSpec = PagingSpec.Cells$.MODULE$.apply(count);
      return this;
    }

    /**
     * Get the paging spec or null if it has not been set.
     *
     * @return the paging spec.
     */
    public PagingSpec getPagingSpec() {
      return mPagingSpec;
    }

    /**
     * Build a new ColumnFamilyInputSpec from the values stored in this builder.
     *
     * @return a new ColumnFamilyInputSpec from the values stored in this builder.
     */
    public ColumnFamilyInputSpec build() {
      final KijiColumnName column = Preconditions.checkNotNull(mColumn,
          "Input column may not be null.");
      // TODO(shashir): This construct method needs to be implemented in ColumnInputSpec.
      return ColumnFamilyInputSpec.construct(
          mColumn,
          mSchemaSpec,
          mMaxVersions,
          mColumnFilterSpec,
          mPagingSpec);;
    }
  }
}
