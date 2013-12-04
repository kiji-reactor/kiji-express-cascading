package org.kiji.express.flow;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;

@ApiAudience.Public
@ApiStability.Experimental
public class ColumnOutputSpecBuilders {

  /** Builder for {@link QualifiedColumnOutputSpec}. */
  public static final class QualifiedColumnOutputSpecBuilder {

    /**
     * Initializes a new empty QualifiedColumnOutputSpecBuilder.
     *
     * @return a new empty QualifiedColumnOutputSpecBuilder.
     */
    public static QualifiedColumnOutputSpecBuilder create() {
      return new QualifiedColumnOutputSpecBuilder(null);
    }

    /**
     * Initializes a new QualifiedColumnOutputSpecBuilder as a copy of the given builder.
     *
     * @param toCopy builder to copy.
     * @return a new QualifiedColumnOutputSpecBuilder as a copy of the given builder.
     */
    public static QualifiedColumnOutputSpecBuilder copy(
        final QualifiedColumnOutputSpecBuilder toCopy
    ) {
      return new QualifiedColumnOutputSpecBuilder(toCopy);
    }

    private KijiColumnName mColumn = null;
    private SchemaSpec mSchemaSpec = null;

    /**
     * Private constructor. Use {@link #create()}
     *
     * @param toCopy builder to copy or null to make an empty builder.
     */
    private QualifiedColumnOutputSpecBuilder(
      final QualifiedColumnOutputSpecBuilder toCopy
    ) {
      if (null != toCopy) {
        mColumn = toCopy.mColumn;
        mSchemaSpec = toCopy.mSchemaSpec;
      }
    }

    /**
     * Configure the output spec to write to the given Kiji column.
     *
     * @param column name of the qualified column into which to write.
     * @return this.
     */
    public QualifiedColumnOutputSpecBuilder withQualifiedColumn(
        final KijiColumnName column
    ) {
      Preconditions.checkNotNull(column, "Output column may not be null.");
      Preconditions.checkArgument(null != column.getQualifier(),
          "Output column must be fully qualified, found: " + column);
      Preconditions.checkState(null == mColumn,
          "Output column already set to: " + mColumn);
      mColumn = column;
      return this;
    }

    /**
     * Configure the output spec to write to the given family and qualifier.
     *
     * @param family Kiji column family of the target qualified column.
     * @param qualifier Kiji column qualifier of the target qualified column.
     * @return this.
     */
    public QualifiedColumnOutputSpecBuilder withQualifiedColumn(
        final String family,
        final String qualifier
    ) {
      Preconditions.checkNotNull(family, "Output family may not be null.");
      Preconditions.checkNotNull(qualifier, "Output qualifier may not be null.");
      Preconditions.checkState(null == mColumn, "Output column already set to: " + mColumn);
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
     * Configure the output spec to infer the writer schema from the value as it is being written.
     * If the value is an Avro value its Schema can be retrieved directly. If the value is a
     * primitive type the Schema can be inferred from the type of the primitive. If the value is a
     * parametrized type the Schema cannot be inferred.
     *
     * @return this.
     */
    public QualifiedColumnOutputSpecBuilder withAvroWriterSchemaGeneric() {
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Writer$.MODULE$;
      return this;
    }

    /**
     * Configure the output spec to write using the default reader schema for this column stored in
     * the table layout.
     *
     * @return this.
     */
    public QualifiedColumnOutputSpecBuilder withAvroDefaultReaderSchema() {
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.DefaultReader$.MODULE$;
      return this;
    }

    /**
     * Configure the output spec to write using the given generic Avro Schema.
     *
     * @param schema Schema to use as the writer Schema.
     * @return this.
     */
    public QualifiedColumnOutputSpecBuilder withAvroSchemaGeneric(
        final Schema schema
    ) {
      Preconditions.checkNotNull(schema, "Schema may not be null.");
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Generic$.MODULE$.apply(schema);
      return this;
    }

    /**
     * Configure the output spec to write using the Schema of the given SpecificRecord subclass.
     *
     * @param specificRecordClass class of SpecificRecord from which to get the writer Schema.
     * @return this.
     */
    public QualifiedColumnOutputSpecBuilder withAvroSchemaSpecific(
        final Class<? extends SpecificRecord> specificRecordClass
    ) {
      Preconditions.checkNotNull(specificRecordClass, "Specific record class may not be null.");
      Preconditions.checkState(null == mSchemaSpec, "SchemaSpec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Specific$.MODULE$.apply(specificRecordClass);
      return this;
    }

    /**
     * Configure the output spec to write using the given SchemaSpec.
     *
     * @param schemaSpec Specification of the Schema with which to write.
     * @return this.
     */
    public QualifiedColumnOutputSpecBuilder withSchemaSpec(
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
     * Build a new QualifiedColumnOutputSpec from the values stored in this builder.
     *
     * @return a new QualifiedColumnOutputSpec from the values stored in this builder.
     */
    public QualifiedColumnOutputSpec build() {
      final KijiColumnName column = Preconditions.checkNotNull(mColumn,
          "Output column may not be null.");
      final SchemaSpec schemaSpec = (null != mSchemaSpec)
          ? mSchemaSpec : QualifiedColumnOutputSpec$.MODULE$.DEFAULT_SCHEMA_SPEC();
      return QualifiedColumnOutputSpec.apply(column.getName(), schemaSpec);
    }
  }

  /** Builder for {@link ColumnFamilyOutputSpec}. */
  public static final class ColumnFamilyOutputSpecBuilder {

    /**
     * Initializes a new empty ColumnFamilyOutputSpecBuilder.
     *
     * @return a new empty ColumnFamilyOutputSpecBuilder.
     */
    public static ColumnFamilyOutputSpecBuilder create() {
      return new ColumnFamilyOutputSpecBuilder(null);
    }

    /**
     * Initializes a new ColumnFamilyOutputSpecBuilder as a copy of the given builder.
     *
     * @param toCopy a ColumnFamilyOutputSpecBuilder to copy.
     * @return a new ColumnFamilyOutputSpecBuilder as a copy of the given builder.
     */
    public static ColumnFamilyOutputSpecBuilder copy(
        final ColumnFamilyOutputSpecBuilder toCopy
    ) {
      return new ColumnFamilyOutputSpecBuilder(toCopy);
    }

    private String mFamily = null;
    private String mQualifierSelector = null;
    private SchemaSpec mSchemaSpec = null;

    /**
     * Private constructor, use {@link #create()} or
     * {@link #copy(org.kiji.express.flow.ColumnOutputSpecBuilders.ColumnFamilyOutputSpecBuilder)}.
     *
     * @param toCopy builder to copy or null to make an empty builder.
     */
    private ColumnFamilyOutputSpecBuilder(
        final ColumnFamilyOutputSpecBuilder toCopy
    ) {
      if (null != toCopy) {
        mFamily = toCopy.mFamily;
        mQualifierSelector = toCopy.mQualifierSelector;
        mSchemaSpec = toCopy.mSchemaSpec;
      }
    }

    /**
     * Configure the output spec to write to the given column family.
     *
     * @param family column family into which to write.
     * @return this.
     */
    public ColumnFamilyOutputSpecBuilder withColumnFamily(
        final KijiColumnName family
    ) {
      Preconditions.checkNotNull(family, "Column family may not be null.");
      Preconditions.checkArgument(null == family.getQualifier(),
          "Column family may not be fully qualified, found: %s", family);
      Preconditions.checkState(null == mFamily, "Column family already set to: " + mFamily);
      mFamily = family.getFamily();
      return this;
    }

    /**
     * Configure the output spec to write to the given column family.
     *
     * @param family column family into which to write.
     * @return this.
     */
    public ColumnFamilyOutputSpecBuilder withColumnFamily(
        final String family
    ) {
      Preconditions.checkNotNull(family, "Column family may not be null.");
      Preconditions.checkState(null == mFamily, "Column family already set to: " + mFamily);
      mFamily = family;
      return this;
    }

    /**
     * Get the configured column family from this builder, or null if one has not been set.
     *
     * @return the configured column family from this builder, or null if one has not been set.
     */
    public String getColumnFamily() {
      return mFamily;
    }

    /**
     * Configure the output spec to write to the qualifier stored in the named field from each
     * tuple.
     *
     * @param qualifierSelector name of the field whose value will be used as the qualifier into
     *     which tuple values will be written.
     * @return this.
     */
    public ColumnFamilyOutputSpecBuilder withQualifierSelector(
        final String qualifierSelector
    ) {
      Preconditions.checkNotNull(qualifierSelector, "Qualifier selector may not be null.");
      Preconditions.checkState(null == mQualifierSelector,
          "Qualifier selector already set to: " + mQualifierSelector);
      mQualifierSelector = qualifierSelector;
      return this;
    }

    /**
     * Configure the output spec to write using the given Avro Schema.
     *
     * @param schema Avro Schema with which to write data.
     * @return this.
     */
    public ColumnFamilyOutputSpecBuilder withAvroSchemaGeneric(
        final Schema schema
    ) {
      Preconditions.checkNotNull(schema, "Schema may not be null.");
      Preconditions.checkState(null == mSchemaSpec, "Schema spec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Generic$.MODULE$.apply(schema);
      return this;
    }

    /**
     * Configure the output spec to write using the Schema of the given SpecificRecord subclass.
     *
     * @param specificRecordClass subclass of specific record from which to retrieve a Schema.
     * @return this.
     */
    public ColumnFamilyOutputSpecBuilder withAvroSchemaSpecific(
        final Class<? extends SpecificRecord> specificRecordClass
    ) {
      Preconditions.checkNotNull(specificRecordClass, "Specific record class may not be null.");
      Preconditions.checkState(null == mSchemaSpec, "Schema spec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Specific$.MODULE$.apply(specificRecordClass);
      return this;
    }

    /**
     * Configure the output spec to infer the writer schema from the value as it is being written.
     * If the value is an Avro value its Schema can be retrieved directly. If the value is a
     * primitive type the Schema can be inferred from the type of the primitive. If the value is a
     * parametrized type the Schema cannot be inferred.
     *
     * @return this.
     */
    public ColumnFamilyOutputSpecBuilder withAvroWriterSchemaGeneric() {
      Preconditions.checkState(null == mSchemaSpec, "Schema spec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.Writer$.MODULE$;
      return this;
    }

    /**
     * Configure the output spec to write using the default reader schema associated with the
     * configured column family.
     *
     * @return this.
     */
    public ColumnFamilyOutputSpecBuilder withAvroDefaultReaderSchema() {
      Preconditions.checkState(null == mSchemaSpec, "Schema spec already set to: " + mSchemaSpec);
      mSchemaSpec = SchemaSpec.DefaultReader$.MODULE$;
      return this;
    }

    /**
     * Configure the output spec to write using the given SchemaSpec.
     *
     * @param schemaSpec Specification of the Schema with which to write.
     * @return this.
     */
    public ColumnFamilyOutputSpecBuilder withSchemaSpec(
        final SchemaSpec schemaSpec
    ) {
      Preconditions.checkNotNull(schemaSpec, "Schema spec may not be null.");
      Preconditions.checkState(null == mSchemaSpec,
          "Schema spec is already set to: " + mSchemaSpec);
      mSchemaSpec = schemaSpec;
      return this;
    }

    /**
     * Get the configured SchemaSpec from this builder or null if one has not been set.
     *
     * @return the configured SchemaSpec from this builder or null if one has not been set.
     */
    public SchemaSpec getSchemaSpec() {
      return mSchemaSpec;
    }

    /**
     * Build a new ColumnFamilyOutputSpec from the values stored in this builder.
     *
     * @return a new ColumnFamilyOutputSpec from the values stored in this builder.
     */
    public ColumnFamilyOutputSpec build() {
      final String family =
          Preconditions.checkNotNull(mFamily, "Output column family may not be null.");
      final String qualifierSelector =
          Preconditions.checkNotNull(mQualifierSelector, "Qualifier selector may not be null.");
      final SchemaSpec schemaSpec = (null != mSchemaSpec)
          ? mSchemaSpec : ColumnFamilyOutputSpec$.MODULE$.DEFAULT_SCHEMA_SPEC();
      return ColumnFamilyOutputSpec$.MODULE$.construct(family, qualifierSelector, schemaSpec);
    }
  }
}
