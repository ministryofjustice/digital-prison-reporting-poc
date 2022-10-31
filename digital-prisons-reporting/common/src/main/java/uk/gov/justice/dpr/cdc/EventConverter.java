package uk.gov.justice.dpr.cdc;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.get_json_object;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.schema_of_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

/**
 * This class converts events from GoldenGate and DMS into a common format
 * The chain is source-type (DMS|GoldenGate) -> ingestion-type (Kinesis|Kafka etc)
 * You would :
 * Dataset<Row> out = EventConverter.fromDMS_3_4_6(fromKinesis(in));
 * @author dominic.messenger
 *
 */
public class EventConverter {

	public static Dataset<Row> fromDMS_3_4_6(final Dataset<Row> in) {
		return in;
	}
	
	public static Dataset<Row> fromKinesis(final Dataset<Row> in) {
		Dataset<Row> out = in
				.withColumn("jsonData", col("data").cast("string"))
				.withColumn("metadata", get_json_object(col("jsonData"), "$.metadata"))
				.withColumn("payload", get_json_object(col("jsonData"), "$.data"))
				.withColumn("timestamp", get_json_object(col("metadata"), "$.timestamp"))
				.withColumn("recordType", get_json_object(col("metadata"), "$.record-type"))
				.withColumn("operation", get_json_object(col("metadata"), "$.operation"))
				.withColumn("partitionKeyType", get_json_object(col("metadata"), "$.partition-key-type"))
				.withColumn("schemaName", get_json_object(col("metadata"), "$.schema-name"))
				.withColumn("tableName", get_json_object(col("metadata"), "$.table-name"))
				.withColumn("transactionId", get_json_object(col("metadata"), "$.transaction-id"))

				.drop("data", "streamName");
		
		return out;
	}
	
	public static Dataset<Row> getPayload(Dataset<Row> df) {
		final DataType schema = getSchema(df, "payload");
		return df.withColumn("parsed", from_json(col("payload"), schema)).select(col("operation").as("_operation"), col("timestamp").as("_timestamp"), col("parsed.*"));
	}
	
	protected static DataType getSchema(Dataset<Row> df, final String column) {
		final Row[] schs = (Row[])df.sqlContext().range(1).select(
				schema_of_json(lit(df.select(column).first().getString(0)))
				).collect();
		final String schemaStr = schs[0].getString(0);
		
		return DataType.fromDDL(schemaStr);
	}
}
