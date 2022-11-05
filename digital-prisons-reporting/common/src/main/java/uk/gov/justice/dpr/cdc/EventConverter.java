package uk.gov.justice.dpr.cdc;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.get_json_object;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.schema_of_json;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.json.JsonMapper;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * This class converts events from GoldenGate and DMS into a common format
 * The chain is source-type (DMS|GoldenGate) -> ingestion-type (Kinesis|Kafka etc)
 * You would :
 * Dataset<Row> out = EventConverter.fromDMS_3_4_6(fromKinesis(in));
 * @author dominic.messenger
 *
 */
public class EventConverter {

	/**
	 * Event
	 * An event has this structure
	 * partitionKey : String
	 * sequenceNumber : number
	 * approximateArrivalTimestamp: timestamp
	 * data : json payload
	 * 
	 * +----------------+--------------------------------------------------------+---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	   |partitionKey    |sequenceNumber                                          |approximateArrivalTimestamp|data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
	   +----------------+--------------------------------------------------------+---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	   |SYSTEM.OFFENDERS|49634404264551915037098342805975859909984010411219877938|2022-10-31 12:34:29.535    |{ "metadata":{"timestamp":"2022-10-31T10:55:52.390858Z","record-type":"data","operation":"update","partition-key-type":"schema-table","schema-name":"SYSTEM","table-name":"OFFENDERS","transaction-id":655368} "data": {"MIDDLE_NAME":"gandalf","OFFENDER_ID":127,"OFFENDER_NAME_SEQ":254000,"ID_SOURCE_CODE":"DUMMY","LAST_NAME":"Beilenson","NAME_TYPE":"NAME","FIRST_NAME":"Anthony","BIRTH_DATE":"1932-10-26T00:00:00Z","SEX_CODE":"male","LAST_NAME_SOUNDEX":"Beilenson","CREATE_DATE":"2015-05-19T00:00:00Z","OFFENDER_ID_DISPLAY":"33b52056-8384-471a-9e8c-74b9cc7b5c0d","MODIFY_DATETIME":"2022-09-01T18:21:36.915241000Z","AGE":89,"CREATE_USER_ID":"9887","CREATE_DATETIME":"2022-09-01T18:21:36.915241000Z","AUDIT_TIMESTAMP":"2022-09-01T18:21:36.915241000Z"}}|
	   |SYSTEM.OFFENDERS|49634404264551915037098343256625928761909809032216969266|2022-10-31 12:57:43.025    |{ "metadata":{"timestamp":"2022-10-31T10:55:52.390858Z","record-type":"data","operation":"update","partition-key-type":"schema-table","schema-name":"SYSTEM","table-name":"OFFENDERS","transaction-id":655368} "data": {"MIDDLE_NAME":"gandalf","OFFENDER_ID":127,"OFFENDER_NAME_SEQ":254000,"ID_SOURCE_CODE":"DUMMY","LAST_NAME":"Beilenson","NAME_TYPE":"NAME","FIRST_NAME":"Anthony","BIRTH_DATE":"1932-10-26T00:00:00Z","SEX_CODE":"male","LAST_NAME_SOUNDEX":"Beilenson","CREATE_DATE":"2015-05-19T00:00:00Z","OFFENDER_ID_DISPLAY":"33b52056-8384-471a-9e8c-74b9cc7b5c0d","MODIFY_DATETIME":"2022-09-01T18:21:36.915241000Z","AGE":89,"CREATE_USER_ID":"9887","CREATE_DATETIME":"2022-09-01T18:21:36.915241000Z","AUDIT_TIMESTAMP":"2022-09-01T18:21:36.915241000Z"}}|
	   |SYSTEM.OFFENDERS|49634404264551915037098344097390314974378451785834561586|2022-10-31 13:40:58.737    |{ "metadata":{"timestamp":"2022-10-31T10:55:52.390858Z","record-type":"data","operation":"update","partition-key-type":"schema-table","schema-name":"SYSTEM","table-name":"OFFENDERS","transaction-id":655368} "data": {"MIDDLE_NAME":"gandalf","OFFENDER_ID":127,"OFFENDER_NAME_SEQ":254000,"ID_SOURCE_CODE":"DUMMY","LAST_NAME":"Beilenson","NAME_TYPE":"NAME","FIRST_NAME":"Anthony","BIRTH_DATE":"1932-10-26T00:00:00Z","SEX_CODE":"male","LAST_NAME_SOUNDEX":"Beilenson","CREATE_DATE":"2015-05-19T00:00:00Z","OFFENDER_ID_DISPLAY":"33b52056-8384-471a-9e8c-74b9cc7b5c0d","MODIFY_DATETIME":"2022-09-01T18:21:36.915241000Z","AGE":89,"CREATE_USER_ID":"9887","CREATE_DATETIME":"2022-09-01T18:21:36.915241000Z","AUDIT_TIMESTAMP":"2022-09-01T18:21:36.915241000Z"}}|
	   +----------------+--------------------------------------------------------+---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	 * 
	 */
	
	private static StructType EVENT_SCHEMA = new StructType()
			.add("partitionKey", DataTypes.StringType)
			.add("sequenceNumber", DataTypes.StringType)
			.add("approximateArrivalTimestamp", DataTypes.TimestampType)
			.add("data", DataTypes.StringType);

	
	
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
	
	public static Dataset<Row> fromRawDMS_3_4_6(final SparkSession spark, final InputStream stream) {
		final JsonMapper mapper = new JsonMapper();
		List<Row> rows = new ArrayList<Row>();
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMaximumFractionDigits(0);
		nf.setGroupingUsed(false);
		nf.setRoundingMode(RoundingMode.FLOOR);
		
		try(MappingIterator<JsonNode> it = mapper.readerFor(JsonNode.class).readValues(stream)) {
			while(it.hasNextValue()) {
				try {
				JsonNode node = it.nextValue();
				String partitionKey = node.path("metadata").path("schema-name").textValue() + "." 
									+ node.path("metadata").path("table-name").textValue();
				String sequenceNumber = nf.format(Math.floor(Math.random() *  100000000000000.0));
				Timestamp timestamp = new Timestamp(System.currentTimeMillis());
				String data = node.toString();
				List<Object> items = Arrays.asList(partitionKey, sequenceNumber, timestamp, data);
				Seq<Object> seq = JavaConverters.asScalaIteratorConverter(items.iterator()).asScala().toSeq();
				rows.add(Row.fromSeq(seq));
				} catch (Exception e) {
					handleError(e);
				}
			}
		} catch (IOException e) {
			handleError(e);
		}
		
		return spark.createDataFrame(rows, EVENT_SCHEMA);
	}
	
	public static Dataset<Row> toKinesis(final Dataset<Row> in) {
		// assume the payload is there;
		// assume _opertion and _timestamp are there
		
		// output to partitionKey & data
		// data is all fields exception _operation & timestamp to_json
		// plus metadata patched back in
		//{
		//	metadata: {...}
		//	data : {...}
		//}
		Dataset<Row> out = in
				.select(
						col("partitionKey"), 
						concat(lit("{ \"metadata\":"), col("metadata"),lit(", \"data\": "), col("payload"), lit("}")).as("data")
				);
		
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
	
	protected static void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
