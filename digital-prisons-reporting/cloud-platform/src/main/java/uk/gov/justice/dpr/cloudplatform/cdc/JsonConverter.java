package uk.gov.justice.dpr.cloudplatform.cdc;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.schema_of_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

public class JsonConverter {

	public static Dataset<Row> getPayload(Dataset<Row> df, final String column) {
		final DataType schema = getJsonSchema(df, column);
		return df.withColumn("parsed", from_json(col(column), schema)).select("parsed.*");
	}
	
	public static DataType getJsonSchema(Dataset<Row> df, final String column) {
		final Row[] schs = (Row[])df.sqlContext().range(1).select(
				schema_of_json(lit(df.select(column).first().getString(0)))
				).collect();
		final String schemaStr = schs[0].getString(0);
		
		return DataType.fromDDL(schemaStr);
	}
}
