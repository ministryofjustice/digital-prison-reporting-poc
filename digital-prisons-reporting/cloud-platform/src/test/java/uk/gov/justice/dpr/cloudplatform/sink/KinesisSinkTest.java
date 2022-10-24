package uk.gov.justice.dpr.cloudplatform.sink;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.stubbing.defaultanswers.ReturnsEmptyValues;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import uk.gov.justice.dpr.BaseSparkTest;

@RunWith(MockitoJUnitRunner.class)
@PrepareForTest(AmazonKinesisClientBuilder.class)
public class KinesisSinkTest extends BaseSparkTest{

	@Mock AmazonKinesis stream;
	@Mock AmazonKinesisClientBuilder akcb;
	
	@Before
	public void before() {
		super.before();
		//akcb = mock(AmazonKinesisClientBuilder.class, new AnswerWithSelf(AmazonKinesisClientBuilder.class));
		//stream = mock(AmazonKinesis.class);
		//when(akcb.build()).thenReturn(stream);
	}
	
	@Test
	@Ignore
	public void shouldOpenAKinesisStream() {
		final KinesisSink sink = create();
		
		sink.open(1, 2);
		assertNotNull(sink.client);
	}
	
	@Test
	@Ignore
	public void shouldCloseAKinesisStream() {
		final KinesisSink sink = create();
		
		sink.open(1, 2);
		assertNotNull(sink.client);
		sink.close(null);
		assertNull(sink.client);
	}
	
	@Test
	@Ignore
	public void shouldWriteToAKinesisStream() throws IOException {
		final KinesisSink sink = create();
		
		sink.open(1, 2);
		assertNotNull(sink.client);
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/event-converter.parquet", "event-converter.parquet");
		
		sink.write(df);
		
		// verify that sink.client
		verify(sink).client.putRecord(any(PutRecordRequest.class));
	}
	
	@Test
	public void shouldFlattenRowToJson() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/event-converter.parquet", "event-converter.parquet");
		
		final KinesisSink sink = create();
		
		final List<Row> json = sink.jsonify(df).collectAsList();
		
		for(final Row r : json) {
			System.out.println(r.getString(0));
			// check that the r.getString is valid Json
			
		}
	}
	
	protected KinesisSink create() {
		final KinesisSink sink = new KinesisSink("eu-west-1", "stream");
		sink.builder = akcb;
		return sink;
	}
	
	public static class AnswerWithSelf implements Answer<Object> {
	    private final Answer<Object> delegate = new ReturnsEmptyValues();
	    private final Class<?> clazz;

	    public AnswerWithSelf(Class<?> clazz) {
	        this.clazz = clazz;
	    }

	    public Object answer(InvocationOnMock invocation) throws Throwable {
	        Class<?> returnType = invocation.getMethod().getReturnType();
	        if (returnType == clazz) {
	            return invocation.getMock();
	        } else {
	            return delegate.answer(invocation);
	        }
	    }
	}
}
