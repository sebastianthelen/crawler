import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class Crawler {
	
	final static String cellarBaseUri = "publications.europa.eu";
	
	final static String outboundQueryTemplate = "prefix cdm: <http://publications.europa.eu/ontology/cdm#>"
			+ " construct '{'<http://{0}/resource/cellar/{1}> ?p ?o'}'"
			+ " where '{'<http://{0}/resource/cellar/{1}> ?p ?o'}'";
	final static String inboundQueryTemplate = "prefix cdm: <http://publications.europa.eu/ontology/cdm#>"
			+ " construct '{'?s ?p <http://{0}/resource/cellar/{1}>'}'"
			+ " where '{'?s ?p <http://{0}/resource/cellar/{1}>'}'";
	final static String embeddingQueryTemplate = "baz";

	public Crawler() {
	}

	static class MyMapper extends TableMapper<Text, Text> {
		static final byte[] CF = "cf".getBytes();
		static final byte[] TYPE = "type".getBytes();

		public MyMapper() {
		}

		protected void map( ImmutableBytesWritable inKey, Result columns, Context context ) throws IOException, InterruptedException {
			// type of operation, i.e., CREATE, UPDATE, DELETE
			String type = new String(columns.getValue(CF, TYPE));
			Text outKey = new Text(Bytes.toString(inKey.get()));
			context.write( outKey,  new Text(type));
		}
	}

	// <KEYIN,VALUEIN,KEYOUT>
	static class MyReducer extends TableReducer<Text, Text, Text> {
		// column families
		static final byte[] DMD = "dmd".getBytes();
		static final byte[] MISC = "misc".getBytes();
		// columns identifiers
		static final byte[] INBOUND = "inbound".getBytes();
		static final byte[] OUTBOUND = "outbound".getBytes();
		static final byte[] CONTENT = "content".getBytes();

		HTable stagingTable;
		List<Delete> deleteList = new ArrayList<Delete>();
		final int buffer = 10000; /* Buffer size, tune it as desired */
		
		public MyReducer() {
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			stagingTable = new HTable(HBaseConfiguration.create(), "staging".getBytes());
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (deleteList.size() > 0) {
				stagingTable.delete(deleteList); 
			}
			stagingTable.close(); 
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String type = values.iterator().next().toString();
			if (type.equals("CREATE") || type.equals("UPDATE")) {
				Put put = aggregate(key);
				context.write(null, put);
			} else if (type.equals("DELETE")) {
				deleteList.add(new Delete(key.getBytes()));
				if (deleteList.size() == buffer) {
					stagingTable.delete(deleteList);
					deleteList.clear();
				}
			} else {
				System.out.println("type = \"" + type + "\" unsupported");
			}
		}

		public static Put aggregate(Text key){
			Put put = new Put(Bytes.toBytes(key.toString()));
			// incoming relations
			byte[] inbound = getInboundTriples(key);
			put.add(DMD, INBOUND, inbound);
			// outgoing relations
			byte[] outbound = getOutboundTriples(key);
			put.add(DMD, OUTBOUND, outbound);
			// concepts
			List<byte[]> concepts = getConcepts(key);
			for (byte[] emb : concepts){
				put.add(MISC, emb, "1".getBytes());
			}            
			byte[] content = getContent(key);
			put.add(MISC, CONTENT, content); 
			return put;
		}

		private static byte[] getContent(Text id) {
			// TODO Auto-generated method stub
			return "myContent".getBytes();
		}

		private static List<byte[]> getConcepts(Text id) {
			// TODO Auto-generated method stub
			List<byte[]> res =  new ArrayList<byte[]>();
			res.add("myConcept".getBytes());
			return res;
		}

		private static byte[] getOutboundTriples(Text id) {
			String query = MessageFormat.format(
					outboundQueryTemplate, cellarBaseUri, id.toString());
			VirtuosoSparqlConnector sparqlConncetor = 
					new VirtuosoSparqlConnector("cellar.publications.europa.eu");
			String nTriples = null;
			try {
				nTriples = sparqlConncetor.request(query);
				System.out.println(nTriples);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return nTriples.getBytes();
		}

		private static byte[] getInboundTriples(Text id) {
			String query = MessageFormat.format(
					inboundQueryTemplate, cellarBaseUri, id.toString());
			VirtuosoSparqlConnector sparqlConncetor = 
					new VirtuosoSparqlConnector("cellar.publications.europa.eu");
			String nTriples = null;
			try {
				nTriples = sparqlConncetor.request(query);
				System.out.println(nTriples);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return nTriples.getBytes();
		}
	}

	public static void main( String[] args ) {
		try {
			// Setup Hadoop
			Configuration conf = HBaseConfiguration.create();
			Job job = Job.getInstance(conf, "events");
			job.setJarByClass( Crawler.class );

			// Create a scan
			Scan scan = new Scan();

			// Configure the Map process to use HBase
			TableMapReduceUtil.initTableMapperJob(
					"events",                    	// The name of the table
					scan,                           // The scan to execute against the table
					MyMapper.class,                 // The Mapper class
					Text.class,             		// The Mapper output key class
					Text.class,            			// The Mapper output value class
					job );                          // The Hadoop job

			TableMapReduceUtil.initTableReducerJob(
					"staging",        				// output table
					MyReducer.class,    			// reducer class
					job);

			// We'll run just one reduce task, but we could run multiple
			job.setNumReduceTasks( 1 );

			// Execute the job
			System.exit( job.waitForCompletion( true ) ? 0 : 1 );

		}
		catch( Exception e ) {
			e.printStackTrace();
		}
	}
}
