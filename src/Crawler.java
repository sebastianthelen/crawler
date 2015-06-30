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
import java.util.ArrayList;
import java.util.List;

public class Crawler {

    public Crawler() {
    }

    static class MyMapper extends TableMapper<Text, Text> {
        
    	public MyMapper() {
        }

        protected void map( ImmutableBytesWritable inKey, Result columns, Context context ) throws IOException, InterruptedException {
        	Text outKey = new Text();
        	String str = Bytes.toString(inKey.get());
        	String val = str.split("\\.")[0];
        	outKey.set(val);
        	context.write( outKey,  new Text(inKey.toString()));
        }
    }

    // <KEYIN,VALUEIN,KEYOUT>
    static class MyReducer extends TableReducer<Text, Text, Text> {
    	// column families
    	public static final byte[] DMD = "dmd".getBytes();
    	public static final byte[] MISC = "misc".getBytes();
    	// columns identifiers
    	public static final byte[] INBOUND = "inbound".getBytes();
    	public static final byte[] OUTBOUND = "outbound".getBytes();
    	public static final byte[] CONTENT = "content".getBytes();
        
    	public MyReducer() {
        }
    	
    	public static Put aggregate(Text key, Iterable<Text> values){
			Put put = new Put(Bytes.toBytes(key.toString()));
    		//Put put = new Put("foo".getBytes());
	        for (Text id : values) {
	        	// incoming relations
	        	byte[] inbound = getInboundTriples(id);
	            put.add(DMD, INBOUND, inbound);
	            // outgoing relations
	            byte[] outbound = getOutboundTriples(id);
	            put.add(DMD, OUTBOUND, outbound);
	            // concepts
	            List<byte[]> concepts = getConcepts(id);
	            for (byte[] emb : concepts){
	            	put.add(MISC, emb, "1".getBytes());
	            }            
	            byte[] content = getContent(id);
	            put.add(MISC, CONTENT, content); 
	        }
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
			// TODO Auto-generated method stub
			return "myOutboundTriples".getBytes();
		}

		private static byte[] getInboundTriples(Text id) {
			// TODO Auto-generated method stub
			return "myInboundTriples".getBytes();
		}

		@Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put aggregation = aggregate(key, values);         
            context.write(null, aggregation);;
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
