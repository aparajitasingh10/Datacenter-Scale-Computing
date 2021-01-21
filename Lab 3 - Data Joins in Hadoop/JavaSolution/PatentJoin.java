import java.io.IOException;
import java.util.*;
        
import java.lang.Integer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

public class PatentJoin {

    static public class CheckCitationCountMapper
	extends Mapper<Object, Text, LongWritable, Text> {
	// Counter enum
	static enum CheckCitationCounter { 
	    MAP_CITATION, MAP_PATENT
	};
	//
	// Just create one of each key/value and then set it to
	//
	private LongWritable mykey = new LongWritable();
	private Text myvalue = new Text();


	//
	// "map" is called for each record (line)
	//
	public void map(Object key, Text value, Context context )
	    throws IOException, InterruptedException {

	    String line = value.toString();
	    String[] words = line.split(",");

	    if ( words.length == 2 ) {
		try {
		    mykey.set(Long.parseLong(words[0]));
		    myvalue.set("y");
		    context.write(mykey, myvalue);
		    context.getCounter(CheckCitationCounter.MAP_CITATION).increment(1);
		} catch (java.lang.NumberFormatException e) {
		    // ignore this - bogus data
		    System.out.println("ERROR-1");
		}
	    } else {
		try {
		    mykey.set(Long.parseLong(words[0]));
		    String allButFirst = String.join(",",
						     Arrays.copyOfRange(words, 1, words.length)
						     );
		    myvalue.set(allButFirst);
		    context.write(mykey, myvalue);
		    context.getCounter(CheckCitationCounter.MAP_PATENT).increment(1);
		} catch (java.lang.NumberFormatException e) {
		    // ignore this - bogus data
		    System.out.println("ERROR-2");
		}
	    }
	    
	}
    }


    static public class CheckCitationCountReducer
	extends Reducer<LongWritable,Text,Text,Text> {

	// Counter types
	static enum CheckCitationCounter { 
	    REDUCE_GOOD, REDUCE_BAD
	};


	private Text outkey = new Text();
	private Text outvalue = new Text();

	public void reduce(LongWritable key, Iterable<Text> values, Context context )
	    throws IOException, InterruptedException {
	    //
	    // Key is the patent # and is ignored
	    // We first need to find the patent reference,
	    // and if there is none, do nothing
	    //
	    String patentInfo = null;
	    int patentCitations = 0;

	    ArrayList<String> citations = new ArrayList<String>();

	    for (Text value : values) {

		String[] fields = value.toString().split(",");

		if ( fields.length > 2 ) {
		    patentInfo = value.toString();
		    try {
			//
			// The 11th field is 12th in the original data -- number of citations
			//
			patentCitations = Integer.parseInt( fields[11] );
		    } catch( java.lang.NumberFormatException e) {
			// leave as -1 -- bogus data
		    }
		} else {
		    citations.add( value.toString() );
		}
	    }

	    //
	    // We might have citations for which we have no patent info...
	    //
	    outkey.set( key.toString() );
	    if ( patentInfo == null ) {
		outvalue.set("bad missing patent info");
	    } else {
		if ( patentCitations == citations.size() ) {
		    outvalue.set( "ok");
			context.getCounter(CheckCitationCounter.REDUCE_GOOD).increment(1);
		} else {

		    String outstr =  "bad cites got "
			+ Integer.toString(patentCitations) + " expected "
			+ Integer.toString(citations.size());

		    outvalue.set( outstr );
		    context.getCounter(CheckCitationCounter.REDUCE_BAD).increment(1);
		}
	    }
	    context.write(outkey, outvalue);
	}
    };


    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
        
	Job job = Job.getInstance(conf, "patent check");

	job.setJarByClass(PatentJoin.class);
	
	job.setMapperClass(CheckCitationCountMapper.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);
      
	//
	// We can not set a Combiner because we rely on having all
	// the information for the join
	//
	job.setReducerClass(CheckCitationCountReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
