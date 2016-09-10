import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {
	
	public static class EquiJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text joinKey = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String rawTuple = value.toString();
			String[] tuple = rawTuple.split(",");
			joinKey.set(tuple[1]);
			
			context.write(joinKey, value);
		}
	}

	public static class EquiJoinReducer extends Reducer<Text,Text,Text,Text> 
	{
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{		
			List<String> table1 = new ArrayList<String>();
			Text result = new Text();
			
			for(Text t: values)
				table1.add(t.toString());
			
			for(int i=0; i < table1.size(); i++) {
	    		for(int j=i+1; j < table1.size(); j++){
	    			
	    			String[] table1tuples = table1.get(i).split(",");
	    		    String[] table2tuples = table1.get(j).split(",");

	    		    if(!table1tuples[0].equals(table2tuples[0]))
	    		    {
	    		    	result.set(table1.get(i)+", "+table1.get(j));
	    		    	context.write(null, result);
	    		    }
	        	}
	        }
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "equijoin");
		job.setJarByClass(equijoin.class);
 
		job.setMapperClass(EquiJoinMapper.class);
		job.setReducerClass(EquiJoinReducer.class);
 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}