/*@name Ali Mousa
 *Using Pair approch for both mapper and reducer
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Product_Occurance 
{
    public static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>  
    {
      private Text word = new Text();
      private HashMap<String, Integer>  assoc;
      public Map()
      {
      	assoc = new HashMap<String, Integer>();
      }

    @Override
    public void map(LongWritable FID, Text value, Context output) throws IOException 
    {
        String key = null;
        List<String> list  = new ArrayList<String>();
        String totalKey;
        String id;
        String line = value.toString();
        Scanner scanner = new Scanner(line);
        String template = "%s, %s";
        String num;
	boolean found = false;
        while (scanner.hasNext()) 
        {
            String currentItem;
            num = scanner.next();
            if(list.size() != 0)
            {
                for(String item : list)
                {
		    if(num.equals(item))
		    {
			    found = true;
			    continue;
		    }
                    totalKey = String.format(template, item, "*");
                    if(assoc.containsKey(totalKey))
                    {
                        assoc.put(totalKey, assoc.get(totalKey) + 1); 
                    }
                    else
                    {
                        assoc.put(totalKey, 1); 
                    }
                    currentItem = String.format(template, item, num);
                    if(!assoc.containsKey(currentItem))
                    {
                        assoc.put(currentItem, 1);
                    }
                    else
                    {
                        assoc.put(currentItem, assoc.get(currentItem) + 1);
                    }
                }
            }
	    if(!found)
	    {
            	list.add(num);
	    }
	    found = false;
        }
      }
      @Override
      protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws java.io.IOException, java.lang.InterruptedException
      {
	      for(String key : assoc.keySet())
	      {
	      	 context.write(new Text(key), new IntWritable(assoc.get(key)));
	      }
      }
    }

    public static class CustomPartitioner<Text, IntWritable> extends org.apache.hadoop.mapreduce.Partitioner<Text, IntWritable> 
    {
	    @Override
	    public int getPartition(Text key, IntWritable value, int numReduceTasks) 
	    {
		    return (key.toString().split(",")[0]).hashCode() % numReduceTasks;
	    }
    }
    public static class Reduce  extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, FloatWritable> 
    {
      int total;

      @Override
      public void reduce(Text key, Iterable<IntWritable> list, Context context) throws IOException, InterruptedException 
      {
        int sum = 0;
	Iterator<IntWritable> values = list.iterator(); 
        while (values.hasNext()) 
	{
         	 sum += values.next().get();
        }
	if(key.toString().indexOf('*') != -1)
	{
		total = sum ;	
	}
	else
	{
        	context.write(key, new FloatWritable((float) sum/total));
	}
      }
    }

    public static void main(String[] args) throws Exception 
    {
    	Job job = new Job();
        job.setJarByClass(Product_Occurance.class);
        job.setJobName("Co existance");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
//        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    	System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

