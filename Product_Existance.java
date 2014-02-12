/*@name Ali Mousa
 *stipe approch
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
 
public class Product_Existance 
{
 
 
    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, MapWritable>  
    {
      private Text word = new Text();
      private Map<String, MapWritable>  assoc;
      public Mapper()
      {
        assoc = new HashMap<String, MapWritable>();
      }
 
      @Override
      public void map(LongWritable FID, Text value, Context output) throws IOException {
        String key = null;
        String totalKey;
        String id;
	boolean found = false;
	List<String> list = new LinkedList<String>();
        String line = value.toString();
        Scanner scanner = new Scanner(line);
        while (scanner.hasNext()) 
	{
          String token = scanner.next();
	  for(String item : list)
	  {
		MapWritable temp;
		if(item.equals(token))
		{
			found = true;
			continue;
		}
		if(!assoc.containsKey(item))
		{
			temp = new MapWritable();
			temp.put(new Text(token), new IntWritable(1));
			assoc.put(item, temp);
		}
		else
		{
			temp = assoc.get(item);
			if(!temp.containsKey(new Text(token)))
			{
				temp.put(new Text(token), new IntWritable(1));
			}
			else
			{
				temp.put(new Text(token), new IntWritable(((IntWritable) temp.get(new Text(token))).get() + 1));
			}
		}
	  }
	  if(!found)
	  {
	  	list.add(token);
	  }
	  found = false;
        }
      }
      @Override
      protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws java.io.IOException, java.lang.InterruptedException
      {
              for(String key : assoc.keySet())
              {
                 context.write(new Text(key), assoc.get(key));
              }
      }
    }
 
    public static class Reduce  extends org.apache.hadoop.mapreduce.Reducer<Text, MapWritable, Text, FloatWritable>
    {
      @Override
      public void reduce(Text key, Iterable<MapWritable> list, Context context) throws IOException, InterruptedException
      {
        int sum = 0;
	Map<String, Integer> assoc = new HashMap<String, Integer>();
        Iterator<MapWritable> values = list.iterator();
	int value; 
        while (values.hasNext())
        {
                 MapWritable temp  = values.next();
		 for(Writable id : temp.keySet())
		 {
			 value = ((IntWritable) temp.get(id)).get();
			 sum += value;
			 assoc.put(((Text) id).toString(), value);
		 }
        }
	for(String id : assoc.keySet())
	{
		context.write(new Text(key + "," + id), new FloatWritable((float) assoc.get(id)/sum));
	}
      }
    }
 
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(Product_Existance.class);
        job.setJobName("Product existance");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Mapper.class);
//	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
 
    }
}

