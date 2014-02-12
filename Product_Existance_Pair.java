/*@name Ali Mousa
 *Product existance using pair mapper and stripe reducer
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
 
public class Product_Existance_Pair
{
 
 
    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>  
    {
      public Mapper()
      {
	      super();
      }
 
      @Override
      public void map(LongWritable FID, Text value, Context context) throws IOException, InterruptedException  
      {
        String key = null;
        String totalKey;
        String id;
	List<String> list = new LinkedList<String>();
        String line = value.toString();
        Scanner scanner = new Scanner(line);
	boolean found = false;
        while (scanner.hasNext()) 
	{
          String token = scanner.next();
	  for(String item : list)
	  {
		IntWritable temp;
		if(item == token)
		{
			found = true;
			continue;
		}
		context.write(new Text(item), new Text(token));
	  }
	  if(!found)
	  {
	  	list.add(token);
	  }
	  found = false;
        }
      }
    }
 
    public static class Reduce  extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, FloatWritable>
    {
      @Override
      public void reduce(Text key, Iterable<Text> list, Context context) throws IOException, InterruptedException
      {
        int sum = 0;
	HashMap<String, Integer> assoc = new HashMap<String, Integer>();
        Iterator<Text> values = list.iterator();
        while (values.hasNext())
        {
		String value = values.next().toString();
		if(assoc.containsKey(value))
		{
			assoc.put(value, assoc.get(value) + 1);
		}
		else
		{
			assoc.put(value, 1);
		}
		sum++;
        }
	for(String id : assoc.keySet())
	{
		context.write(new Text(key + "," + id), new FloatWritable((float) assoc.get(id)/sum));
	}
      }
    }
 
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(Product_Existance_Pair.class);
        job.setJobName("Product existance pair");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Mapper.class);
//	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
 
    }
}

