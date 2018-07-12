package communitydetection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;


class InitMapper extends Mapper<Object, Text, Text, Text> {
    //value : name [\t] pr [\t] list
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String []tokens = value.toString().split("\t");
        if (tokens.length >= 3)
            context.write(new Text(tokens[0]), new Text(tokens[2]));
        else
            System.out.println("ERROR ERROR ERROR ERROR\n");
    }
}

class InitReducer extends Reducer<Text, Text, Text, Text> {

    private static int counter = 1;

    private HashMap<String, Integer> hash = new HashMap<>();

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String val = "";
        if (hash.containsKey(key.toString()))
            val = val + hash.get(key.toString()) + "\t";
        else
        {
            hash.put(key.toString(), counter);
            val = val+(counter++ + "\t");
        }
        boolean first = true;
        for (Text t : values) {
            String[] tokens = t.toString().split(";");
            for (String token: tokens)
            {
                if (!first)
                    val = val + ";";
                first = false;
                String name = token.split(",")[0];
                if (hash.containsKey(name))
                    val = val +token + ","+ hash.get(name);
                else
                {
                    hash.put(name, counter);
                    val = val + (token + "," + counter++);
                }
            }
        }
        context.write(key, new Text(val));
    }
}


public class Init {
    public static void main(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Initiate all nodes's labels. ");
        job.setMapperClass(InitMapper.class);
        job.setReducerClass(InitReducer.class);
        job.setJarByClass(Init.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
