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

class ViewerMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    //name pagerank
    //name label ......
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split("\t");
        context.write(new Text(strings[1]), new Text(strings[0]));
    }
}

class ViewerReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t: values)
            context.write(t, key);
    }
}


class ResultViewer {
    static void main(String label, String output) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ResultViewer");
        job.setJarByClass(ResultViewer.class);
        job.setMapperClass(ViewerMapper.class);
        job.setReducerClass(ViewerReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(label));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
