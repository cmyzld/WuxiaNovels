package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

class PRMapper extends Mapper<Object, Text, Text, Text> {
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tuple = value.toString().split("\t");
        String k = tuple[0];
        double pr = Double.parseDouble(tuple[1]);
        if (tuple.length > 2) {
            String[] linkNames = tuple[2].split(";");
            for (String linkname : linkNames) {
                String[] token = linkname.split(",");
                String prValue = String.valueOf(pr * Double.parseDouble(token[1]));
                context.write(new Text(token[0]), new Text(prValue));
            }
            context.write(new Text(k), new Text("#" + tuple[2]));
        }
    }
}

class PRReducer extends Reducer<Text, Text, Text, Text> {

    private static final double damping = 0.85;

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double pagerank = 0;
        String links = "";
        for (Text text: values)
        {
            String temp = text.toString();
            if (temp.startsWith("#"))
            {
                links = "\t" + temp.substring(temp.indexOf("#") + 1);
                continue;
            }
            pagerank += Double.parseDouble(temp);
        }
        pagerank = (1 - damping) + damping * pagerank;
        context.write(new Text(key), new Text(String.valueOf(pagerank) + links));
    }
}

public class PageRank {

    private final static int times = 50;

    public static void main(String dir) throws Exception {
        String input, output;
        for (int i = 1; i <= times; i++) {
            input = dir + "/iter" + String.valueOf(i - 1);
            output = dir + "/iter" + i;
            iterate(input, output);
        }
        input = dir + "/iter" + times;
        output = dir + "/finalPageRank";
        Sorter.main(input, output);
    }

    private static void iterate(String input, String output) throws Exception
    {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Pagerank Iterator");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRMapper.class);
        job.setReducerClass(PRReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
