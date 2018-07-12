package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

class Comparator extends DoubleWritable.Comparator
{
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -super.compare(a, b);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -super.compare(b1, s1, l1, b2, s2, l2);
    }
}

class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String name = line[0];
        double pr = Double.parseDouble(line[1]);
        context.write(new DoubleWritable(pr), new Text(name));
    }
}

class Sorter {
    static void main(String input, String output) throws Exception
    {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Pagerank sorter");
        job.setJarByClass(Sorter.class);
        job.setMapperClass(SortMapper.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setSortComparatorClass(Comparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
