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
import java.util.ArrayList;
import java.util.HashMap;

class Data
{
    String name;
    double weight;
    int label;
    Data(String n, double w, int l)
    {
        name = n;
        weight =w;
        label = l;
    }
}

class IteratorMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    // name [\t] label [\t] [list]
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String []tokens = value.toString().split("\t");
        int originLabel = Integer.parseInt(tokens[1]);
        String []names = tokens[2].split(";");
        ArrayList<Data> list = new ArrayList<>();
        HashMap<Integer, Double> map = new HashMap<>();
        double maxWeight = 0;
        int maxLabel = -1;
        for (String name: names)
        {
            String[] strs = name.split(",");
            double w = Double.parseDouble(strs[1]);
            int label = Integer.parseInt(strs[2]);
            list.add(new Data(strs[0], w, label));
            map.put(label, map.containsKey(label) ? (map.get(label) + w) : w);
            if (map.get(label) > maxWeight)
            {
                maxWeight = map.get(label);
                maxLabel = label;
            }
        }
        if (maxLabel != originLabel) {
            for (Data d: list)
                context.write(new Text(d.name), new Text("#" + tokens[0] + "," + maxLabel));
        }
        context.write(new Text(tokens[0]), new Text(maxLabel + "\t" + tokens[2]));
    }
}


class IteratorReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer>changedList = new HashMap<>();
        ArrayList<Data> names = new ArrayList<>();
        int label = -1;
        for (Text t: values)
        {
            String item = t.toString();
            if (item.startsWith("#"))
            {
                item = item.substring(1);
                String[] tokens = item.split(",");
                changedList.put(tokens[0], Integer.parseInt(tokens[1]));
            }
            else
            {
                label = Integer.parseInt(item.split("\t")[0]);
                String[] tokens = item.split("\t")[1].split(";");
                for (String token: tokens)
                {
                    String[] strs = token.split(",");
                    names.add(new Data(strs[0], Double.parseDouble(strs[1]), Integer.parseInt(strs[2])));
                }
            }
        }
        String out = String.valueOf(label) + "\t";
        boolean first = true;
        for (int i = 0; i < names.size(); i++)
        {
            if (!first)
                out = out + ";";
            first = false;
            Data data = names.get(i);
            out = out + data.name + "," + String.valueOf(data.weight) + ",";
            if (changedList.containsKey(data.name))
                out = out + changedList.get(data.name);
            else
                out = out + String.valueOf(data.label);
        }
        context.write(key, new Text(out));
    }
}


class LabelPropagation {
    static void main(String dir) throws Exception
    {
        int time = 1;
        while(time <= 20)        //TODO change this to stop condition of the algorithm
        {
            String inputDir = dir + "/iter" + String.valueOf(time - 1);
            String outputDir = dir + "/iter" + time;
            iterate(inputDir, outputDir);
            time = time + 1;
        }
    }

    private static void iterate(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Community detection Iterator");
        job.setJarByClass(LabelPropagation.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(IteratorMapper.class);
        job.setReducerClass(IteratorReducer.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
