package parser;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

class ParserMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Boolean first  = true;
    private HashSet<String> nameList = new HashSet<>();


    private void initDic()
    {
        for(String s: nameList)
            DicLibrary.insert(DicLibrary.DEFAULT, s, "nrr", 1000);
    }

    private List<String> parseNames(String value)
    {
        if(first) {
            initDic();
            first = false;
        }
        HashSet<String> list = new HashSet<>();
        Result parse = DicAnalysis.parse(value);
        for (Term term: parse)
            if (term.getNatureStr().equals("nrr"))
                list.add(term.getName());
        return new ArrayList<>(list);
    }

    protected void setup(Context context) {
        String names = context.getConfiguration().get("namelist");
        String[] list = names.split(",");
        Collections.addAll(nameList, list);
    }

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        List<String>names = parseNames(value.toString());
        for(int i = 0; i < names.size(); i++)
            for (int j = 0; j < i; j++) {
                context.write(new Text(names.get(j) + "," + names.get(i)), one);
                context.write(new Text(names.get(i) + "," + names.get(j)), one);
            }
    }
}


class ParserCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for(IntWritable val: values)
            sum += val.get();
        result.set(sum);
        context.write(key, result);
    }
}

class ParserPatitioner extends HashPartitioner<Text, IntWritable> {
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        String term = key.toString().split(",")[0];
        return super.getPartition(new Text(term), value, numReduceTasks);
    }
}

class ParserReducer extends Reducer<Text, IntWritable, Text, Text> {

    private String last = " ";
    private HashMap<String, Integer> edgeList = new HashMap<>();
    private long count = 0;

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String []names =  key.toString().split(",");
        if(names[0].compareTo(last) != 0)
        {
            if(last.compareTo(" ") != 0)
            {
                writeLastTerm(context);
                edgeList = new HashMap<>();
                count = 0;
            }
            last = names[0];
        }
        int sum = 0;
        for(IntWritable val: values)
            sum += val.get();
        if(edgeList.containsKey(names[1]))
            sum += edgeList.get(names[1]);
        count += sum;
        edgeList.put(names[1], sum);
    }

    private void writeLastTerm(Context context) throws InterruptedException, IOException
    {
        StringBuilder out = new StringBuilder();
        boolean first = true;
        double f = 1.0/count;
        out.append("1.0\t");
        for (String k: edgeList.keySet())
        {
            if(!first) out.append(";");
            first = false;
            out.append(k);
            out.append(",");
            out.append(String.format("%.6f", f * edgeList.get(k)));
        }
        context.write(new Text(last), new Text(out.toString()));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        writeLastTerm(context);
    }
}


public class JobParser {
    private static String readNamesList(String path)
    {
        String names = "";
        boolean first = true;
        try{
            File file = new File(path);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while((line = reader.readLine()) != null) {
                if(!first)
                    names += ",";
                first = false;
                names += line;
            }
            return names;
        }catch (IOException e) {
            System.out.println("failed to read the data from distributed cache");
        }
        return null;
    }


    public static void main(String inputPath, String outputPath, String nameListPath) throws Exception
    {
        Configuration configuration = new Configuration();
        String names = readNamesList(nameListPath);
        if (names == null)
        {
            System.out.println("FILE READ ERROR");
            return;
        }
        configuration.set("namelist", names);
        Job job = Job.getInstance(configuration, "Parse and count");

        job.setJarByClass(JobParser.class);
        job.setMapperClass(ParserMapper.class);
        job.setCombinerClass(ParserCombiner.class);
        job.setPartitionerClass(ParserPatitioner.class);
        job.setReducerClass(ParserReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }
}
