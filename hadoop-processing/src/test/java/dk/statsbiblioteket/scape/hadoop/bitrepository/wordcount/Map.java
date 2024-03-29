package dk.statsbiblioteket.scape.hadoop.bitrepository.wordcount;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/17/13
 * Time: 9:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    static enum Counters { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive = true;

    private long numRecords = 0;
    private String inputFile;

    public void map(
            LongWritable key, Text value,
            OutputCollector<Text, IntWritable> output,
            Reporter reporter)
            throws IOException {
        String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();


        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            output.collect(word, one);
            reporter.incrCounter(Counters.INPUT_WORDS, 1);
        }

        if ((++numRecords % 100) == 0) {
            reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
        }
    }
}
