package dk.statsbiblioteket.scape.hadoop.bitrepository.wordcount;

import dk.statsbiblioteket.bitrepository.processing.BitrepoTool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.ArrayList;
import java.util.List;
/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/17/13
 * Time: 9:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class WordCount extends  BitrepoTool {


    public WordCount() {
        setToolname("Wordcount");
    }

    @Override
    public int run(String collectionID, String outputFolder, String... args) throws Exception {
        JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName(getToolname());

        FileInputFormat.setInputPaths(conf, new Path(collectionID));
        conf.setInputFormat(TextInputFormat.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(conf, new Path(outputFolder));

        RunningJob result = JobClient.runJob(conf);
        if (result.isSuccessful()){
            return 0;
        }
        return 1;
    }

}
