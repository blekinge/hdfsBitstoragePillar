package dk.statsbiblioteket.bitrepository;

import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/16/13
 * Time: 1:46 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Processing {

    public void addFileToProcessingNodes(File file);

    public int invokeProcessingJob(String collectionID, Tool tool,String... args) throws IOException, InterruptedException;
}
