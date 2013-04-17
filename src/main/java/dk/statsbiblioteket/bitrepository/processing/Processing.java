package dk.statsbiblioteket.bitrepository.processing;

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

    public int invokeProcessingJob(String collectionID,
                                   String outputFolder,
                                   Class<? extends BitrepoTool> toolClass,
                                   String... args) throws Exception;
}
