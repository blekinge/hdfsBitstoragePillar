package dk.statsbiblioteket.bitrepository.processing;

import org.apache.hadoop.util.Tool;

import java.io.File;

/**
 * This is the interface to the Processing subsystem of the bit repository
 */
public interface Processing {


    /**
     * Add a file to the distributed cache, so that all nodes in the cluster have access to it. This method should be
     * used to distribute jar files to all nodes, before invoking a job
     * @see #invokeProcessingJob(String, String, Class, String...)
     * @param file the file to distribute
     * @param path the path in the hdfs cluster
     * @throws Exception if anything went wrong
     */
    public void addFileToProcessingNodes(File file, String path) throws Exception;

    /**
     * Invoke a tool on a collection in the bitrepository.
     * @param collectionID the id of the collection
     * @param outputFolder the output folder for the results
     * @param toolClass the toolClass to invoke. This class must be available on all the nodes
     * @param args the additional args to the tool
     * @return 0 if the process succeeded, 1 otherwise
     * @throws Exception if anything went wrong
     */
    public int invokeProcessingJob(String collectionID,
                                   String outputFolder,
                                   Class<? extends BitrepoTool> toolClass,
                                   String... args) throws Exception;
}
