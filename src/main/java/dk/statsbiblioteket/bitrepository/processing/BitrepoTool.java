package dk.statsbiblioteket.bitrepository.processing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.util.Arrays;

/**
 * This is a slightly specialised instance of a hadoop processing tool. This is meant to be the basis for
 * hadoop jobs in the bitrepository
 */
public abstract class BitrepoTool extends Configured implements Tool {

    /**
     * the name of the tool
     */
    private String toolname;

    /**
     * The tool must have no-args constructor
     */
    public BitrepoTool() {
    }

    /**
     * This is the method to implement to make your tool Do Stuff
     * @param collectionID the collection ID of the collection to work on
     * @param outputFolder the folder, relative or absolute, of where the output should be placed.
     *                     This folder will not exist previously, the tool will have to create it.
     * @param args additional args to the tool
     * @return the return code, 0 or 1 depending on success
     * @throws Exception if anything went wrong
     */
    public abstract int run(String collectionID,String outputFolder, String... args) throws Exception;


    public final int run(String[] args) throws Exception {
        String collectionID = args[0];
        String outputFolder = args[1];
        return run(collectionID,outputFolder, Arrays.copyOfRange(args,2,args.length ));
    }

    /**
     * Get the tool name or null if not set
     * @return the tool name
     */
    public String getToolname() {
        return toolname;
    }

    /**
     * Set the tool name
     * @param toolname the tool name
     */
    public void setToolname(String toolname) {
        this.toolname = toolname;
    }
}
