package dk.statsbiblioteket.bitrepository.processing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/17/13
 * Time: 10:03 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class BitrepoTool extends Configured implements Tool {

    private String toolname;

    public BitrepoTool() {
    }

    public abstract int run(String collectionID,String outputFolder, String... args) throws Exception;

    public final int run(String[] args) throws Exception {
        String collectionID = args[0];
        String outputFolder = args[1];
        return run(collectionID,outputFolder, Arrays.copyOfRange(args,2,args.length ));
    }

    public String getToolname() {
        return toolname;
    }

    public void setToolname(String toolname) {
        this.toolname = toolname;
    }
}
