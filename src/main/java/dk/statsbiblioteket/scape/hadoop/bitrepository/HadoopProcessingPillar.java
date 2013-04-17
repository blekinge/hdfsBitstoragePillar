package dk.statsbiblioteket.scape.hadoop.bitrepository;

import dk.statsbiblioteket.bitrepository.Processing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/16/13
 * Time: 1:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class HadoopProcessingPillar implements Processing{

    private final Configuration configuration;
    private final String hadoopUser;
    private String[] hadpopargs;

    public HadoopProcessingPillar(Configuration configuration, String hadoopUser, String... hadpopargs) {
        this.configuration = configuration;
        this.hadoopUser = hadoopUser;

        this.hadpopargs = hadpopargs;
    }

    public void addFileToProcessingNodes(File file) {

        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int invokeProcessingJob(String collectionID, final Tool tool,  String... args) throws IOException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUser);

        //OK, what do we want to do
        //Generic args should be set in the constructor
        //Libs should be distributed in the other method
        //Then the tool should be invoked here
        ArrayList<String> argsList = new ArrayList<String>();
        argsList.addAll(Arrays.asList(hadpopargs));
        argsList.addAll(Arrays.asList(args));
        final String[] argsComplete = argsList.toArray(new String[argsList.size()]);


        final int[] returnCode = {0};
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                configuration.set("hadoop.job.ugi", hadoopUser);

                tool.setConf(configuration);
                int res = tool.run(argsComplete);
                returnCode[0] = res;
                return null;
            }
        });
        return returnCode[0];
    }




}
