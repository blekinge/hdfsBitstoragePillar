package dk.statsbiblioteket.scape.hadoop.bitrepository;

import dk.statsbiblioteket.bitrepository.processing.BitrepoTool;
import dk.statsbiblioteket.bitrepository.processing.Processing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    private String[] hadoopArgs;
    private URI filesystemDefaultURI;

    public HadoopProcessingPillar(Configuration configuration, String hadoopUser, String... hadoopArgs) {
        this.configuration = configuration;
        this.hadoopUser = hadoopUser;

        this.hadoopArgs = hadoopArgs;
    }

    public void addFileToProcessingNodes(File file, String path) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(filesystemDefaultURI, configuration, hadoopUser);
        Path destFile = new Path(path, file.getName());
        fileSystem.copyFromLocalFile(new Path(file.toURI()),new Path(path,file.getName()));
        DistributedCache.addFileToClassPath(destFile,configuration,fileSystem);
    }

    public int invokeProcessingJob(String collectionID,
                                   String outputFolder,
                                   Class<? extends BitrepoTool> toolClass,
                                   String... args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUser);

        //OK, what do we want to do
        //Generic args should be set in the constructor
        //Libs should be distributed in the other method
        //Then the tool should be invoked here
        ArrayList<String> argsList = new ArrayList<String>();
        argsList.addAll(Arrays.asList(hadoopArgs));
        argsList.add(collectionID);
        argsList.add(outputFolder);
        argsList.addAll(Arrays.asList(args));
        final String[] argsComplete = argsList.toArray(new String[argsList.size()]);

        final BitrepoTool tool;
        tool = toolClass.newInstance();


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
