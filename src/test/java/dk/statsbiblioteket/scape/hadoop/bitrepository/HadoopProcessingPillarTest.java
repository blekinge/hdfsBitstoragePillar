package dk.statsbiblioteket.scape.hadoop.bitrepository;

import dk.statsbiblioteket.scape.hadoop.bitrepository.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/17/13
 * Time: 8:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class HadoopProcessingPillarTest {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testInvokeProcessingJob() throws Exception {
        Configuration conf = new Configuration(true);
        conf.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("core-site.xml"));
        conf.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("mapred-site.xml"));
        File outputDir = new File("target/" + UUID.randomUUID().toString());


        HadoopProcessingPillar pillar = new HadoopProcessingPillar(conf, "Scape");


        File collectionFolder = new File("target/testcoll");
        collectionFolder.mkdirs();


        pillar.invokeProcessingJob(collectionFolder.getAbsolutePath(),outputDir.getAbsolutePath(), WordCount.class);

        String[] files = outputDir.list();

        assertTrue(files.length>0);
        boolean success = false;
        for (String file : files) {
            if (file.contains("SUCCESS")){
                success = true;
            }
        }
        assertTrue(success);
    }
}
