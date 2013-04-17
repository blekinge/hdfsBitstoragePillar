package dk.statsbiblioteket.scape.hadoop.bitrepository;

import dk.statsbiblioteket.bitrepository.FileAlreadyExistsException;
import dk.statsbiblioteket.bitrepository.FileID;
import dk.statsbiblioteket.bitrepository.FileStatus;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/15/13
 * Time: 4:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class HadoopPillarTest {
    private HadoopPillar hadoopPillar;
    private FileID testfile;

    @org.junit.Before
    public void setUp() throws Exception {
        hadoopPillar = new HadoopPillar("scape",URI.create("target/"));
        /*hadoopPillar = new HadoopPillar("scape",new URI("hdfs://zone1.isilon.sblokalnet/"));*/
        testfile = new FileID("testFile","testColl");
        hadoopPillar.storeInTemporaryStore(testfile,Thread.currentThread().getContextClassLoader().getResourceAsStream("datafile1"));
    }

    @org.junit.After
    public void tearDown() throws Exception {
        Collection<FileID> files = hadoopPillar.getAllFileIds("testColl");
        for (FileID file : files) {
            hadoopPillar.deleteFile(file);
        }
    }

    @org.junit.Test
    public void testGetFileContents() throws Exception {
        InputStream stream = hadoopPillar.getFileContents(testfile);
        StringWriter writer = new StringWriter();
        IOUtils.copy(stream,writer);
        assertThat("datafile1\n" +
                "datafile1\n" +
                "or\n" +
                "datafile1",is(writer.toString()));

    }

    @org.junit.Test
    public void testGetFileStatus() throws Exception {
        FileStatus status = hadoopPillar.getFileStatus(testfile);
        assertThat(status.getFileID(),is(testfile));
        assertThat(status.isArchived(),is(false));
        assertThat(status.getFileSize(),is((long)(("datafile1\n" +
                "datafile1\n" +
                "or\n" +
                "datafile1").length())));
    }

    @org.junit.Test
    public void testGetAllFileIds() throws Exception {
        Collection<FileID> ids = hadoopPillar.getAllFileIds("testColl");
        for (FileID id : ids) {
            System.out.println(id);
        }
        assertFalse(ids.isEmpty());

    }

    @org.junit.Test
    public void testStoreInTemporaryStore() throws Exception {
        InputStream contents = new ByteArrayInputStream("datafile2".getBytes());

        FileID fileID = new FileID("datafile2","testColl");
        hadoopPillar.storeInTemporaryStore(fileID,contents);

        InputStream readContents = hadoopPillar.getFileContents(fileID);
        assertThat(IOUtils.toString(readContents),is("datafile2"));

        assertFalse(hadoopPillar.getFileStatus(fileID).isArchived());

        hadoopPillar.deleteFile(fileID);

        try{
            hadoopPillar.getFileStatus(fileID);
            fail();
        } catch (FileNotFoundException e){

        }

    }

    @org.junit.Test
    public void testMoveToArchive() throws Exception {
        boolean result = hadoopPillar.moveToArchive(testfile);
        FileStatus status = hadoopPillar.getFileStatus(testfile);
        assertThat(status.isArchived(),is(true));

        try {
            hadoopPillar.moveToArchive(testfile);
            fail();
        } catch (FileAlreadyExistsException e){

        }

    }

    @org.junit.Test
    public void testDeleteFile() throws Exception {
        hadoopPillar.deleteFile(testfile);
        try {
            hadoopPillar.getFileStatus(testfile);
            fail();
        } catch (FileNotFoundException e){

        }
    }
}
