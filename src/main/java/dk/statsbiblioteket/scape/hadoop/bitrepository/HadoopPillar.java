package dk.statsbiblioteket.scape.hadoop.bitrepository;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 9/28/12
 * Time: 3:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class HadoopPillar implements FileStoreImproved {

    private FileSystem fileSystem;

    private final Path stageDir;
    private final Path archiveDir;
    public HadoopPillar(String user) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        fileSystem = FileSystem.get(FileSystem.getDefaultUri(conf), conf, user);
        stageDir = new Path("staging");
        archiveDir = new Path("archiving");
    }

    private Path getPath(Path dir,FileID fileID){
        return new Path(fileID.getCollectionID(),new Path(dir,fileID.getFileID()));
    }

    public InputStream getFileContents(FileID fileID) throws FileNotFoundException, IOException {
        Path finalPath = getPath(archiveDir,fileID);
        Path stagePath = getPath(stageDir,fileID);
        Path path;
        if (fileSystem.exists(finalPath)){
            path = finalPath;
        } else  if (fileSystem.exists(stagePath)){
            path = stagePath;
        } else {
            throw new FileNotFoundException("File "+fileID+" is not found in the filestore");
        }
        return fileSystem.open(path);
    }

    public FileStatus getFileStatus(FileID fileID) throws IOException {
        Path finalPath = getPath(archiveDir,fileID);
        Path stagePath = getPath(stageDir,fileID);
        Path path;
        boolean archived;
        if (fileSystem.exists(finalPath)){
            path = finalPath;
            archived = true;
        } else if (fileSystem.exists(stagePath)){
            path = stagePath;
            archived = false;
        } else {
            throw new FileNotFoundException("File "+fileID+" was not found in the filestore");
        }

        org.apache.hadoop.fs.FileStatus hdfsStatus = fileSystem.getFileStatus(path);
        return new FileStatus(fileID,hdfsStatus.getLen(),archived,hdfsStatus.getModificationTime());
    }

    public Collection<FileID> getAllFileIds(String collectionID) throws IOException {
        ArrayList<FileID> result = new ArrayList<FileID>();

        RemoteIterator<LocatedFileStatus> founds = fileSystem.listFiles(new Path(collectionID), true);
        while (founds.hasNext()){
            LocatedFileStatus found = founds.next();
            result.add(new FileID(found.getPath().getName(),collectionID));
        }
        return result;
    }

    public boolean storeInTemporaryStore(FileID fileID, InputStream inputStream) throws FileAlreadyExistsException, IOException {
        Path finalPath = getPath(archiveDir,fileID);
        Path stagePath = getPath(stageDir,fileID);
        fileSystem.mkdirs(stageDir);

        if (fileSystem.exists(finalPath) || fileSystem.exists(stagePath)){
            throw new FileAlreadyExistsException("File "+fileID+" already exists in the filestore, aborting");
        }
        FSDataOutputStream stageFile = fileSystem.create(stagePath);
        IOUtils.copyLarge(inputStream,stageFile);
        inputStream.close();
        stageFile.close();
        return true;
    }

    public boolean moveToArchive(FileID fileID) throws IOException {
        Path finalPath = getPath(archiveDir,fileID);
        Path stagePath = getPath(stageDir,fileID);
        if (fileSystem.exists(finalPath)){
            throw new FileAlreadyExistsException("File "+fileID+" already exists in the filestore, aborting");
        }
        if (!fileSystem.exists(stagePath)){
            throw new FileNotFoundException("File "+fileID+" does not exist in the filestore, aborting");
        }
        return fileSystem.rename(stagePath, finalPath);
    }



    public boolean deleteFile(FileID fileID) throws IOException {
        Path finalPath = getPath(archiveDir,fileID);
        Path stagePath = getPath(stageDir,fileID);
        Path path;
        if (fileSystem.exists(finalPath)){
            path = finalPath;
        } else  if (fileSystem.exists(stagePath)){
            path = stagePath;
        } else {
            throw new FileNotFoundException("File "+fileID+" is not found in the filestore");
        }
        return fileSystem.delete(path,false);
    }

}
