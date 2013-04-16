package dk.statsbiblioteket.bitrepository;

import org.apache.commons.io.IOUtils;
import org.bitrepository.common.FileStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

/**
 *Mapper for using the Hadoop pillar with the somewhat odd standard FileStore interface
 */
public class InterfaceMapper implements FileStore {

    private FileStoreImproved fileStoreImproved;
    private File localTempFolder;

    public InterfaceMapper(FileStoreImproved fileStoreImproved, File localTempFolder) {
        this.fileStoreImproved = fileStoreImproved;
        this.localTempFolder = localTempFolder;
    }

    public FileInputStream getFileAsInputstream(String s, String s2) throws IOException {
        File file = getFile(s, s2);
        return new FileInputStream(file);
    }

    public File getFile(String s, String s2) {
        File localFile = getLocalFile(s, s2);
        if (! localFile.exists()){
            InputStream contents = null;
            try {
                contents = fileStoreImproved.getFileContents(new FileID(s2, s));
                IOUtils.copyLarge(contents,new FileOutputStream(localFile));
                contents.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return localFile;

    }

    private File getLocalFile(String fileID, String collectionID){
        File localFile = new File(localTempFolder, fileID + "/" + collectionID);
        return localFile;
    }

    public boolean hasFile(String s, String s2) {
        try {
            File localFile = getLocalFile(s, s2);
            if (localFile.exists()){
                return true;
            }
            FileStatus status = fileStoreImproved.getFileStatus(new FileID(s2, s));
            return true;
        } catch (IOException e) {
            return false;
        }

    }

    public Collection<String> getAllFileIds(String s) {
        try {
            Collection<FileID> fileIDs = fileStoreImproved.getAllFileIds(s);
            ArrayList<String> result = new ArrayList<String>(fileIDs.size());
            for (FileID fileID : fileIDs) {
                result.add(fileID.getFileID());
            }
            return result;
        } catch (IOException e) {
            return null;
        }
    }

    public File downloadFileForValidation(String s, String s2, InputStream inputStream) throws IOException {
        File localFile = getLocalFile(s, s2);
        if (! localFile.exists()){
            localFile.createNewFile();
            IOUtils.copyLarge(inputStream,new FileOutputStream(localFile));
            inputStream.close();
            return localFile;
        } else {
            throw new FileAlreadyExistsException("File already exists, download failed");
        }
    }

    public void moveToArchive(String s, String s2) {
        try {
            File localFile = getLocalFile(s, s2);
            fileStoreImproved.storeInTemporaryStore(new FileID(s2,s),new FileInputStream(localFile));
            fileStoreImproved.moveToArchive(new FileID(s2,s));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void deleteFile(String s, String s2) {
        try {
            File localFile = getLocalFile(s, s2);
            if (localFile.exists()){
                localFile.delete();
            }
            fileStoreImproved.deleteFile(new FileID(s2,s));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
