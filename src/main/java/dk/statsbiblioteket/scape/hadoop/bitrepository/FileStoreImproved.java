package dk.statsbiblioteket.scape.hadoop.bitrepository;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * Interface for the file stores and the reference archive.
 */
public interface FileStoreImproved {
    /**
     * Retrieves the wanted file as a InputStream.
     * @param fileID The id of the wanted file.
     * @return A InputStream to the requested file.
     * @throws java.io.FileNotFoundException if the file is not found in the archive
     * @throws IOException If a problem occurs during the retrieval of the file.
     */
    public InputStream getFileContents(FileID fileID) throws FileNotFoundException,IOException;


    public FileStatus getFileStatus(FileID fileID) throws IOException;


    /**
     * Retrieves id of all the files within the storage.
     * @return The collection of file ids in the storage.
     * @throws IOException If anything unexpected occurs.
     */
    public Collection<FileID> getAllFileIds(String collectionID) throws IOException;

    /**
     * Stores a file given through an InputStream. The file is only intended to be stored in a temporary zone until it
     * has been validated. Then it should be archived through the 'moveToArchive' method.
     * @param fileID The id of the file to store.
     * @param inputStream The InputStream with the content of the file.
     * @throws FileAlreadyExistsException if the filename is already used in stage
     * @throws IOException If anything unexpected occurs (e.g. file already exists, not enough space, etc.)
     * @see #moveToArchive(String)
     */
    public boolean storeInTemporaryStore(FileID fileID, InputStream inputStream) throws FileAlreadyExistsException,IOException;

    /**
     * Moves a file from the temporary file zone to the archive.
     * @param fileID The id of the file to move to archive.
     * @throws java.io.FileNotFoundException if the file is not found in the temporary archive
     * @throws FileAlreadyExistsException if the filename is already permament archive
     * @throws IOException If anything unexpected occurs (not enough space, etc.)
     * @see #storeInTemporaryStore(String, java.io.InputStream)
     */
    public boolean moveToArchive(FileID fileID) throws FileNotFoundException,FileAlreadyExistsException,IOException;

    /**
     * Removes a file from the storage area.
     * @param fileID The id of the file to remove.
     * @throws java.io.FileNotFoundException if the file is not found in the archive
     * @throws IOException If anything unexpected occurs
     */
    public boolean deleteFile(FileID fileID) throws FileNotFoundException,IOException;


}


