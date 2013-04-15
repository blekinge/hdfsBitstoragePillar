package dk.statsbiblioteket.scape.hadoop.bitrepository;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/15/13
 * Time: 3:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileStatus {

    private final FileID fileID;
    private final long fileSize;
    private final boolean archived;
    private final long lastWritten;

    public FileStatus(FileID fileID, long fileSize, boolean archived, long lastWritten) {
        this.fileID = fileID;
        this.fileSize = fileSize;
        this.archived = archived;
        this.lastWritten = lastWritten;
    }

    public FileID getFileID() {
        return fileID;
    }

    public long getFileSize() {
        return fileSize;
    }

    public boolean isArchived() {
        return archived;
    }

    public long getLastWritten() {
        return lastWritten;
    }
}
