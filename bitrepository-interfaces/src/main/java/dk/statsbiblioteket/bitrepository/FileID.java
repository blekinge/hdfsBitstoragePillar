package dk.statsbiblioteket.bitrepository;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 4/15/13
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileID {

    private final String fileID;

    private final String collectionID;

    public FileID(String fileID, String collectionID) {
        this.fileID = fileID;
        this.collectionID = collectionID;
    }

    public String getFileID() {
        return fileID;
    }

    public String getCollectionID() {
        return collectionID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileID)) return false;

        FileID fileID1 = (FileID) o;

        if (collectionID != null ? !collectionID.equals(fileID1.collectionID) : fileID1.collectionID != null)
            return false;
        if (fileID != null ? !fileID.equals(fileID1.fileID) : fileID1.fileID != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = fileID != null ? fileID.hashCode() : 0;
        result = 31 * result + (collectionID != null ? collectionID.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FileID{" +
                "fileID='" + fileID + '\'' +
                ", collectionID='" + collectionID + '\'' +
                '}';
    }
}
