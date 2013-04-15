package dk.statsbiblioteket.scape.hadoop.bitrepository;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 9/28/12
 * Time: 4:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileAlreadyExistsException extends IOException {
    public FileAlreadyExistsException() {
    }

    public FileAlreadyExistsException(String message) {
        super(message);
    }

    public FileAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}
