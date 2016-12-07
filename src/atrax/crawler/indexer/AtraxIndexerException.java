package sel.crawler.indexer;

public class AtraxIndexerException extends Exception {
    public AtraxIndexerException(String message, Exception innerException) {
        super(message);
        this.initCause(innerException);
    }

    public AtraxIndexerException(String message) {
        super(message);
    }
}
