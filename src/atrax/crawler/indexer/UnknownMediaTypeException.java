package sel.crawler.indexer;

public class UnknownMediaTypeException extends Exception {
    public final String MediaType;
    public UnknownMediaTypeException(String mediaType) {
        super(mediaType);
        MediaType = mediaType;
    }
}
