package sel.crawler.indexer;

import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypes;
import sel.common.SimpleListFile;

import java.util.SortedSet;
import java.util.TreeSet;

public class IndexableMediaTypes {
    private SortedSet<String> indexableMediaTypes = new TreeSet<String>();
    private SortedSet<String> indexableMimePrimaryTypes = new TreeSet<String>();
    private SortedSet<String> nonIndexableMediaTypes = new TreeSet<String>();
    private SortedSet<String> nonIndexableMimePrimaryTypes = new TreeSet<String>();
    private SortedSet<String> commonMediaTypes;

    public IndexableMediaTypes() throws SimpleListFile.ReadFailed {
        readIndexableMediaTypes(new SimpleListFile(getClass().getResourceAsStream("IndexableMediaTypes.txt")));
    }

    public IndexableMediaTypes(SimpleListFile config) throws SimpleListFile.ReadFailed {
        readIndexableMediaTypes(config);
    }

    private void readIndexableMediaTypes(SimpleListFile config) throws SimpleListFile.ReadFailed {

        for(String mediaType : config.get("NonIndexableMediaTypes")) {
            if(mediaType.endsWith("/*")) {
                nonIndexableMimePrimaryTypes.add(mediaType.substring(0, mediaType.length() - 2));
            } else {
                nonIndexableMediaTypes.add(mediaType);
            }
        }

        for(String mediaType : config.get("IndexableMediaTypes")) {
            if(mediaType.endsWith("/*")) {
                indexableMimePrimaryTypes.add(mediaType.substring(0, mediaType.length() - 2));
            } else {
                indexableMediaTypes.add(mediaType);
            }
        }

        for(MediaType mediaType: MimeTypes.getDefaultMimeTypes().getMediaTypeRegistry().getTypes()) {
            if(!nonIndexableMimePrimaryTypes.contains(mediaType.getType()) &&
                    !nonIndexableMediaTypes.contains(mediaType.toString()) &&
                    !indexableMimePrimaryTypes.contains(mediaType.getType())) {
                indexableMediaTypes.add(mediaType.toString());
            }
        }

        commonMediaTypes = new TreeSet<String>(config.get("CommonMediaTypes"));
    }

    public boolean isIndexable(String mediaType) throws UnknownMediaTypeException {
        if(mediaType == null) {
            return false;
        }
        mediaType = mediaType.toLowerCase();
        String type = mediaType.contains("/") ? mediaType.substring(0, mediaType.indexOf('/')) : mediaType;
        if(nonIndexableMimePrimaryTypes.contains(type)) {
            return false;
        }
        if(nonIndexableMediaTypes.contains(mediaType)) {
            return false;
        }
        if(indexableMimePrimaryTypes.contains(type)) {
            return true;
        }
        if(indexableMediaTypes.contains(mediaType)) {
            return true;
        }

        throw new UnknownMediaTypeException(mediaType);
    }

    public boolean isCommonType(String mediaType) {
        return commonMediaTypes.contains(mediaType);
    }
}
