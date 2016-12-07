package sel.crawler.indexer;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Item;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import sel.aws.SimpleDbItem;
import sel.common.SimHash;
import sel.common.SimpleListFile;
import sel.common.TextExtraction;
import sel.http.Headers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class Indexer {
    private final Client esClient;
    private final String bucketName;
    private final IndexResponseHandler indexResponseHandler;
    private Tika tika = new Tika();
    JSONParser jsonParser = new JSONParser();

    private final LanguageDetector languageDetector;
    private final String crawlJobName;
    private final IndexableMediaTypes indexableMediaTypes;
    private final AmazonS3Client s3;
    private final AmazonSimpleDBClient sdb;
    private final static Logger logger = Logger.getLogger(Indexer.class.getName());

    public Indexer(String crawlJobName, String host, AmazonS3Client s3, AmazonSimpleDBClient sdb)
            throws SimpleListFile.ReadFailed, LangDetectException {
        this(crawlJobName, host, s3, sdb, new LanguageDetector(), new IndexableMediaTypes());
    }

    public Indexer(String crawlJobName, String host, AmazonS3Client s3,
                   AmazonSimpleDBClient sdb, LanguageDetector languageDetector,
                   IndexableMediaTypes indexableMediaTypes)
            throws LangDetectException, SimpleListFile.ReadFailed {
        this.crawlJobName = crawlJobName;
        this.s3 = s3;
        this.sdb = sdb;
        this.languageDetector = languageDetector;
        this.indexableMediaTypes = indexableMediaTypes;

//        if(host.equals("localhost")) {
//            Node node = nodeBuilder().local(true).node();
//            this.esClient = node.client();
//        } else {
            Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "atrax_indexer").build();
            this.esClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, 9300));
//        }

        this.bucketName = crawlJobName + ".content";

        // Allow files up to half the size of the maximum amount of memory available to the JVM.
        Runtime runtime = Runtime.getRuntime();
        tika.setMaxStringLength((int) (runtime.maxMemory() / 2));
        this.indexResponseHandler = IndexResponseHandler.start(logger);
    }

    public void index(SimpleDbItem item) throws FileNotFoundException, LangDetectException, AtraxIndexerException {
        Headers.ContentType probableContentType = null;

        String fingerprint = item.get("fingerprint");
        List<Item> rawItems = sel.aws.SimpleDbUtils.getAll(this.sdb, getDuplicatesQueryString(fingerprint));
        boolean indexableBody = false;
        List<SimpleDbItem> allItems = new ArrayList<SimpleDbItem>(rawItems.size());

        for(Item rawItem : rawItems) {
            SimpleDbItem itemInstance = new SimpleDbItem(rawItem);
            allItems.add(itemInstance);

            Headers.ContentType crawlerContentType = new Headers.ContentType(itemInstance.get("content-type"));
            if(crawlerContentType.MediaType != null) {
                probableContentType = crawlerContentType;
            }

            try {
                if (indexableMediaTypes.isIndexable(crawlerContentType.MediaType)) {
                    indexableBody = true;
                    break;
                }
            } catch (UnknownMediaTypeException ex) {
                logger.log(Level.WARNING, String.format("Unknown Media Type: %s %s",
                        ex.MediaType, itemInstance.getName()));
            }
        }

        if(probableContentType == null) {
            probableContentType = new Headers.ContentType("");
        }

        try {
            TextDocument textDocument;
            if (indexableBody) {
                String s3Key = StringUtils.strip(item.getName(), "?/");
                S3Object object = s3.getObject(new GetObjectRequest(bucketName, s3Key));
                S3ObjectInputStream s3InputStream = object.getObjectContent();
                GZIPInputStream inputStream = new GZIPInputStream(s3InputStream);
                byte[] bytes = IOUtils.toByteArray(inputStream);
                textDocument = createTextDocument(fingerprint, bytes, allItems, probableContentType);
            } else {
                // Technically this isn't a text document but I'll fix this later.
                textDocument = createBodylessTextDocument(fingerprint, allItems, probableContentType);
            }

            addToIndex(textDocument);
        } catch(AmazonS3Exception ex) {
            if(ex.getErrorCode().equals("NoSuchKey")) {
                logger.log(Level.WARNING, String.format("S3 key does not exist: %s", item.getName()));
            }
        } catch(IOException ex) {
            throw new AtraxIndexerException("Failed to read from S3", ex);
        } catch(ParseException ex) {
            throw new AtraxIndexerException("Failed to parse attributes", ex);
        }
    }

    private String getDuplicatesQueryString(String fingerprint) {
        return String.format("select `url`, `raw_url`, `fetched`, `discovered`, `last-modified`, `content-length`, " +
                "`content-type`, `content-language`, `content-disposition`, `googlebot_exclusion_reasons` " +
                "from `%s.crawled-urls` where `fingerprint`='%s'", this.crawlJobName, fingerprint);
    }

    private TextDocumentWithResolvers createBodylessTextDocument(String id, List<SimpleDbItem> items,
                                                      Headers.ContentType crawlerContentType) throws ParseException {
        TextDocumentWithResolvers document = new TextDocumentWithResolvers();

        document.setId(id);

        for(SimpleDbItem item : items) {
            String url = item.get("url");
            document.addUrl(item.get("url"));
            document.addRawUrl(item.get("raw_url"));
            document.addFilename(item.get("content-disposition"), url);

            String dateFetched = document.addDateFetched(item.get("fetched"));
            document.addDateDiscovered(item.get("discovered"), dateFetched);
            document.addDateModified(item.get("last-modified"));
            String exclusionReasons = item.get("googlebot_exclusion_reasons");
            JSONArray array = (JSONArray)(jsonParser.parse(exclusionReasons));
            document.addGoogleBotBlocked(array.size() > 0);
        }

        SimpleDbItem exemplar = items.get(0);
        document.setSize(exemplar.get("content-length"), 0, 0);
        document.addLanguage(exemplar.get("content-language"));

        if (crawlerContentType.MediaType != null) {
            document.mediaType = crawlerContentType.MediaType;
        }

        if (crawlerContentType.Charset != null) {
            document.charset = crawlerContentType.Charset;
        }

        return document;
    }

    protected TextDocument createTextDocument(String id, byte[] bytes, List<SimpleDbItem> items,
                                      Headers.ContentType crawlerContentType)
            throws FileNotFoundException, LangDetectException, AtraxIndexerException, ParseException {

        TextDocumentWithResolvers document = createBodylessTextDocument(id, items, crawlerContentType);

        Metadata tikaMetadata = new Metadata();

        String filename = null;
        for(String f : document.filenames) {
            if(f.contains(".")) {
                filename = f;
                break;
            }
        }

        if(filename != null) {
            tikaMetadata.set(Metadata.RESOURCE_NAME_KEY, filename); // This is only a suggestion to Tika.
        }

        tikaMetadata.set(Metadata.CONTENT_TYPE, crawlerContentType.MediaType); // This is only a suggestion to Tika.
        String content;

        try {
            TikaInputStream reader = TikaInputStream.get(bytes);
            document.setBody(tika.parseToString(reader, tikaMetadata));

            Headers.ContentType tikaContentType = new Headers.ContentType(tikaMetadata.get(Metadata.CONTENT_TYPE));

            document.setMediaType(crawlerContentType.MediaType, tikaContentType.MediaType);
            document.setCharset(crawlerContentType.Charset, tikaContentType.Charset);

            content = new String(bytes, document.charset == null ? "UTF-8" : document.charset);
        } catch (TikaException ex) {
            logger.log(Level.WARNING, ex.getMessage());
            return document;
        } catch (IOException ex) {
            logger.log(Level.WARNING, ex.getMessage());
            throw new AtraxIndexerException("Failed to read bytes", ex);
        }

        if(document.mediaType.equals("text/html") || document.mediaType.equals("application/xhtml+xml")) {
            document.addOutlinks(TextExtraction.ExtractLinksFromHtml(content, document.urls.get(0)));
        }

        document.addEmailAddresses(TextExtraction.ExtractEmailAddresses(content));

        String language = document.languages.size() > 0 ? document.languages.iterator().next() : null;

        if(document.body != null && document.body.length() > 0) {
            document.addOutlinks(TextExtraction.ExtractLinksFromText(document.body));
            document.addEmailAddresses(TextExtraction.ExtractEmailAddresses(document.body));

            document.setTextLength(document.body.length());

            String detectedLanguage = null;
            try {
                detectedLanguage = languageDetector.Detect(document.body);
                document.addLanguage(detectedLanguage);
            } catch(LangDetectException ex) {
                logger.log(Level.WARNING, "No language could be detected.");
            }

            String tikaLanguage = tikaMetadata.get(TikaCoreProperties.LANGUAGE);
            document.addLanguage(tikaLanguage);

            if(detectedLanguage != null) {
                language = detectedLanguage.toLowerCase();
            } else if(tikaLanguage != null) {
                language = tikaLanguage.toLowerCase();
            }

            document.setSimilarityHashDigest(SimHash.ByWord(document.body, language));
        }

        document.size = Math.max(document.size, Math.max(bytes.length, document.textLength));

        document.addDateCreated(tikaMetadata.get(TikaCoreProperties.CREATED));
        document.addDateModified(tikaMetadata.get(TikaCoreProperties.MODIFIED));

        document.setAuthors(tikaMetadata.get("Author"),
                tikaMetadata.get(TikaCoreProperties.CREATOR),
                tikaMetadata.get(TikaCoreProperties.CONTRIBUTOR),
                tikaMetadata.get(TikaCoreProperties.MODIFIER));
        document.setMisc(tikaMetadata.get(TikaCoreProperties.COMMENTS),
                tikaMetadata.get(TikaCoreProperties.DESCRIPTION),
                tikaMetadata.get(TikaCoreProperties.SOURCE),
                tikaMetadata.get(TikaCoreProperties.KEYWORDS));
        document.setTitle(tikaMetadata.get(TikaCoreProperties.TITLE));
        document.setIsCommonType(indexableMediaTypes.isCommonType(document.mediaType));

        document.setAnalyzer(language);

        return document;
    }

    protected void addToIndex(TextDocument textDocument) throws IllegalArgumentException, IOException {
        if(textDocument == null || !textDocument.isValid()) {
            throw new IllegalArgumentException(
                String.format("Could not index document due to null or missing parameters"));
        }

        IndexRequestBuilder indexRequest = esClient.prepareIndex(crawlJobName, "text_document", textDocument.id);

        try {
            XContentBuilder builder = jsonBuilder().startObject().field("url", textDocument.urls);
            if(textDocument.body != null) {
                builder.field("body", textDocument.body);
            }
            builder.field("raw_url", textDocument.rawUrls);
            builder.field("filename", textDocument.filenames);
            builder.field("title", textDocument.title);
            builder.field("media_type", textDocument.mediaType);
            builder.field("charset", textDocument.charset);
            builder.field("date_discovered", textDocument.datesDiscovered);
            builder.field("date_fetched", textDocument.datesFetched);
            builder.field("date_created", textDocument.datesCreated);
            builder.field("date_modified", textDocument.datesModified);
            builder.field("language", textDocument.languages);
            builder.field("analyzer", textDocument.analyzer);
            if(textDocument.authors != null) {
                builder.field("authors", textDocument.authors);
            }
            if(textDocument.misc != null) {
                builder.field("misc", textDocument.misc);
            }
            builder.field("email_addresses", textDocument.emailAddresses);
            builder.field("out_links", textDocument.outLinks);
            builder.field("googlebot_blocked", textDocument.googleBotBlocked);
            builder.field("text_length", textDocument.textLength);
            builder.field("size", textDocument.size);
            builder.field("similarity_hash_digest", textDocument.similarityHashDigest);
            indexResponseHandler.addIndexResponseFuture(
                    new IndexResponseFutureInfo(textDocument.id, indexRequest.setSource(builder).execute()));
        } catch (IOException ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
