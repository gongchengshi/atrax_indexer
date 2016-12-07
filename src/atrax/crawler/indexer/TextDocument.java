package sel.crawler.indexer;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static sel.http.Headers.GetFilenameFromContentDisposition;

class TextDocument {
    public String id = null,
            title = null, body = null,
            mediaType = null, charset = null, analyzer = null;

    public List<String> urls = new LinkedList<String>(), rawUrls = new LinkedList<String>();
    public Set<String> filenames = new HashSet<String>();
    public Collection<String> datesDiscovered = new LinkedList<String>(), datesFetched = new LinkedList<String>(),
            datesCreated = new LinkedList<String>(), datesModified = new LinkedList<String>();

    public String[] authors = null, misc = null;
    public Set<String> outLinks = new HashSet<String>(), emailAddresses = new HashSet<String>();
    public HashSet<String> languages = new HashSet<String>();
    public boolean googleBotBlocked = false, isCommonType = false;
    public int textLength, size = 0;
    public long similarityHashDigest = 0;

    public boolean isValid() {
        return !(id == null || id.isEmpty() || urls.isEmpty() || rawUrls.isEmpty());
    }
}

class TextDocumentWithResolvers extends TextDocument {
    private Parser parser = new Parser();

    public void setId(String itemName) {
        this.id = itemName;
    }

    public void addUrl(String url) { this.urls.add(url); }

    public void addRawUrl(String rawUrl) { this.rawUrls.add(rawUrl); }

    public void addFilename(String contentDisposition, String url) {
        String filename = GetFilenameFromContentDisposition(contentDisposition);
        try {
            if (filename == null) {
                String path = new URL(url).getPath();
                if(path.length() > 1) {
                    filename = path.substring(path.indexOf('/') + 1);
                }
            }

            if(filename != null) {
                this.filenames.add(filename);
            }
        } catch(MalformedURLException ex) {
            // Ignore
        }
    }

    static String UnixTimeStringToIsoDateTimeString(String unixTime){
        return DateFormatUtils.ISO_DATETIME_FORMAT.format((long)(Double.parseDouble(unixTime) * 1000));
    }

    public String addDateFetched(String dateFetched) {
        String dateFetchedStr = UnixTimeStringToIsoDateTimeString(dateFetched);
        this.datesFetched.add(dateFetchedStr);
        return dateFetchedStr;
    }

    private String convertToIsoDatetimeFormat(String date) {
        String isoDatetime = null;

        List<DateGroup> dateGroups = parser.parse(date);
        for(DateGroup group : dateGroups) {
            List<Date> dates = group.getDates();
            if(dates.size() > 0) {
                isoDatetime = DateFormatUtils.ISO_DATETIME_FORMAT.format(dates.get(0));
            }
        }
        return isoDatetime;
    }

    public void addDateDiscovered(String dateDiscovered, String dateFetched) {
        this.datesDiscovered.add(dateDiscovered == null ? dateFetched : UnixTimeStringToIsoDateTimeString(dateDiscovered));
    }

    public void addDateCreated(String created) {
        if(created != null) {
            String isoDatetime = convertToIsoDatetimeFormat(created);
            if(isoDatetime != null) {
                this.datesCreated.add(isoDatetime);
            }
        }
    }

    public void addDateModified(String lastModified) {
        if(lastModified != null) {
            String isoDatetime = convertToIsoDatetimeFormat(lastModified);
            if(isoDatetime != null) {
                this.datesModified.add(isoDatetime);
            }
        }
    }

    public void addLanguage(String language) {
        if(language != null) {
            this.languages.add(language.toLowerCase());
        }
    }

    public void addGoogleBotBlocked(boolean blocked) {
        this.googleBotBlocked |= blocked;
    }

    public void setSize(String contentLength, int calculatedSize, int textSize) {
        try {
            this.size = Integer.parseInt(contentLength);
        }catch (NumberFormatException ex) {
            this.size = Math.max(calculatedSize, textSize);
        }
    }

    public void setMediaType(String mimeType, String tikaMimeType) {
        mediaType = tikaMimeType == null ? mimeType : tikaMimeType;
    }

    public void setBody(String content) {
        body = content.trim();
    }

    public void setCharset(String charset, String tikaCharset) {
        this.charset = tikaCharset == null ? charset : tikaCharset;
    }

    public void setTextLength(int length) {
        this.textLength = length;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setAuthors(String author, String creator, String contributor, String modifier) {
        this.authors = new String[] {author, creator, contributor, modifier};
    }

    public void setMisc(String comments, String description, String source, String keywords) {
        this.misc = new String[] {comments, description, source, keywords};
    }

    public void setIsCommonType(boolean isCommonType) {
        this.isCommonType = isCommonType;
    }

    public void addOutlinks(Collection<String> urls) {
        this.outLinks.addAll(urls);
    }

    public void addEmailAddresses(Collection<String> emailAddresses) {
        this.emailAddresses.addAll(emailAddresses);
    }

    public void setSimilarityHashDigest(long similarityHashDigest) {
        this.similarityHashDigest = similarityHashDigest;
    }

    public void setAnalyzer(String language) {
        this.analyzer = ElasticSearchLanguageAnalyzers.lookup(language);
    }
}
