package sel.crawler.indexer;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.cybozu.labs.langdetect.LangDetectException;
import org.elasticsearch.common.base.Joiner;
import sel.aws.SimpleDbItem;
import sel.common.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IndexerProcess {
    private final static Logger logger = Logger.getLogger(Indexer.class.getName());
    private static final String STATE_FILE_PATH = "IndexerState.txt";
    private static final int SLEEP_PERIOD = 10000; // 10 Seconds

    private String lastIndexed;

    private final String crawlJobName;
    AmazonSimpleDBClient sdb;
    private final Indexer indexer;



    public IndexerProcess(String crawlJobName, AmazonSimpleDBClient sdb, Indexer indexer)
            throws LangDetectException, IOException, org.json.simple.parser.ParseException {
        this.sdb = sdb;
        this.indexer = indexer;
        this.crawlJobName = crawlJobName;
        this.lastIndexed = lastIndexed();
    }

    public void run() throws InterruptedException, IOException, LangDetectException {
        try {
            while (true) {
                SelectRequest request = new SelectRequest(getOriginalsQueryString());

                do {
                    SelectResult result = sdb.select(request);
                    List<Item> originals = result.getItems();

                    if (originals.size() == 0) {
                        break;
                    }

                    for (Item original : originals) {
                        SimpleDbItem originalItem = new SimpleDbItem(original);

                        try {
                            indexer.index(originalItem);
                        } catch (AtraxIndexerException ex) {
                            String exMessage = Joiner.on("\n").join(Utils.getExceptionMessageChain(ex));

                            logger.log(Level.WARNING, String.format("Couldn't process: %s\n%s", originalItem.getName(), exMessage));
                        }
                        String fetched = originalItem.get("fetched");
                        if (fetched != null) {
                            lastIndexed = fetched;
                        }
                    }
                    saveState(); // This will throw if it can't save the indexer's state

                    // SimpleDB will return the results in 1MB chunks. The nextToken will be null if there are no
                    // more chunks to return. In this case we need to wait for there to be more items to index.
                    request.setNextToken(result.getNextToken());
                } while (request.getNextToken() != null);

                Thread.sleep(SLEEP_PERIOD); // Wait for more documents to index
            }
        } finally {
            saveState();
        }
    }

    private String getOriginalsQueryString() {
        String whereClause = "";

        String lastIndexedLastCrawled = lastIndexed();
        if(lastIndexedLastCrawled != null) {
            whereClause = "and `fetched` > '" + lastIndexedLastCrawled + "'";
        }

        return String.format("select `fetched`, `fingerprint` from `%s.crawled-urls` " +
                "where `original`='self' and `fingerprint` is not null and `fetched` is not null %s order by `fetched` ASC",
                this.crawlJobName, whereClause);
    }

    private String lastIndexed() {
        if(this.lastIndexed == null) {
            try {
                String line = new Scanner(new File(STATE_FILE_PATH)).nextLine();
                this.lastIndexed = line.trim();
            } catch(Exception ex) {
                return null;
            }
        }
        return this.lastIndexed;
    }

    private void saveState() throws IOException {
        if(this.lastIndexed != null) {
            Files.write(Paths.get(STATE_FILE_PATH), lastIndexed.getBytes(), StandardOpenOption.CREATE);
        }
    }
}
