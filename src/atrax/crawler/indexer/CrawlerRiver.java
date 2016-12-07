package sel.crawler.indexer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.cybozu.labs.langdetect.LangDetectException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.json.simple.parser.ParseException;
import sel.common.SimpleListFile;

import java.io.IOException;

public class CrawlerRiver extends AbstractRiverComponent implements River {
    private final IndexerProcess _indexerProcess;
    protected CrawlerRiver(RiverName riverName, RiverSettings settings) throws LangDetectException, IOException, ParseException, SimpleListFile.ReadFailed, InterruptedException {
        super(riverName, settings);

        String jobName = (String)settings.settings().get("job_name");

        AWSCredentials _awsCredentials = new BasicAWSCredentials(
                "", "");
        Region _region = Region.getRegion(Regions.US_WEST_2);

        ClientConfiguration _awsClientConfig = new ClientConfiguration();

        AmazonSimpleDBClient _sdb = new AmazonSimpleDBClient(_awsCredentials, _awsClientConfig);
        _sdb.setRegion(_region);

        AmazonS3Client _s3 = new AmazonS3Client(_awsCredentials, _awsClientConfig);
        _s3.setRegion(_region);

        Indexer indexer = new Indexer(jobName, "localhost:9600", _s3, _sdb);

        _indexerProcess = new IndexerProcess(jobName, _sdb, indexer);
    }

    @Override
    public void start() {
        try {
            _indexerProcess.run();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void close() {

    }
}
