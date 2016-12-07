package sel.crawler.indexer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;
import com.cybozu.labs.langdetect.LangDetectException;
import org.elasticsearch.common.base.Joiner;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;
import sel.aws.SimpleDbItem;
import sel.common.SimpleListFile;
import sel.common.Utils;
import sel.http.Headers;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;

public class IndexerTest {
    private String jobName;
    private Indexer target;
    private AmazonSimpleDBClient sdb;

    public IndexerTest() throws SimpleListFile.ReadFailed, LangDetectException {
        AWSCredentials _awsCredentials =
                new BasicAWSCredentials("", "");
        Region _region = Region.getRegion(Regions.US_WEST_2);

        ClientConfiguration _awsClientConfig = new ClientConfiguration();

        sdb = new AmazonSimpleDBClient(_awsCredentials, _awsClientConfig);
        sdb.setRegion(_region);

        AmazonS3Client s3 = new AmazonS3Client(_awsCredentials, _awsClientConfig);

        jobName = "";
        target = new Indexer(jobName, "10.16.45.170", s3, sdb);

//        String jobName = "siemens17042013";
//        target = new IndexerAccessWrapper(jobName, "localhost:9600", s3, sdb);
    }

    @Test
    public void TestIndex() throws LangDetectException, SimpleListFile.ReadFailed, FileNotFoundException {
//        String urlId = "www.siemens.unisvet.ru/production/boxes.html";
//        String urlId = "www.siemens.com/future-of-energy/publications.html";
        String urlId = "www.siemens.com/future-of-energy/products-and-solutions.html";
        SimpleDbItem originalItem = new SimpleDbItem(urlId, sdb.getAttributes(new GetAttributesRequest(jobName + ".crawled-urls", urlId)));

        try {
            target.index(originalItem);
        } catch (AtraxIndexerException ex) {
            String exMessage = Joiner.on("\n").join(Utils.getExceptionMessageChain(ex));
            System.out.println(String.format("Couldn't process: %s\n%s", originalItem.getName(), exMessage));
        }
    }

    private String getQueryString(String id) {
        String whereClause = String.format("`redirectsTo` is null and itemName()='%s'", id);
        String orderClause = "and `lastCrawled` is not null order by `lastCrawled` ASC";
        String fields = "`url`, `lastCrawled`, `dateDiscovered`, `last-modified`, `content-length`, " +
                "`content-type`, `content-language`, `content-disposition`, `googleBotExclusionReasons`";

        return String.format("select %s from `%s` where %s %s limit 1", fields, "crawled-urls.siemens17042013", whereClause, orderClause);
    }

    private Item Query(String id) {
        String query = getQueryString(id);
        SelectRequest request = new SelectRequest(query);
        SelectResult result = sdb.select(request);

        List<Item> items = result.getItems();

        for(Item item : items) {
            return item;
        }
        return null;
    }

    @Test
    public void testResolveFields() throws Exception {
//        String name = "www.industry.siemens.com/topics/global/de/gusstechnik/Documents/Allgemeine_Lieferbedingungen_fuer_Erzeugnisse_u_Leistungen_der_Elektroindustrie.doc";
//        String name = "www.water.siemens.com/SiteCollectionDocuments/Product_Lines/Industrial_Process_Water/Brochures/SP-113_PO-CP-BR-0611_SR.pdf";
        String name = "w3.usa.siemens.com/powerdistribution/us/en/roadshow/Documents/Road%20Show%20Schedule%202013.xls";
//        String name = "w3.usa.siemens.com/powerdistribution/us/en/roadshow/Documents/Road Show Schedule 2013.xls";
//        String name = "www.automation.siemens.com/w1/automation-technology-kontinuierliche-verwiegung-19554.htm";
//        String name = "www.siemens.de/fidamat";

        Item item = Query(name);
        HashMap<String, String> attributes = new HashMap<String, String>(10);
        for(Attribute attribute : item.getAttributes()) {
            attributes.put(attribute.getName(), attribute.getValue());
        }
        Headers.ContentType crawlerContentType = new Headers.ContentType(attributes.get("content-type"));
        TextDocument actual = new TextDocument();
//        target.createTextDocument(actual,  crawlerContentType);
    }

    @Test
    public void testIndex() throws Exception {

    }
}

class IndexerAccessWrapper extends Indexer {
    public IndexerAccessWrapper(String jobName, String host, AmazonS3Client s3, AmazonSimpleDBClient sdb)
            throws LangDetectException, SimpleListFile.ReadFailed {
        super(jobName, host, s3, sdb);
    }

    protected TextDocument createTextDocument(
            String id, byte[] bytes, List<SimpleDbItem> items,
            Headers.ContentType crawlerContentType)
            throws FileNotFoundException, LangDetectException, AtraxIndexerException, ParseException {
        return super.createTextDocument(id, bytes, items, crawlerContentType);
    }
}

