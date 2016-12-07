package sel.crawler.indexer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.log4j.BasicConfigurator;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) throws Exception {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.INFO);

        Logger globalLogger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(Level.INFO);

        BasicConfigurator.configure();

        Logger logger = Logger.getLogger(Indexer.class.getName());
        logger.log(Level.INFO, "Starting");

        try {
            AWSCredentials _awsCredentials = new BasicAWSCredentials("",
                    "");
            Region _region = Region.getRegion(Regions.US_WEST_2);

            ClientConfiguration _awsClientConfig = new ClientConfiguration();

            AmazonSimpleDBClient sdb = new AmazonSimpleDBClient(_awsCredentials, _awsClientConfig);
            sdb.setRegion(_region);

            AmazonS3Client s3 = new AmazonS3Client(_awsCredentials, _awsClientConfig);

            ArgumentParser parser = ArgumentParsers.newArgumentParser("Crawler Indexer").defaultHelp(true);
            parser.addArgument("job name");
            Namespace ns = parser.parseArgs(args);
            String jobName = ns.getString("job name");

            Indexer indexer = new Indexer(jobName, "localhost", s3, sdb);

            IndexerProcess indexerProcess = new IndexerProcess(jobName, sdb, indexer);

            logger.log(Level.INFO, "Started");
            indexerProcess.run();

        } catch(Exception ex) {
            logger.log(Level.INFO, ex.getMessage());
            throw ex;
        } finally {
            logger.log(Level.INFO, "Stopped");
            // Todo: Emit SNS message
        }
    }
}
