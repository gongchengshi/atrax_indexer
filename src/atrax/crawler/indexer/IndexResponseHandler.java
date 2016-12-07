package sel.crawler.indexer;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexResponse;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

class IndexResponseFutureInfo {
    public final String id;
    public final ListenableActionFuture<IndexResponse> indexResponseFuture;

    public IndexResponseFutureInfo(String id, ListenableActionFuture<IndexResponse> IndexResponseFuture) {

        this.id = id;
        indexResponseFuture = IndexResponseFuture;
    }
}

class IndexResponseHandler implements Runnable {
    private LinkedBlockingQueue<IndexResponseFutureInfo> queue =
            new LinkedBlockingQueue<IndexResponseFutureInfo>();
    private Logger logger;

    private IndexResponseHandler(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void run() {
        IndexResponseFutureInfo item = null;
        boolean stopRequested = false;

        while(true) {
            try {
                item = queue.poll(2, TimeUnit.SECONDS);
                if(item == null) {
                    if(stopRequested) {
                        break; // Only exit thread once all responses have been handled.
                    }
                    continue;
                }
                item.indexResponseFuture.get();
            } catch (InterruptedException e) {
                stopRequested = true;
            } catch (Exception ex) {
                logger.log(Level.WARNING, (item != null ? "Failed to index: " + item.id + "\n" : "") + ex.getMessage());
            }
        }
    }

    public void addIndexResponseFuture(IndexResponseFutureInfo indexResponseFutureInfo) {
        queue.add(indexResponseFutureInfo);
    }

    private static Thread thread;

    public static IndexResponseHandler start(Logger logger) {
        IndexResponseHandler indexResponseHandler = new IndexResponseHandler(logger);
        thread = new Thread(indexResponseHandler);
        thread.start();
        return indexResponseHandler;
    }

    public static void stop() throws InterruptedException {
        thread.interrupt();
        thread.join();
    }
}
