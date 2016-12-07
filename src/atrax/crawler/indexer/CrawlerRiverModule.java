package sel.crawler.indexer;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class CrawlerRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(CrawlerRiver.class).asEagerSingleton();
    }
}
