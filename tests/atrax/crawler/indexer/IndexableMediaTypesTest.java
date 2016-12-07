package sel.crawler.indexer;

import junit.framework.Assert;
import org.testng.annotations.Test;
import sel.common.SimpleListFile;

import java.net.URISyntaxException;

public class IndexableMediaTypesTest {
    IndexableMediaTypes target;

    public IndexableMediaTypesTest() throws SimpleListFile.ReadFailed, URISyntaxException {
        target = new IndexableMediaTypes(new SimpleListFile(getClass().getResourceAsStream("IndexableMediaTypesTest/IndexableMediaTypes.txt")));
    }

    @Test
    public void testIsIndexable() throws Exception {
        Assert.assertTrue(target.isIndexable("message/dummy"));
        Assert.assertTrue(target.isIndexable("pdf"));
        Assert.assertTrue(target.isIndexable("text/plain"));
        Assert.assertTrue(target.isIndexable("application/vnd.ms-powerpoint.presentation.macroenabled.12"));

        Assert.assertFalse(target.isIndexable("audio/dummy"));
        Assert.assertFalse(target.isIndexable("application/vnd.adobe.air-application-installer-package+zip"));

        try {
            Assert.assertFalse(target.isIndexable("dummy/dummy"));
        } catch (UnknownMediaTypeException ex) {
            Assert.assertEquals("dummy/dummy", ex.getMessage());
        }
    }

    @Test
    public void testIsCommonType() throws Exception {
        Assert.assertTrue(target.isCommonType("application/vnd.ms-excel"));
        Assert.assertFalse(target.isCommonType("application/vnd.ms-excel2"));
    }
}

