package sel.aws;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

import java.util.LinkedList;
import java.util.List;

public class SimpleDbUtils {
    static public List<Item> getAll(AmazonSimpleDBClient sdb, String query) {
        List<Item> items = new LinkedList<Item>();

        String nextToken = null;
        do {
            SelectRequest request = new SelectRequest(query);
            request.setNextToken(nextToken);
            SelectResult result = sdb.select(request);
            items.addAll(result.getItems());
            nextToken = result.getNextToken();
        } while(nextToken != null);

        return items;
    }
}
