package sel.aws;

import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;

import java.util.HashMap;
import java.util.List;

public class SimpleDbItem {
    HashMap<String, String> attributes;
    private String name;

    public SimpleDbItem(Item item) {
        this(item.getName(), item.getAttributes());
    }

    public SimpleDbItem(String name, GetAttributesResult item) {
        this(name, item.getAttributes());
    }

    private SimpleDbItem(String name, List<Attribute> attributes) {
        this.name = name;
        this.attributes = new HashMap<String, String>(attributes.size());

        // There's no way to pull out individual attributes from the item so we need to loop through them and
        // put them in a dictionary for easy lookup later.
        for (Attribute attribute : attributes) {
            this.attributes.put(attribute.getName(), attribute.getValue());
        }
    }

    public String getName() {
        return this.name;
    }

    public String get(String key) {
        return this.attributes.get(key);
    }
}
