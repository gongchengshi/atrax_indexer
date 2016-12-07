package sel.crawler.indexer;

import java.util.HashMap;
import java.util.Map;

public class ElasticSearchLanguageAnalyzers {
    private static final Map<String, String> languageAnalyzers;

    static {
        languageAnalyzers = new HashMap<String, String>();
        languageAnalyzers.put("ar", "arabic");
        languageAnalyzers.put("hy", "armenian");
        languageAnalyzers.put("eu", "basque");
        languageAnalyzers.put("pt-br", "brazilian");
        languageAnalyzers.put("bg", "bulgarian");
        languageAnalyzers.put("ca", "catalan");
        languageAnalyzers.put("zh", "chinese");
        languageAnalyzers.put("ja", "cjk");
        languageAnalyzers.put("ko", "cjk");
        languageAnalyzers.put("cs", "czech");
        languageAnalyzers.put("da", "danish");
        languageAnalyzers.put("nl", "dutch");
        languageAnalyzers.put("en", "english");
        languageAnalyzers.put("fi", "finnish");
        languageAnalyzers.put("fr", "french");
        languageAnalyzers.put("gl", "galician");
        languageAnalyzers.put("de", "german");
        languageAnalyzers.put("el", "greek");
        languageAnalyzers.put("hi", "hindi");
        languageAnalyzers.put("hu", "hungarian");
        languageAnalyzers.put("id", "indonesian");
        languageAnalyzers.put("it", "italian");
        languageAnalyzers.put("no", "norwegian");
        languageAnalyzers.put("fa", "persian");
        languageAnalyzers.put("pt", "portuguese");
        languageAnalyzers.put("ro", "romanian");
        languageAnalyzers.put("ru", "russian");
        languageAnalyzers.put("es", "spanish");
        languageAnalyzers.put("sv", "swedish");
        languageAnalyzers.put("tr", "turkish");
        languageAnalyzers.put("th", "thai");
    }

    public static String lookup(String language) {
        String analyzer = null;

        if(language != null && language.length() >= 2) {
            analyzer = languageAnalyzers.get(language.substring(0, 2));
            if(analyzer != null && analyzer.equals("pt") && language.startsWith("pt-br")) {
                analyzer = languageAnalyzers.get("pt-br");
            }
        }

        if(analyzer == null) {
            analyzer = "english";
        }

        return analyzer;
    }
}
