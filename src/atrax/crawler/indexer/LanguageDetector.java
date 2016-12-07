package sel.crawler.indexer;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class LanguageDetector {
    public LanguageDetector() throws LangDetectException {
        try {
            DetectorFactory.loadProfile(Paths.get(
                    new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getParent(),
                    "language_profiles").toString());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

//    public LanguageDetector() throws LangDetectException {
//        DetectorFactory.loadProfile("language_profiles");
//    }

    public LanguageDetector(File profileDir) throws LangDetectException {
        DetectorFactory.loadProfile(profileDir);
    }

    public LanguageDetector(String profileDir) throws LangDetectException {
        DetectorFactory.loadProfile(profileDir);
    }

    public String Detect(String text) throws LangDetectException {
        Detector detector = DetectorFactory.create();
        detector.append(text);
        return detector.detect();
    }
}
