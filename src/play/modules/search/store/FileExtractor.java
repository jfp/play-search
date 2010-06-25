package play.modules.search.store;

import java.util.ArrayList;
import java.util.List;

import play.Logger;
import play.Play;
import play.classloading.ApplicationClasses.ApplicationClass;
import play.db.jpa.FileAttachment;
import play.modules.search.store.extractors.TextExtractor;
import play.modules.search.store.mime.ExtensionGuesser;
import play.modules.search.store.mime.MimeGuesser;

/**
 * This class performs Full text extraction from various
 * file formats
 * @author jfp
 */
public class FileExtractor {
    
    public static List<TextExtractor> extractors = new ArrayList<TextExtractor>();
    public static MimeGuesser mimeGuesser = new ExtensionGuesser();
    static {
        List<ApplicationClass> classes = Play.classes.getAssignableClasses(TextExtractor.class);
        for (ApplicationClass applicationClass : classes) {
            try {
                extractors.add((TextExtractor) applicationClass.javaClass.newInstance());
            } catch (Exception e) {
                Logger.warn(e,"Could not instanciate text extractor %s",applicationClass.javaClass.getName());
            }
        }
    }
    
    public static String getText (FileAttachment file) {
        // Guess mime
        String mime = mimeGuesser.guess (file);
        // Invoke the handlers
        for (TextExtractor extractor : extractors) {
            if (extractor.handles(mime)) {
                Logger.debug ("Using %s extractor to handle file %s, mime=%s",extractor.getClass().getName(),file.filename,mime);
                return extractor.extract(file);
            }
        }
        Logger.warn("No handlers able to index %s mime type, file was %s",mime,file.filename);
        return null;
    }
}
