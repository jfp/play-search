package play.modules.search.store.extractors;

import play.db.jpa.FileAttachment;

public interface TextExtractor {
    public boolean handles (String mime);
    public String extract (FileAttachment file);
}
