package play.modules.search.store.mime;

import play.db.jpa.FileAttachment;

public interface MimeGuesser {
    public String guess (FileAttachment file);
}
