package play.modules.search.store;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class DocumentBuilder {
    protected Document document = new Document();

    protected StringBuffer allValue = new StringBuffer();

    public DocumentBuilder(String docId) {
        document.add(new Field("_docID", docId, Field.Store.YES, Field.Index.UN_TOKENIZED));
    }

    public void addField(String name, String value, play.modules.search.Field index) {
        if (value == null) {
            return;
        }

        document.add(new Field(name, value, index.stored() ? Field.Store.YES : Field.Store.NO,
                        index.tokenize() ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED));

        if (index.tokenize() && index.sortable()) {
            document.add(new Field(name + "_untokenized", value, index.stored() ? Field.Store.YES : Field.Store.NO,
                            Field.Index.UN_TOKENIZED));
        }

        allValue.append(value).append(' ');
    }

    public Document toDocument() {
        document.add(new Field("allfield", allValue.toString(), Field.Store.NO, Field.Index.TOKENIZED));
        return document;
    }
}
