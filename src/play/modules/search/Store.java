package play.modules.search;

import org.apache.lucene.search.IndexSearcher;

/**
 * Manages a search backed (indexes, searchers, readers...)
 * @author jfp
 */
public interface Store {
    public void start () throws Exception;
    public void stop () throws Exception;
    public void unIndex(Object object);
    public void index(Object object);
    public void rebuildAllIndexes() throws Exception;
    public IndexSearcher getIndexSearcher (String searcherName);
}
