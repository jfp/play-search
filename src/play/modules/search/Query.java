package play.modules.search;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Sort;

import play.Play;
import play.db.jpa.JPA;
import play.db.jpa.JPABase;
import play.exceptions.UnexpectedException;
import play.modules.search.store.ConvertionUtils;
import play.modules.search.store.Store;

/**
 * Query API. This is a chainable API you get from Search.search () methods
 * 
 * @author jfp
 */
public class Query {
    private Class<JPABase> clazz;

    private String query;

    private Store store;

    private String[] order = new String[0];

    private int offset = 0;

    private int pageSize = 10;

    private boolean reverse = false;

    private Hits hits = null;

    protected Query(String query, Class<JPABase> clazz, Store store) {
        this.query = query;
        this.clazz = clazz;
        this.store = store;
    }

    public Query page(int offset, int pageSize) {
        this.offset = offset;
        this.pageSize = pageSize;
        return this;
    }

    public Query all() {
        pageSize = -1;
        return this;
    }

    public Query reverse() {
        this.reverse = true;
        return this;
    }

    public Query orderBy(String... order) {
        this.order = new String[order.length];
        for (int i = 0; i < order.length; i++) {
            this.order[i] = order[i] + (ConvertionUtils.isForcedUntokenized(clazz, order[i]) ? "_untokenized" : "");
        }
        return this;
    }

    private Sort getSort() {
        Sort sort = new Sort();
        if (order.length > 0) {
            if (reverse) {
                if (order.length != 1)
                    throw new SearchException("reverse can be used while sorting only one field with oderBy");
                else
                    sort.setSort(order[0], reverse);
            } else
                sort.setSort(order);
        }
        return sort;
    }

    /**
     * Executes the query and return directly JPABase objects (No score
     * information)
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T extends JPABase> List<T> fetch() throws SearchException {
        try {
            List<QueryResult> results = executeQuery(true);
            List<JPABase> objects = new ArrayList<JPABase>();
            for (QueryResult queryResult : results) {
                objects.add(queryResult.object);
            }
            return (List<T>) objects;
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    public List<Long> fetchIds() throws SearchException {
        try {
            List<QueryResult> results = executeQuery(false);
            List<Long> objects = new ArrayList<Long>();
            for (QueryResult queryResult : results) {
                objects.add(Long.parseLong(queryResult.id));
            }
            return objects;
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    public long count() throws SearchException {
        try {
            org.apache.lucene.search.Query luceneQuery = new QueryParser("_docID", Search.getAnalyser()).parse(query);
            hits = store.getIndexSearcher(clazz.getName()).search(luceneQuery, getSort());
            return hits.length();
        } catch (ParseException e) {
            throw new SearchException(e);
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    /**
     * Executes the lucene query against the index. You get QueryResults.
     * 
     * @param fetch load the corresponding JPABase objects in the QueryResult
     *            Object
     * @return
     */
    public List<QueryResult> executeQuery(boolean fetch) throws SearchException {
        try {
            if (hits == null) {
                org.apache.lucene.search.Query luceneQuery =
                                new QueryParser("_docID", Search.getAnalyser()).parse(query);
                BooleanQuery.setMaxClauseCount(Integer.parseInt(Play.configuration.getProperty(
                                "play.search.maxClauseCount", "1024")));
                hits = store.getIndexSearcher(clazz.getName()).search(luceneQuery, getSort());
            }
            List<QueryResult> results = new ArrayList<QueryResult>();
            if (hits == null)
                return results;

            int l = hits.length();
            if (offset > l) {
                return results;
            }
            List<Long> ids = new ArrayList<Long>();
            if (pageSize > 0) {
                for (int i = offset; i < (offset + pageSize > l ? l : offset + pageSize); i++) {
                    QueryResult qresult = new QueryResult();
                    qresult.score = hits.score(i);
                    qresult.id = hits.doc(i).get("_docID");
                    if (fetch) {
                        Object objectId = ConvertionUtils.getIdValueFromIndex(clazz, qresult.id);
                        qresult.object = (JPABase)JPA.em().find(clazz, objectId);
                        if (qresult.object == null)
                            throw new SearchException("Please re-index");
                    }
                    results.add(qresult);
                }
            } else {
                for (int i = 0; i < l; i++) {
                    QueryResult qresult = new QueryResult();
                    qresult.score = hits.score(i);
                    qresult.id = hits.doc(i).get("_docID");
                    if (fetch) {
                        Object objectId = ConvertionUtils.getIdValueFromIndex(clazz, qresult.id);
                        qresult.object = (JPABase)JPA.em().find(clazz, objectId);
                        if (qresult.object == null)
                            throw new SearchException("Please re-index");
                    }
                    results.add(qresult);
                }
            }
            return results;
        } catch (ParseException e) {
            throw new SearchException(e);
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    public static class QueryResult {
        public String id;

        public float score;

        public JPABase object;
    }

    public static class SearchException extends RuntimeException {
        public SearchException(String message, Throwable cause) {
            super(message, cause);
        }

        public SearchException(Throwable cause) {
            super(cause);
        }

        public SearchException(String message) {
            super(message);
        }
    }
}
