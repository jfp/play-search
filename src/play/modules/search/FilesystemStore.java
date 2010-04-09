package play.modules.search;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

import play.Logger;
import play.Play;
import play.classloading.ApplicationClasses.ApplicationClass;
import play.db.jpa.JPA;
import play.db.jpa.JPASupport;
import play.exceptions.UnexpectedException;

public class FilesystemStore implements Store {

    private static Map<String, IndexWriter> indexWriters = new HashMap<String, IndexWriter>();
    private static Map<String, IndexSearcher> indexReaders = new HashMap<String, IndexSearcher>();
    public static String DATA_PATH;
    public static boolean sync = true;

    public void unIndex(Object object) {
        try {
            if (!(object instanceof JPASupport))
                return;
            if (object.getClass().getAnnotation(Indexed.class) == null)
                return;
            JPASupport jpaSupport = (JPASupport) object;
            String index = object.getClass().getName();
            getIndexWriter(index).deleteDocuments(new Term("_docID", ConvertionUtils.getIdValueFor(jpaSupport) + ""));
            if (sync) {
                getIndexWriter(index).flush();
                dirtyReader(index);
            }
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    public void index(Object object) {
        try {
            if (!(object instanceof JPASupport)) {
                Logger.warn("Unable to index " + object + ", unsupported class type. Only play.db.jpa.Model or  play.db.jpa.JPASupport classes are supported.");
                return;
            }
            JPASupport jpaSupport = (JPASupport) object;
            String index = object.getClass().getName();
            Document document = ConvertionUtils.toDocument(object);
            if (document == null)
                return;
            getIndexWriter(index).deleteDocuments(new Term("_docID", ConvertionUtils.getIdValueFor(jpaSupport) + ""));
            getIndexWriter(index).addDocument(document);
            if (sync) {
                getIndexWriter(index).flush();
                dirtyReader(index);
            }
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }
    
    public IndexSearcher getIndexSearcher(String name) {
        try {
            if (!indexReaders.containsKey(name)) {
                synchronized (this) {
                    File root = new File(DATA_PATH, name);
                    if (root.exists()) {
                        IndexSearcher reader = new IndexSearcher(FSDirectory.getDirectory(root));
                        indexReaders.put(name, reader);
                    } else
                        throw new UnexpectedException("Could not find " + name + " index. Please re-index");
                }
            }
            return indexReaders.get(name);
        } catch (Exception e) {
            throw new UnexpectedException("Cannot open index", e);
        }
    }

    /**
     * Used to synchronize reads after write
     *
     * @param name of the reader to be reopened
     */
    public void dirtyReader(String name) {
        synchronized (this) {
            try {
                if (indexReaders.containsKey(name)) {
                    IndexReader rd = indexReaders.get(name).getIndexReader().reopen();
                    indexReaders.get(name).close();
                    indexReaders.remove(name);
                    indexReaders.put(name, new IndexSearcher(rd));
                }
            } catch (IOException e) {
                throw new UnexpectedException("Can't reopen reader", e);
            }
        }
    }

    private IndexWriter getIndexWriter(String name) {
        try {
            if (!indexWriters.containsKey(name)) {
                synchronized (this) {
                    File root = new File(DATA_PATH, name);
                    if (!root.exists())
                        root.mkdirs();
                    if (new File(root, "write.lock").exists())
                        new File(root, "write.lock").delete();
                    IndexWriter writer = new IndexWriter(FSDirectory.getDirectory(root), true, Search.getAnalyser());
                    indexWriters.put(name, writer);
                }
            }
            return indexWriters.get(name);
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    public void rebuildAllIndexes () throws Exception {
        stop();
        File fl = new File(DATA_PATH);
        FileUtils.deleteDirectory(fl);
        fl.mkdirs();
        List<ApplicationClass> classes = Play.classes.getAnnotatedClasses(Indexed.class);
        for (ApplicationClass applicationClass : classes) {
            List<JPASupport> objects = (List<JPASupport>) JPA.em().createQuery(
                    "select e from " + applicationClass.javaClass.getCanonicalName() + " as e").getResultList();
            for (JPASupport jpaSupport : objects) {
                index(jpaSupport);
            }
        }
        Logger.info("Rebuild index finished");
    }

    public void start() {
        if (Play.configuration.containsKey("play.search.path"))
            DATA_PATH = Play.configuration.getProperty("play.search.path");
        else
            DATA_PATH = Play.applicationPath.getAbsolutePath() + "/data/search/";
        Logger.trace("Search module repository is in " + DATA_PATH);
        sync = Boolean.parseBoolean(Play.configuration.getProperty("play.search.synch", "true"));
        Logger.trace("Write operations sync: " + sync);

    }

    public void stop() throws Exception {
        for (IndexWriter writer : indexWriters.values()) {
            writer.close();
        }
        for (IndexSearcher searcher : indexReaders.values()) {
            searcher.close();
        }
        indexWriters.clear();
        indexReaders.clear();
    }
}
