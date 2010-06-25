package play.modules.search.store;

import java.util.Collection;

import javax.persistence.Id;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import play.Logger;
import play.data.binding.Binder;
import play.db.jpa.JPASupport;
import play.db.jpa.Model;
import play.exceptions.UnexpectedException;
import play.modules.search.Indexed;

/**
 * Various utils handling object to index
 * and query result to object conversion
 * @author jfp
 *
 */
public class ConvertionUtils {
    /**
     * Examines a JPASupport object and 
     * creates the corresponding Lucene
     * Document
     * @param object to examine, expected a JPASupport object
     * @return the corresponding Lucene document
     * @throws Exception
     */
    public static Document toDocument(Object object) throws Exception {
        Indexed indexed = object.getClass().getAnnotation(Indexed.class);
        if (indexed == null)
            return null;
        if (!(object instanceof JPASupport))
            return null;
        JPASupport jpaSupport = (JPASupport) object;
        Document document = new Document();
        document.add(new Field("_docID", getIdValueFor(jpaSupport) + "", Field.Store.YES, Field.Index.UN_TOKENIZED));
        StringBuffer allValue = new StringBuffer ();
        for (java.lang.reflect.Field field : object.getClass().getFields()) {
            play.modules.search.Field index = field.getAnnotation(play.modules.search.Field.class);
            if (index == null)
                continue;
            if (field.getType().isArray())
                continue;
            if (field.getType().isAssignableFrom(Collection.class))
                continue;

            String name = field.getName();
            String value = null;

            if (field.getType().isAssignableFrom(JPASupport.class) && !index.joinField().isEmpty()) {
                JPASupport joinObject = (JPASupport) field.get(object);
                for (java.lang.reflect.Field joinField : joinObject.getClass().getFields()) {
                    if (joinField.getName().equals(index.joinField())) {
                        name = joinField.getName();
                        value = valueOf(joinObject, joinField);
                    }
                }
            }
            else {
                value = valueOf(object, field);
            }

            if (value == null)
                continue;

            document.add(new Field(name, value, index.stored() ? Field.Store.YES : Field.Store.NO, index.tokenize() ? Field.Index.TOKENIZED
                    : Field.Index.UN_TOKENIZED));
            if(index.tokenize() && index.sortable() ) {
                document.add(new Field(name + "_untokenized", value, index.stored() ? Field.Store.YES : Field.Store.NO, Field.Index.UN_TOKENIZED));
            }
            allValue.append(value).append(' ');
        }
        document.add(new Field("allfield", allValue.toString(), Field.Store.NO, Field.Index.TOKENIZED));
        return document;
    }

    public static String valueOf(Object object, java.lang.reflect.Field field) throws Exception {
        if (field.getType().equals(String.class))
            return (String) field.get(object);
        return "" + field.get(object);
    }

    /**
     * Looks for the type of the id fiels on the JPASupport target class
     * and use play's binder to retrieve the corresponding object
     * used to build JPA load query
     * @param clazz JPASupport target class
     * @param indexValue String value of the id, taken from index
     * @return Object id expected to build query
     */
    public static Object getIdValueFromIndex (Class clazz, String indexValue) {
        java.lang.reflect.Field field = getIdField(clazz);
        Class parameter = field.getType();
        try {
            return Binder.directBind(indexValue, parameter);
        } catch (Exception e) {
            throw new UnexpectedException("Could not convert the ID from index to corresponding type",e);
        }
    }

    /**
     * Find a ID field on the JPASupport target class
     * @param clazz JPASupport target class
     * @return corresponding field
     */
    public static java.lang.reflect.Field getIdField (Class clazz) {
        for (java.lang.reflect.Field field : clazz.getFields()) {
            if (field.getAnnotation(Id.class) != null) {
                return field;
            }
        }
        throw new RuntimeException("Your class " + clazz.getName() + " is annotated with javax.persistence.Id but the field Id was not found");
    }

    /**
     * Lookups the id field, being a Long id for Model and an annotated field @Id for JPASupport
     * and returns the field value.
     *
     * @param jpaSupport is a Play! Framework that supports JPA
     * @return the field value (a Long or a String for UUID)
     */
    public static Object getIdValueFor(JPASupport jpaSupport) {
        if (jpaSupport instanceof Model) {
            return ((Model) jpaSupport).id;
        }

        java.lang.reflect.Field field = getIdField(jpaSupport.getClass());
        Object val = null;
        try {
            val = field.get(jpaSupport);
        } catch (IllegalAccessException e) {
            Logger.error("Unable to read the field value of a field annotated with @Id " + field.getName() + " due to " + e.getMessage(), e);
        }
        return val;
    }

    public static boolean isForcedUntokenized(Class clazz, String fieldName) {
        try {
            java.lang.reflect.Field field = clazz.getField(fieldName);
            play.modules.search.Field index = field.getAnnotation(play.modules.search.Field.class);
            return index.tokenize() && index.sortable();
        } catch (Exception e) {
            Logger.error("%s", e.getCause());
        }
        return false;
    }
}
