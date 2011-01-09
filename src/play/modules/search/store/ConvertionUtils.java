package play.modules.search.store;

import java.util.Arrays;
import java.util.Collection;

import javax.persistence.Id;
import javax.persistence.ManyToOne;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import play.Logger;
import play.data.binding.Binder;
import play.db.jpa.FileAttachment;
import play.db.jpa.JPABase;
import play.db.jpa.Model;
import play.exceptions.UnexpectedException;
import play.modules.search.Indexed;

/**
 * Various utils handling object to index and query result to object conversion
 * 
 * @author jfp
 */
public class ConvertionUtils {
    private static final String JAVA_MEMBER_ACCESSOR = ".";

	/**
     * Examines a JPABase object and creates the corresponding Lucene Document
     * 
     * @param object to examine, expected a JPABase object
     * @return the corresponding Lucene document
     * @throws Exception
     */
    public static Document toDocument(Object object) throws Exception {
        Indexed indexed = object.getClass().getAnnotation(Indexed.class);
        if (indexed == null)
            return null;
        if (!(object instanceof JPABase))
            return null;

        JPABase jpaBase = (JPABase) object;
        DocumentBuilder builder = new DocumentBuilder(getIdValueFor(jpaBase) + "");

        for (java.lang.reflect.Field field : object.getClass().getFields()) {
            play.modules.search.Field index = field.getAnnotation(play.modules.search.Field.class);
            if (index == null)
                continue;
            if (field.getType().isArray())
                continue;
            if (Collection.class.isAssignableFrom(field.getType()))
                continue;

            String name = field.getName();
            if (JPABase.class.isAssignableFrom(field.getType()) && index.joinField().length > 0) {
                traverseJoinFields(builder, name, "", index, (JPABase ) field.get(object));
            } else {
                builder.addField(name, valueOf(object, field), index);
            }
        }

        return builder.toDocument();
    }

    static void traverseJoinFields(DocumentBuilder builder, String indexName, String prefix, play.modules.search.Field index, JPABase object) throws Exception {
        for (java.lang.reflect.Field field : object.getClass().getFields()) {
            String name = prefix + field.getName();
            if (isJoinFieldMatch(index.joinField(), name)) {
                if (JPABase.class.isAssignableFrom(field.getType())) {
                    traverseJoinFields(builder, indexName, name + JAVA_MEMBER_ACCESSOR, index, (JPABase ) field.get(object));
                } else if (Arrays.asList(index.joinField()).contains(name)) {
                    builder.addField(indexName + JAVA_MEMBER_ACCESSOR + name, valueOf(object, field), index);
                }
            }
        }
    }

    static boolean isJoinFieldMatch(String[] joinFields, String name) {
        for (String joinField : joinFields) {
            if (joinField.equals(name) || joinField.startsWith(name + JAVA_MEMBER_ACCESSOR)) {
                return true;
            }
        }

        return false;
    }

    public static String valueOf(Object object, java.lang.reflect.Field field) throws Exception {
        if (field.getType().equals(String.class)) {
            return (String ) field.get(object);
        }
        if (field.getType().equals(FileAttachment.class) && field.get(object) != null) {
            return FileExtractor.getText((FileAttachment ) field.get(object));
        }

        Object o = field.get(object);
        if (field.isAnnotationPresent(ManyToOne.class) && o instanceof JPABase) {
            return "" + getIdValueFor((JPABase ) o);
        }

        return "" + field.get(object);
    }

    /**
     * Looks for the type of the id fiels on the JPABase target class and use
     * play's binder to retrieve the corresponding object used to build JPA load
     * query
     * 
     * @param clazz JPABase target class
     * @param indexValue String value of the id, taken from index
     * @return Object id expected to build query
     */
    public static Object getIdValueFromIndex(Class clazz, String indexValue) {
        java.lang.reflect.Field field = getIdField(clazz);
        Class parameter = field.getType();
        try {
            return Binder.directBind(indexValue, parameter);
        } catch (Exception e) {
            throw new UnexpectedException("Could not convert the ID from index to corresponding type", e);
        }
    }

    /**
     * Find a ID field on the JPABase target class
     * 
     * @param clazz JPABase target class
     * @return corresponding field
     */
    public static java.lang.reflect.Field getIdField(Class clazz) {
        for (java.lang.reflect.Field field : clazz.getFields()) {
            if (field.getAnnotation(Id.class) != null) {
                return field;
            }
        }
        throw new RuntimeException("Your class " + clazz.getName()
                        + " is annotated with javax.persistence.Id but the field Id was not found");
    }

    /**
     * Lookups the id field, being a Long id for Model and an annotated field @Id
     * for JPABase and returns the field value.
     * 
     * @param jpaBase is a Play! Framework that supports JPA
     * @return the field value (a Long or a String for UUID)
     */
    public static Object getIdValueFor(JPABase jpaBase) {
        if (jpaBase instanceof Model) {
            return ((Model ) jpaBase).id;
        }

        java.lang.reflect.Field field = getIdField(jpaBase.getClass());
        Object val = null;
        try {
            val = field.get(jpaBase);
        } catch (IllegalAccessException e) {
            Logger.error("Unable to read the field value of a field annotated with @Id " + field.getName() + " due to "
                            + e.getMessage(), e);
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
