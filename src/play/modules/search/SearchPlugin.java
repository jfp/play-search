package play.modules.search;

import play.PlayPlugin;
import play.exceptions.UnexpectedException;
/**
 * Integrated to Play's lifcycle, SearchPlugin
 * will trap JPA events and drive the Search
 * service.
 * @author jfp
 *
 */
public class SearchPlugin extends PlayPlugin {
    
    @Override
	public void onApplicationStart() {
        Search.init();
	}

	@Override
    public void onApplicationStop() {
        try {
            Search.shutdown();
        } catch (Exception e) {
            throw new UnexpectedException (e);
        }
    }

    @Override
    public void onEvent(String message, Object context) {
        if (!message.startsWith("JPASupport")) 
            return;
        if (message.equals("JPASupport.objectPersisted") || message.equals("JPASupport.objectUpdated")) {
            Search.index (context);
        } else if (message.equals("JPASupport.objectDeleted")) {
            Search.unIndex(context);
        }
    }
}
