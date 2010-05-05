package controllers.modules.search;

import java.util.List;

import play.Play;
import play.Play.Mode;
import play.modules.search.Search;
import play.modules.search.store.ManagedIndex;
import play.mvc.Before;
import play.mvc.Controller;
import play.mvc.results.Forbidden;

public class Administration extends Controller {
    
    @Before
    protected static void check () {
        if (Play.mode == Mode.PROD)
            throw new Forbidden("Access Denied");
    }
    
    public static void index () {
        List<ManagedIndex> indexes = Search.getCurrentStore().listIndexes();
        render(indexes);
    }
    
    public static void optimize (String name) {
        Search.getCurrentStore().optimize(name);
        index();
    }
    
    public static void reindex(String name) {
        Search.getCurrentStore().rebuild(name);
        index();
    }
    
    public static void reopen (String name) {
        Search.getCurrentStore().reopen(name);
        index();
    }
}
