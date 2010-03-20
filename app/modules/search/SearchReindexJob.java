package modules.search;

import play.Play;
import play.jobs.Job;
import play.jobs.OnApplicationStart;
import play.modules.search.Search;

@OnApplicationStart
public class SearchReindexJob extends Job {
    
    public void doJob() throws Exception {
        if(Boolean.parseBoolean(Play.configuration.getProperty("play.search.reindex","false"))) {
            try {
                Search.reindex();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
