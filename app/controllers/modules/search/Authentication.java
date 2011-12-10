package controllers.modules.search;

import play.Play;
import play.libs.Codec;
import play.mvc.Before;
import play.mvc.Controller;
import play.mvc.Http;


public class Authentication extends Controller {

    @Before(unless={"showAuth","checkAuth"})
    protected static void check () {
        if (Play.configuration.getProperty("play.search.auth.method", "http").equals("session")) {
            checkSession ();
        } else {
            checkHttp ();
        }
    }
	
    protected static void checkSession () {
        if (!session.contains("play.search.login")) {
            showAuth();
        }
    }
    
    public static void showAuth () {
        render();
    }
    
    public static void checkAuth (String login, String password) {
        String confLogin = Play.configuration.getProperty("play.search.login","search");
        String confPassword = Play.configuration.getProperty("play.search.password","search");
        if (confLogin.equals(login) && confPassword.equals(password)) {
            session.put("play.search.login", login);
            Administration.index();
        }
        showAuth();
    }
    
    protected static void checkHttp () {
        Http.Header auth = request.headers.get("authorization");
        if(auth != null) {
            String encodedPassword = auth.value().substring("Basic ".length());
            String password = new String(Codec.decodeBASE64(encodedPassword));
            String user = password.substring(0, password.indexOf(':'));
            String pwd = password.substring((user + ":").length());
            if (! pwd.equals(Play.configuration.getProperty("play.search.password","search")))
                    unauthorized("You are not authorized");
         } else {
             unauthorized("You are not authorized");
         }
    }
}
