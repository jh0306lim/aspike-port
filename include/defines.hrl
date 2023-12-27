-ifndef(__DEFINES_HRL__).
-define(__DEFINES_HRL__, 1).

-define(APPNAME, aspike_port).

-define(DEFAULT_HOST, application:get_env(?APPNAME, host, "127.0.0.1")).
-define(DEFAULT_PORT, application:get_env(?APPNAME, port, 3010)).
-define(DEFAULT_TIMEOUT, application:get_env(?APPNAME, timeout, 10000)).
-define(DEFAULT_USER, application:get_env(?APPNAME, user, "")).
-define(DEFAULT_PSW, application:get_env(?APPNAME, psw, "")).

-endif.
