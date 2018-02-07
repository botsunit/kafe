% @hidden
-module(kafe_utils).

-export([
         broker_id/2
         , broker_name/2
         , timestamp/0
         , gen_server_call/2
        ]).

broker_id(Host, Port) ->
  bucs:to_atom(bucs:to_string(Host) ++ ":" ++ bucs:to_string(Port)).

broker_name(Host, Port) ->
  Host1 = if
            is_tuple(Host) ->
              bucinet:ip_to_string(Host);
            true ->
              bucs:to_string(Host)
          end,
  string:join([bucs:to_string(Host1), bucs:to_string(Port)], ":").

timestamp() ->
  erlang:system_time(millisecond).

% Because meck can't mock gen_server:call/2
gen_server_call(ServerRef, Request) ->
  gen_server:call(ServerRef, Request).

