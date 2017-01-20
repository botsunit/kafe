% @hidden
-module(kafe_conn).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Addr, Port) ->
  gen_server:start_link(?MODULE, {Addr, Port}, [{timeout, 5000}]).

init({Addr, Port}) ->
  erlang:process_flag(trap_exit, true),
  SndBuf = doteki:get_env([kafe, socket, sndbuf], ?DEFAULT_SOCKET_SNDBUF),
  RecBuf = doteki:get_env([kafe, socker, recbuf], ?DEFAULT_SOCKET_RECBUF),
  Buffer = lists:max([SndBuf, RecBuf, doteki:get_env([kafe, socket, buffer], max(SndBuf, RecBuf))]),
  case gen_tcp:connect(Addr, Port, [{mode, binary},
                                    {active, true},
                                    {packet, 4},
                                    {sndbuf, SndBuf},
                                    {recbuf, RecBuf},
                                    {buffer, Buffer}]) of
    {ok, Socket} ->
      {ok, {LocalAddr, LocalPort}} = inet:sockname(Socket),
      lager:debug("Connected to broker ~s:~p from ~s:~p", [bucinet:ip_to_string(Addr), Port, bucinet:ip_to_string(LocalAddr), LocalPort]),
      ApiVersion = doteki:get_env([kafe, api_version], ?DEFAULT_API_VERSION),
      CorrelationID = doteki:get_env([kafe, correlation_id], ?DEFAULT_CORRELATION_ID),
      ClientID = doteki:get_env([kafe, client_id], ?DEFAULT_CLIENT_ID),
      {ok, #{
         ip => Addr,
         port => Port,
         socket => Socket,
         api_version => ApiVersion,
         correlation_id => CorrelationID,
         client_id => ClientID,
         requests => orddict:new()
        }};
    {error, Reason} ->
      lager:debug("Connection failed to ~s:~p : ~p", [bucinet:ip_to_string(Addr), Port, Reason]),
      {stop, Reason}
  end.

handle_call({call, Request, RequestParams, Response}, From, State) ->
  handle_call({call, Request, RequestParams, Response, []}, From, State);
handle_call({call, Request, RequestParams, Response, ResponseParams}, From, State) ->
  try
    send_request(erlang:apply(Request, RequestParams ++ [State]),
                 From,
                 {Response, ResponseParams},
                 State)
  catch
    Type:Error ->
      lager:error("Request error: ~p:~p", [Type, Error]),
      {reply, {error, Error}, State}
  end;
handle_call(alive, _From, #{socket := Socket} = State) ->
  case inet:setopts(Socket, []) of
    ok ->
      {reply, ok, State};
    {error, _} = Reason ->
      {reply, Reason, State}
  end;
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({tcp, _Socket, Packet}, State) ->
  case kafe_protocol:response(Packet, State) of
    {ok, State1} ->
      {noreply, State1};
    {error, _} = Reason ->
      {stop, Reason, State}
  end;
handle_info({tcp_closed, _Socket}, #{ip := Addr, port := Port} = State) ->
  lager:debug("Connection to broker ~s:~p closed", [bucinet:ip_to_string(Addr), Port]),
  {stop, normal, State};
handle_info(Info, State) ->
  lager:warning("Invalid message : ~p", [Info]),
  lager:debug("--- State ~p", [State]),
  {noreply, State}.

terminate(_Reason, #{ip := IP, port := Port, socket := Socket}) ->
  lager:debug("Close connection to broker ~p:~p", [IP, Port]),
  _ = gen_tcp:close(Socket),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

send_request(#{packet := Packet},
             _From,
             {undefined, []},
             #{socket := Socket} = State1) ->
  case gen_tcp:send(Socket, Packet) of
    ok ->
      {reply, ok, State1};
    {error, _} = Error ->
      {stop, normal, Error, State1}
  end;
send_request(#{packet := Packet, state := State2, api_version := ApiVersion},
             From,
             Handler,
             #{correlation_id := CorrelationId,
               requests := Requests,
               socket := Socket} = State1) ->
  case gen_tcp:send(Socket, Packet) of
    ok ->
      {noreply,
       maps:update(
         requests,
         orddict:store(CorrelationId,
                       #{from => From, handler => Handler, socket => Socket, api_version => ApiVersion},
                       Requests),
         State2)};
    {error, _} = Error ->
      {stop, normal, Error, State1}
  end.

-ifdef(TEST).

handle_info_tcp_test_() ->
  {foreach,
    fun ()->
      meck:new(kafe_protocol)
    end,
    fun (_)->
      meck:unload()
    end,
    [
     {"Forwards packets to kafe_protocol:response",
      ?_test(begin
               meck:expect(kafe_protocol, response, fun(Packet, _State) -> {ok, #{state => Packet}} end),
               ?assertEqual({noreply, #{state => <<"packet">>}},
                             handle_info({tcp, fake_socket, <<"packet">>}, #{state => before}))
             end)},
     {"Errors in response processing stop kafe_conn",
      ?_test(begin
               meck:expect(kafe_protocol, response, fun(_Packet, _State) -> {error, some_reason} end),
               ?assertEqual({stop, {error, some_reason}, #{state => before}},
                             handle_info({tcp, fake_socket, <<1, 2, 3>>}, #{state => before}))
             end)}
    ]
  }.

-endif.
