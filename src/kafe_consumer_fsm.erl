% @hidden
% see https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design
-module(kafe_consumer_fsm).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_fsm).
-include("../include/kafe.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API.
-export([start_link/2]).

%% gen_fsm.
-export([init/1]).
-export([handle_event/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([terminate/3]).
-export([code_change/4]).

% States
-export([
         dead/2,
         awaiting_sync/2,
         stable/2
        ]).

-record(state, {
          group_id,
          client_id = undefined,
          member_id,
          leader_id = undefined,
          generation_id = undefined,
          topics = [],
          session_timeout,
          members = [],
          protocol_group
         }).

-define(MIN_TIMEOUT, 10).
-define(DEAD_TIMEOUT(_), ?MIN_TIMEOUT).
-define(PREPARING_REBALANCE(_), ?MIN_TIMEOUT).
-define(AWAITING_SYNC_TIMEOUT(_), ?MIN_TIMEOUT).
-define(STABLE_TIMEOUT(State),
        begin
          #state{session_timeout = SessionTimeout} = State,
          bucs:to_integer(SessionTimeout - (0.1 * SessionTimeout))
        end).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupID, Options) when is_map(Options) ->
  gen_fsm:start_link(?MODULE, [GroupID, Options], []).

%% gen_fsm.

% @hidden
init([GroupID, Options]) ->
  lager:info("Starting consumer for group ~s", [GroupID]),
  erlang:process_flag(trap_exit, true),
  kafe_consumer_store:insert(GroupID, fsm_pid, self()),
  SessionTimeout = maps:get(session_timeout, Options, ?DEFAULT_JOIN_GROUP_SESSION_TIMEOUT),
  MemberID = maps:get(member_id, Options, ?DEFAULT_JOIN_GROUP_MEMBER_ID),
  kafe_consumer_store:insert(GroupID, member_id, MemberID),
  Topics = lists:map(fun
                       ({_, _} = T) -> T;
                       (T) -> {T, kafe:partitions(T)}
                     end, maps:get(topics, Options,
                                   lists:map(fun(#{topic := Topic, partitions := Partitions}) ->
                                                 {Topic, Partitions}
                                             end, ?DEFAULT_GROUP_PARTITION_ASSIGNMENT))),
  State = #state{
             group_id = bucs:to_binary(GroupID),
             % client_id
             member_id = MemberID,
             % leader_id
             % generation_id
             topics = Topics,
             session_timeout = SessionTimeout
             % members
             % protocol_group
            },
  {ok, dead, State, ?DEAD_TIMEOUT(State)}.

% @hidden
dead(timeout, #state{group_id = GroupID,
                     member_id = MemberID,
                     topics = Topics,
                     session_timeout = SessionTimeout} = State) ->
  lager:debug("Group ~s : join_group...", [GroupID]),
  ProtocolTopics = lists:map(fun({Topic, _}) -> Topic;
                                (Topic) -> Topic
                             end, Topics),
  case kafe:join_group(GroupID, #{session_timeout => SessionTimeout,
                                  member_id => MemberID,
                                  protocol_type => ?DEFAULT_JOIN_GROUP_PROTOCOL_TYPE,
                                  protocols => [kafe:default_protocol(
                                                  ?DEFAULT_GROUP_PROTOCOL_NAME,
                                                  ?DEFAULT_GROUP_PROTOCOL_VERSION,
                                                  ProtocolTopics,
                                                  ?DEFAULT_GROUP_USER_DATA)]}) of
    {ok, #{error_code := none,
           generation_id := GenerationID,
           leader_id := LeaderID,
           member_id := NewMemberID,
           protocol_group := ProtocolGroup}} ->
      kafe_consumer_store:insert(GroupID, member_id, NewMemberID),
      kafe_consumer_store:insert(GroupID, generation_id, GenerationID),
      next_state(State#state{generation_id = GenerationID,
                             leader_id = LeaderID,
                             member_id = NewMemberID,
                             protocol_group = ProtocolGroup});
    {ok, #{error_code := unknown_member_id}} ->
      next_state(dead, State#state{member_id = <<>>});
    {ok, #{error_code := _}} ->
      next_state(dead, State);
    {error, Reason} ->
      lager:warning("Join group failed: ~p", [Reason]),
      timer:sleep(2000),
      next_state(State)
  end.

% @hidden
awaiting_sync(timeout, #state{group_id = GroupID,
                              generation_id = GenerationID,
                              member_id = MemberID,
                              leader_id = LeaderID,
                              members = Members,
                              topics = Topics} = State) ->
  lager:debug("Group ~s : awaiting_sync...", [GroupID]),
  GroupAssignment = group_assignment(LeaderID, MemberID, Topics, Members),
  case kafe:sync_group(GroupID, GenerationID, MemberID, GroupAssignment) of
    {ok, #{error_code := none}} ->
      next_state(State);
    {ok, #{error_code := Error}} ->
      lager:warning("Sync group failed: ~p", [Error]),
      next_state(State);
    {error, Reason} ->
      lager:warning("Sync group failed: ~p", [Reason]),
      next_state(State)
  end.

% @hidden
stable(timeout, #state{group_id = GroupID,
                       member_id = MemberID,
                       generation_id = GenerationID} = State) ->
  lager:debug("Group ~s : heartbeat...", [GroupID]),
  case kafe:heartbeat(GroupID, GenerationID, MemberID) of
    {ok, #{error_code := none}} ->
      next_state(State);
    {ok, #{error_code := Error}} ->
      lager:warning("Heartbeat error: ~p", [Error]),
      next_state(dead, State);
    {error, Reason} ->
      lager:warning("Heartbeat failed: ~p", [Reason]),
      next_state(State)
  end.

% @hidden
handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

% @hidden
handle_sync_event(_Event, _From, StateName, State) ->
  {reply, ignored, StateName, State}.

% @hidden
handle_info(_Info, StateName, State) ->
  {next_state, StateName, State}.

% @hidden
terminate(_Reason, _StateName, #state{group_id = GroupID}) ->
  kafe_consumer_store:delete(GroupID, fsm_pid),
  ok.

% @hidden
code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

next_state(#state{group_id = GroupID,
                  member_id = MemberID} = State) ->
  case kafe:describe_group(GroupID) of
    {ok, [#{error_code := none,
            state := GroupState,
            members := Members}]} ->
      case [T || #{member_assignment := #{partition_assignment := T,
                                          version := V},
                   member_id := M} <- Members, M == MemberID, V =/= -1] of
        [] ->
          ok;
        [Topics] ->
          kafe_utils:gen_server_call(kafe_consumer_store:value(GroupID, server_pid),
                                     {topics, [{T, P} || #{partitions := P, topic := T} <- Topics]})
      end,
      {NextState, Timeout} = group_state(State, GroupState),
      {next_state, NextState, State#state{members = Members}, Timeout};
    {ok, [#{error_code := Error}]} ->
      lager:warning("Can't get group ~s description: ~p", [GroupID, Error]),
      next_state(dead, State);
    {error, _} ->
      next_state(dead, State)
  end.

next_state(NextState, State) ->
  {NextState1, Timeout} = group_state(State, NextState),
  {next_state, NextState1, State, Timeout}.

group_state(State, Next) when is_binary(Next) ->
  group_state(State, state_by_name(Next));
group_state(_State, preparing_rebalance) ->
  {stable, ?PREPARING_REBALANCE(_State)}; % never append ?
group_state(#state{group_id = GroupID} = _State, dead) ->
  case kafe_consumer_store:lookup(GroupID, server_pid) of
    {ok, PID} ->
      _ = kafe_utils:gen_server_call(PID, stop_fetch);
    _ ->
      erlang:exit(group_is_dead)
  end,
  {dead, ?DEAD_TIMEOUT(_State)};
group_state(#state{group_id = GroupID} = _State, awaiting_sync) ->
  case kafe_consumer_store:lookup(GroupID, server_pid) of
    {ok, PID} ->
      _ = kafe_utils:gen_server_call(PID, stop_fetch);
    _ ->
      erlang:exit(group_is_dead)
  end,
  {awaiting_sync, ?AWAITING_SYNC_TIMEOUT(_State)};
group_state(#state{group_id = GroupID} = State, stable) ->
  case kafe_consumer_store:lookup(GroupID, server_pid) of
    {ok, PID} ->
      _ = kafe_utils:gen_server_call(PID, start_fetch);
    _ ->
      erlang:exit(group_is_dead)
  end,
  {stable, ?STABLE_TIMEOUT(State)}.

state_by_name(<<"PreparingRebalance">>) -> preparing_rebalance;
state_by_name(<<"Dead">>) -> dead;
state_by_name(<<"AwaitingSync">>) -> awaiting_sync;
state_by_name(<<"Stable">>) -> stable.

group_assignment(MemberID, MemberID, Topics, Members) ->
  MemberIDs = [M || #{member_id := M} <- Members],
  [#{member_id => Member,
     member_assignment => #{
       version => ?DEFAULT_GROUP_PROTOCOL_VERSION,
       user_data => ?DEFAULT_GROUP_USER_DATA,
       partition_assignment => [#{topic => T,
                                  partitions => P} || {T, P} <- Assignment, length(P) =/= 0]}}
   || {Member, Assignment} <- lists:zip(MemberIDs, assign(Topics, length(MemberIDs), lists:duplicate(length(MemberIDs), [])))];
group_assignment(_, _, _, _) ->
  [].

assign([], _, Acc) ->
  Acc;
assign([{T, P}|Rest], S, Acc) ->
  Repartition = split_list_parts(P, S),
  Repartition1 = if
                   S > length(Repartition) ->
                     Repartition ++ lists:duplicate(S - length(Repartition), []);
                   true ->
                     Repartition
                 end,
  Repartition2 = lists:zip(lists:duplicate(S, T), Repartition1),
  assign(Rest, S, assign_zip(Acc, Repartition2, [])).

split_list_parts(List, N) ->
  split_list_parts(List, erlang:length(List), N, []).

split_list_parts([], _, _, Acc) ->
  lists:reverse(Acc);
split_list_parts(List, Size, N, Acc) ->
  NE = Size div N,
  case lists:split(NE, List) of
    {[], Rest} ->
      split_list_parts(Rest, Size, N - 1, Acc);
    {R, Rest} ->
      split_list_parts(Rest, Size - erlang:length(R), N - 1, [R|Acc])
  end.

assign_zip([], [], Acc) ->
  Acc;
assign_zip([L|RL], [E|RE], Acc) ->
  assign_zip(RL, RE, [[E|L]|Acc]).

-ifdef(TEST).
kafe_consumer_fsm_init_default_test_() ->
  {setup,
   fun() ->
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:new(kafe),
       meck:expect(kafe, topics, 0,
                   #{<<"topic">> => #{0 => "localhost:9093",
                                      1 => "localhost:9094",
                                      2 => "localhost:9092"}}),
       meck:expect(kafe, partitions, 1, [0, 1, 2])
   end,
   fun(_) ->
       meck:unload(kafe),
       meck:unload(kafe_consumer_store)
   end,
   [
    fun() ->
        ?assertEqual(
           {ok, dead,
            #state{
               group_id = <<"group">>,
               member_id = <<>>,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout = 30000}, 10},
           init([<<"group">>, #{}]))
    end
   ]}.

kafe_consumer_fsm_init_custom_test_() ->
  {setup,
   fun() ->
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:new(kafe),
       meck:expect(kafe, topics, 0,
                   #{<<"topic">> => #{0 => "localhost:9093",
                                      1 => "localhost:9094",
                                      2 => "localhost:9092"}}),
       meck:expect(kafe, partitions, 1, [0, 1, 2])
   end,
   fun(_) ->
       meck:unload(kafe),
       meck:unload(kafe_consumer_store)
   end,
   [
    fun() ->
        ?assertEqual(
           {ok, dead,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               topics = [{<<"test1">>, [0, 1, 2]},
                         {<<"test2">>, [0, 3]}],
               session_timeout = 1000}, 10},
           init([<<"group">>, #{session_timeout => 1000,
                                member_id => <<"member">>,
                                topics => [<<"test1">>, {<<"test2">>, [0, 3]}]}]))
    end
   ]}.

kafe_consumer_fsm_dead_stable_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, join_group, 2,
                   {ok, #{error_code => none,
                          generation_id => 1,
                          leader_id => <<"member">>,
                          member_id => <<"member">>,
                          protocol_group => protocol_group}}),
       meck:expect(kafe, default_protocol, 4, default_protocol),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1}}
                                      ]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            stable,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = <<"member">>,
               generation_id = 1,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout =100,
               members = [#{member_assignment => #{
                    partition_assignment => [#{
                      partitions => [0, 1, 2],
                      topic => <<"topic">>}],
                    version => 1}}],
               protocol_group = protocol_group},
            90},
           dead(timeout, #state{group_id = <<"group">>,
                                member_id = <<"member">>,
                                topics = [{<<"topic">>, [0, 1, 2]}],
                                session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_dead_stable_join_group_unknown_member_id_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, join_group, 2,
                   {ok, #{error_code => unknown_member_id}}),
       meck:expect(kafe, default_protocol, 4, default_protocol),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1}}
                                      ]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            dead,
            #state{
               group_id = <<"group">>,
               member_id = <<>>,
               leader_id = undefined,
               generation_id = undefined,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout = 100,
               members = [],
               protocol_group = undefined},
            10},
           dead(timeout, #state{group_id = <<"group">>,
                                member_id = <<"member">>,
                                topics = [{<<"topic">>, [0, 1, 2]}],
                                session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_dead_stable_join_group_kafka_error_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, join_group, 2,
                   {ok, #{error_code => unknown}}),
       meck:expect(kafe, default_protocol, 4, default_protocol),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1}}
                                      ]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            dead,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = undefined,
               generation_id = undefined,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout = 100,
               members = [],
               protocol_group = undefined},
            10},
           dead(timeout, #state{group_id = <<"group">>,
                                member_id = <<"member">>,
                                topics = [{<<"topic">>, [0, 1, 2]}],
                                session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_dead_stable_join_group_kafka_internal_error_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, join_group, 2,
                   {error, test_error}),
       meck:expect(kafe, default_protocol, 4, default_protocol),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1}}
                                      ]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            stable,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = undefined,
               generation_id = undefined,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout = 100,
               members = [#{member_assignment => #{
                    partition_assignment => [#{
                      partitions => [0, 1, 2],
                      topic => <<"topic">>}],
                    version => 1}}],
               protocol_group = undefined},
            90},
           dead(timeout, #state{group_id = <<"group">>,
                                member_id = <<"member">>,
                                topics = [{<<"topic">>, [0, 1, 2]}],
                                session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_dead_awaiting_sync_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, join_group, 2,
                   {ok, #{error_code => none,
                          generation_id => 1,
                          leader_id => <<"member">>,
                          member_id => <<"member">>,
                          protocol_group => protocol_group}}),
       meck:expect(kafe, default_protocol, 4, default_protocol),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"AwaitingSync">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1}}
                                      ]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            awaiting_sync,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = <<"member">>,
               generation_id = 1,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout =100,
               members = [#{member_assignment => #{
                    partition_assignment => [#{
                      partitions => [0, 1, 2],
                      topic => <<"topic">>}],
                    version => 1}}],
               protocol_group = protocol_group},
            10},
           dead(timeout, #state{group_id = <<"group">>,
                                member_id = <<"member">>,
                                topics = [{<<"topic">>, [0, 1, 2]}],
                                session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_dead_dead_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, join_group, 2,
                   {ok, #{error_code => none,
                          generation_id => 1,
                          leader_id => <<"member">>,
                          member_id => <<"member">>,
                          protocol_group => protocol_group}}),
       meck:expect(kafe, default_protocol, 4, default_protocol),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Dead">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1}}
                                      ]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            dead,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = <<"member">>,
               generation_id = 1,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout =100,
               members = [#{member_assignment => #{
                    partition_assignment => [#{
                      partitions => [0, 1, 2],
                      topic => <<"topic">>}],
                    version => 1}}],
               protocol_group = protocol_group},
            10},
           dead(timeout, #state{group_id = <<"group">>,
                                member_id = <<"member">>,
                                topics = [{<<"topic">>, [0, 1, 2]}],
                                session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_dead_preparing_rebalance_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, join_group, 2,
                   {ok, #{error_code => none,
                          generation_id => 1,
                          leader_id => <<"member">>,
                          member_id => <<"member">>,
                          protocol_group => protocol_group}}),
       meck:expect(kafe, default_protocol, 4, default_protocol),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"PreparingRebalance">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1}}
                                      ]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, insert, 3, ok),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            stable,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = <<"member">>,
               generation_id = 1,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout =100,
               members = [#{member_assignment => #{
                    partition_assignment => [#{
                      partitions => [0, 1, 2],
                      topic => <<"topic">>}],
                    version => 1}}],
               protocol_group = protocol_group},
            10},
           dead(timeout, #state{group_id = <<"group">>,
                                member_id = <<"member">>,
                                topics = [{<<"topic">>, [0, 1, 2]}],
                                session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_awaiting_sync_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, sync_group, 4,
                   {ok, #{error_code => none}}),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1},
                                        member_id => <<"member">>}]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            stable,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = <<"member">>,
               generation_id = 1,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout =100,
               members = [#{member_assignment => #{
                              partition_assignment => [#{
                                partitions => [0, 1, 2],
                                topic => <<"topic">>}],
                              version => 1},
                            member_id => <<"member">>}]},
            90},
           awaiting_sync(timeout, #state{group_id = <<"group">>,
                                         generation_id = 1,
                                         member_id = <<"member">>,
                                         leader_id = <<"member">>,
                                         topics = [{<<"topic">>, [0, 1, 2]}],
                                         members = [#{member_assignment =>
                                                      #{partition_assignment =>
                                                        [#{partitions => [0, 1, 2],
                                                           topic => <<"topic">>}],
                                                        version => 1},
                                                      member_id => <<"member">>}],
                                         session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_awaiting_sync_kafe_error_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, sync_group, 4,
                   {ok, #{error_code => unknown}}),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1},
                                        member_id => <<"member">>}]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            stable,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = <<"member">>,
               generation_id = 1,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout =100,
               members = [#{member_assignment => #{
                              partition_assignment => [#{
                                partitions => [0, 1, 2],
                                topic => <<"topic">>}],
                              version => 1},
                            member_id => <<"member">>}]},
            90},
           awaiting_sync(timeout, #state{group_id = <<"group">>,
                                         generation_id = 1,
                                         member_id = <<"member">>,
                                         leader_id = <<"member">>,
                                         topics = [{<<"topic">>, [0, 1, 2]}],
                                         members = [#{member_assignment =>
                                                      #{partition_assignment =>
                                                        [#{partitions => [0, 1, 2],
                                                           topic => <<"topic">>}],
                                                        version => 1},
                                                      member_id => <<"member">>}],
                                         session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_awaiting_sync_kafe_internal_error_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, sync_group, 4,
                   {error, test_error}),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1},
                                        member_id => <<"member">>}]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
    fun() ->
        ?assertEqual(
           {next_state,
            stable,
            #state{
               group_id = <<"group">>,
               member_id = <<"member">>,
               leader_id = <<"member">>,
               generation_id = 1,
               topics = [{<<"topic">>, [0, 1, 2]}],
               session_timeout =100,
               members = [#{member_assignment => #{
                              partition_assignment => [#{
                                partitions => [0, 1, 2],
                                topic => <<"topic">>}],
                              version => 1},
                            member_id => <<"member">>}]},
            90},
           awaiting_sync(timeout, #state{group_id = <<"group">>,
                                         generation_id = 1,
                                         member_id = <<"member">>,
                                         leader_id = <<"member">>,
                                         topics = [{<<"topic">>, [0, 1, 2]}],
                                         members = [#{member_assignment =>
                                                      #{partition_assignment =>
                                                        [#{partitions => [0, 1, 2],
                                                           topic => <<"topic">>}],
                                                        version => 1},
                                                      member_id => <<"member">>}],
                                         session_timeout = 100}))
    end
   ]}.

kafe_consumer_fsm_stable_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, heartbeat, 3,
                   {ok, #{error_code => none}}),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1},
                                        member_id => <<"member">>}]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
     fun() ->
         State = #state{group_id = <<"group">>,
                        member_id = <<"member">>,
                        generation_id = <<"generation_id">>,
                        session_timeout = 100,
                        members = [#{member_assignment => #{
                                       partition_assignment => [#{
                                         partitions => [0, 1, 2],
                                         topic => <<"topic">>}],
                                       version => 1},
                                     member_id => <<"member">>}]},
         ?assertEqual(
            {next_state, stable, State, 90},
            stable(timeout, State))
     end
   ]}.

kafe_consumer_fsm_stable_kafka_error_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, heartbeat, 3,
                   {ok, #{error_code => unknown}}),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1},
                                        member_id => <<"member">>}]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
     fun() ->
         State = #state{group_id = <<"group">>,
                        member_id = <<"member">>,
                        generation_id = <<"generation_id">>,
                        session_timeout = 100,
                        members = [#{member_assignment => #{
                                       partition_assignment => [#{
                                         partitions => [0, 1, 2],
                                         topic => <<"topic">>}],
                                       version => 1},
                                     member_id => <<"member">>}]},
         ?assertEqual(
            {next_state, dead, State, 10},
            stable(timeout, State))
     end
   ]}.

kafe_consumer_fsm_stable_kafe_internal_error_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, heartbeat, 3,
                   {error, test_error}),
       meck:expect(kafe, describe_group, 1,
                   {ok, [#{error_code => none,
                           state => <<"Stable">>,
                           members => [#{member_assignment =>
                                         #{partition_assignment =>
                                           [#{partitions => [0, 1, 2],
                                              topic => <<"topic">>}],
                                           version => 1},
                                        member_id => <<"member">>}]}]}),
       meck:new(kafe_consumer_store),
       meck:expect(kafe_consumer_store, value, 2, ok),
       meck:expect(kafe_consumer_store, lookup, 2, {ok, c:pid(0, 0, 0)}),
       meck:new(kafe_utils),
       meck:expect(kafe_utils, gen_server_call, 2, ok)
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       meck:unload(kafe_consumer_store),
       meck:unload(kafe)
   end,
   [
     fun() ->
         State = #state{group_id = <<"group">>,
                        member_id = <<"member">>,
                        generation_id = <<"generation_id">>,
                        session_timeout = 100,
                        members = [#{member_assignment => #{
                                       partition_assignment => [#{
                                         partitions => [0, 1, 2],
                                         topic => <<"topic">>}],
                                       version => 1},
                                     member_id => <<"member">>}]},
         ?assertEqual(
            {next_state, stable, State, 90},
            stable(timeout, State))
     end
   ]}.

-endif.
