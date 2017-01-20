% @hidden
-module(kafe_consumer_srv).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-callback init(Args :: list()) -> {ok, any()} | ignore.
-callback consume(Offset :: integer(),
                  Key :: binary(),
                  Value :: binary()) -> ok.

%% API.
-export([start_link/2]).

%% gen_server.
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

-record(state, {
          group_id,
          topics = [],
          fetchers = [],
          callback = undefined,
          fetch_interval = ?DEFAULT_CONSUMER_FETCH_INTERVAL,
          fetch_pids = [],
          max_bytes = ?DEFAULT_FETCH_MAX_BYTES,
          min_bytes = ?DEFAULT_FETCH_MIN_BYTES,
          max_wait_time = ?DEFAULT_FETCH_MAX_WAIT_TIME,
          commit = ?DEFAULT_CONSUMER_COMMIT,
          from_beginning = ?DEFAULT_CONSUMER_START_FROM_BEGINNING,
          allow_unordered_commit = ?DEFAULT_CONSUMER_ALLOW_UNORDERED_COMMIT,
          commits = #{},
          on_start_fetching = ?DEFAULT_CONSUMER_ON_START_FETCHING,
          on_stop_fetching = ?DEFAULT_CONSUMER_ON_STOP_FETCHING,
          on_assignment_change = ?DEFAULT_CONSUMER_ON_ASSIGNMENT_CHANGE,
          can_fetch = ?DEFAULT_CONSUMER_CAN_FETCH,
          errors_actions = ?DEFAULT_CONSUMER_ERRORS_ACTIONS
         }).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupID, Options) ->
  gen_server:start_link(?MODULE, [GroupID, Options], []).

%% gen_server.

% @hidden
init([GroupID, Options]) ->
  erlang:process_flag(trap_exit, true),
  kafe_consumer_store:insert(GroupID, server_pid, self()),
  kafe_consumer_store:insert(GroupID, can_fetch, false),
  AllowUnorderedCommit = maps:get(allow_unordered_commit, Options, ?DEFAULT_CONSUMER_ALLOW_UNORDERED_COMMIT),
  kafe_consumer_store:insert(GroupID, allow_unordered_commit, AllowUnorderedCommit),
  FetchInterval = maps:get(fetch_interval, Options, ?DEFAULT_CONSUMER_FETCH_INTERVAL),
  MaxBytes = maps:get(max_bytes, Options, ?DEFAULT_FETCH_MAX_BYTES),
  MinBytes = maps:get(min_bytes, Options, ?DEFAULT_FETCH_MIN_BYTES),
  MaxWaitTime = maps:get(max_wait_time, Options, ?DEFAULT_FETCH_MAX_WAIT_TIME),
  Commit = maps:get(commit, Options, ?DEFAULT_CONSUMER_COMMIT),
  FromBeginning = maps:get(from_beginning, Options, ?DEFAULT_CONSUMER_START_FROM_BEGINNING),
  OnStartFetching = maps:get(on_start_fetching, Options, ?DEFAULT_CONSUMER_ON_START_FETCHING),
  OnStopFetching = maps:get(on_stop_fetching, Options, ?DEFAULT_CONSUMER_ON_STOP_FETCHING),
  OnAssignmentChange = maps:get(on_assignment_change, Options, ?DEFAULT_CONSUMER_ON_ASSIGNMENT_CHANGE),
  CanFetch = maps:get(can_fetch, Options, ?DEFAULT_CONSUMER_CAN_FETCH),
  ErrorsActions = maps:get(errors_actions, Options, ?DEFAULT_CONSUMER_ERRORS_ACTIONS),
  kafe_consumer_store:insert(GroupID, can_fetch_fun, CanFetch),
  {ok, #state{
          group_id = bucs:to_binary(GroupID),
          callback = maps:get(callback, Options),
          fetch_interval = FetchInterval,
          max_bytes = MaxBytes,
          min_bytes = MinBytes,
          max_wait_time = MaxWaitTime,
          commit = Commit,
          from_beginning = FromBeginning,
          allow_unordered_commit = AllowUnorderedCommit,
          on_start_fetching = OnStartFetching,
          on_stop_fetching = OnStopFetching,
          on_assignment_change = OnAssignmentChange,
          can_fetch = CanFetch,
          errors_actions = ErrorsActions
         }}.

% @hidden
handle_call({topics, Topics}, _From, #state{topics = CurrentTopics, group_id = GroupID} = State) ->
  if
    Topics == CurrentTopics ->
      {reply, ok, State};
    true ->
      kafe_consumer_store:insert(GroupID, topics, Topics),
      {reply, ok, update_fetchers(Topics, State#state{topics = Topics})}
  end;
handle_call(start_fetch, _From, #state{group_id = GroupID, on_start_fetching = OnStartFetching} = State) ->
  case kafe_consumer_store:lookup(GroupID, can_fetch) of
    {ok, true} ->
      ok;
    _ ->
      kafe_consumer_store:insert(GroupID, can_fetch, true),
      case OnStartFetching of
        Fun when is_function(Fun, 1) ->
          _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID]) end);
        {Module, Function} when is_atom(Module),
                                is_atom(Function) ->
          case bucs:function_exists(Module, Function, 1) of
            true ->
              _ = erlang:spawn(fun() -> erlang:apply(Module, Function, [GroupID]) end);
            _ ->
              ok
          end;
        _ ->
          ok
      end
  end,
  {reply, ok, State};
handle_call(stop_fetch, _From, #state{group_id = GroupID, on_stop_fetching = OnStopFetching} = State) ->
  case kafe_consumer_store:lookup(GroupID, can_fetch) of
    {ok, true} ->
      kafe_consumer_store:insert(GroupID, can_fetch, false),
      case OnStopFetching of
        Fun when is_function(Fun, 1) ->
          _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID]) end);
        {Module, Function} when is_atom(Module),
                                is_atom(Function) ->
          case bucs:function_exists(Module, Function, 1) of
            true ->
              _ = erlang:spawn(fun() -> erlang:apply(Module, Function, [GroupID]) end);
            _ ->
              ok
          end;
        _ ->
          ok
      end;
    _ ->
      ok
  end,
  {reply, ok, State};
handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info({'DOWN', MonitorRef, Type, Object, Info}, State) ->
  lager:debug("DOWN ~p, ~p, ~p, ~p", [MonitorRef, Type, Object, Info]),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(Reason, #state{group_id = GroupID, fetchers = Fetchers} = State) ->
  lager:debug("Will stop fetchers : ~p~nStacktrace:~s", [Reason, lager:pr_stacktrace(erlang:get_stacktrace())]),
  stop_fetchers([TP || {TP, _, _} <- Fetchers], State),
  kafe_consumer_store:delete(GroupID, server_pid),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% @hidden
update_fetchers(Topics, #state{fetchers = Fetchers,
                               group_id = GroupID,
                               on_assignment_change = OnAssignmentChange} = State) ->
  CurrentFetchers = [TP || {TP, _, _} <- Fetchers],
  NewFetchers = lists:foldl(fun({Topic, Partitions}, Acc) ->
                                lists:zip(
                                  lists:duplicate(length(Partitions), Topic),
                                  Partitions) ++ Acc
                            end, [], Topics),
  FetchersToStop = CurrentFetchers -- NewFetchers,
  FetchersToSart = NewFetchers -- CurrentFetchers,
  lager:debug("CurrentFetchers = ~p", [CurrentFetchers]),
  lager:debug("NewFetchers     = ~p", [NewFetchers]),
  lager:debug("Stop            = ~p", [FetchersToStop]),
  lager:debug("Start           = ~p", [FetchersToSart]),
  case OnAssignmentChange of
    Fun when is_function(Fun, 3) ->
      _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID, FetchersToStop, FetchersToSart]) end);
    {Module, Function} when is_atom(Module),
                            is_atom(Function) ->
      case bucs:function_exists(Module, Function, 3) of
        true ->
          _ = erlang:spawn(fun() -> erlang:apply(Module, Function, [GroupID, FetchersToStop, FetchersToSart]) end);
        _ ->
          ok
      end;
    _ ->
      ok
  end,
  State1 = stop_fetchers(FetchersToStop, State),
  start_fetchers(FetchersToSart, State1).

stop_fetchers([], State) ->
  State;
stop_fetchers([TP|Rest], #state{fetchers = Fetchers, commits = Commits, group_id = GroupID} = State) ->
  case lists:keyfind(TP, 1, Fetchers) of
    {{Topic, Partition} = TP, Pid, MRef} ->
      kafe_metrics:delete_consumer_partition(GroupID, Topic, Partition),
      CommitStoreKey = erlang:term_to_binary(TP),
      _ = erlang:demonitor(MRef),
      try
        kafe_consumer_group_sup:stop_child(Pid)
      catch
        C:E ->
          lager:error("Can't terminate kafe_consumer_fetcher #~p: ~p:~p", [Pid, C, E])
      end,
      stop_fetchers(Rest, State#state{fetchers = lists:keydelete(TP, 1, Fetchers),
                                      commits = maps:remove(CommitStoreKey, Commits)});
    false ->
      stop_fetchers(Rest, State)
  end.

start_fetchers([], State) ->
  State;
start_fetchers([{Topic, Partition}|Rest], #state{fetchers = Fetchers,
                                                 fetch_interval = FetchInterval,
                                                 group_id = GroupID,
                                                 commit = Commit,
                                                 from_beginning = FromBeginning,
                                                 min_bytes = MinBytes,
                                                 max_bytes = MaxBytes,
                                                 max_wait_time = MaxWaitTime,
                                                 callback = Callback,
                                                 errors_actions = ErrorsActions} = State) ->
  kafe_metrics:init_consumer_partition(GroupID, Topic, Partition),
  case kafe_consumer_group_sup:start_child(Topic, Partition, FetchInterval,
                                           GroupID, Commit, FromBeginning,
                                           MinBytes, MaxBytes, MaxWaitTime,
                                           ErrorsActions, Callback) of
    {ok, Pid} ->
      MRef = erlang:monitor(process, Pid),
      start_fetchers(Rest, State#state{fetchers = [{{Topic, Partition}, Pid, MRef}|Fetchers]});
    {error, Error} ->
      lager:error("Faild to start fetcher for topic ~s, partition ~p: ~p", [Topic, Partition, Error]),
      start_fetchers(Rest, State)
  end.

-ifdef(TEST).
start_fetch_without_fun_test() ->
  kafe_consumer_store:new(<<"test_cg">>),
  ?assertNot(true == kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  State = #state{group_id = <<"test_cg">>, on_start_fetching = undefined},
  ?assertEqual({reply, ok, State}, handle_call(start_fetch, from, State)),
  ?assert(kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  kafe_consumer_store:delete(<<"test_cg">>).

start_fetch_with_fun_test() ->
  kafe_consumer_store:new(<<"test_cg">>),
  ?assertNot(true == kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  State = #state{group_id = <<"test_cg">>,
                 on_start_fetching = fun(G) ->
                                         ?assertEqual(<<"test_cg">>, G)
                                     end},
  ?assertEqual({reply, ok, State}, handle_call(start_fetch, from, State)),
  ?assert(kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  kafe_consumer_store:delete(<<"test_cg">>).

start_fetch_with_invalid_fun_test() ->
  kafe_consumer_store:new(<<"test_cg">>),
  ?assertNot(true == kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  State = #state{group_id = <<"test_cg">>,
                 on_start_fetching = fun(_, _) ->
                                         ?assert(false)
                                     end},
  ?assertEqual({reply, ok, State}, handle_call(start_fetch, from, State)),
  ?assert(kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  kafe_consumer_store:delete(<<"test_cg">>).

stop_fetch_without_fun_test() ->
  kafe_consumer_store:new(<<"test_cg">>),
  kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true),
  State = #state{group_id = <<"test_cg">>,
                 on_stop_fetching = undefined},
  ?assertEqual({reply, ok, State}, handle_call(stop_fetch, from, State)),
  ?assertNot(kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  kafe_consumer_store:delete(<<"test_cg">>).

stop_fetch_with_fun_test() ->
  kafe_consumer_store:new(<<"test_cg">>),
  kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true),
  State = #state{group_id = <<"test_cg">>,
                 on_stop_fetching = fun(G) ->
                                        ?assertEqual(<<"test_cg">>, G)
                                    end},
  ?assertEqual({reply, ok, State}, handle_call(stop_fetch, from, State)),
  ?assertNot(kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  kafe_consumer_store:delete(<<"test_cg">>).

stop_fetch_with_invalid_fun_test() ->
  kafe_consumer_store:new(<<"test_cg">>),
  kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true),
  State = #state{group_id = <<"test_cg">>,
                 on_stop_fetching = fun(_, _) ->
                                        ?assert(false)
                                    end},
  ?assertEqual({reply, ok, State}, handle_call(stop_fetch, from, State)),
  ?assertNot(kafe_consumer_store:value(<<"test_cg">>, can_fetch)),
  kafe_consumer_store:delete(<<"test_cg">>).

update_fetchers_create_test() ->
  meck:new(kafe_consumer_group_sup, [passthrough]),
  meck:expect(kafe_consumer_group_sup, start_child, 11, {ok, c:pid(0, 0, 0)}),
  meck:expect(kafe_consumer_group_sup, stop_child, 1, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, delete_consumer_partition, 3, ok),
  meck:expect(kafe_metrics, init_consumer_partition, 3, ok),
  kafe_consumer_store:new(<<"test_cg">>),
  State = #state{group_id = <<"test_cg">>},
  Topics = [{<<"topic0">>, [0, 1, 2]}, {<<"topic1">>, [0, 1]}],
  ?assertMatch({reply, ok,
                #state{group_id = <<"test_cg">>,
                       topics = Topics,
                       fetchers = [{{<<"topic0">>, 2}, _, _},
                                   {{<<"topic0">>, 1}, _, _},
                                   {{<<"topic0">>, 0}, _, _},
                                   {{<<"topic1">>, 1}, _, _},
                                   {{<<"topic1">>, 0}, _, _}]}},
               handle_call({topics, Topics}, from, State)),
  ?assertEqual([{<<"topic0">>, 0},
                {<<"topic0">>, 1},
                {<<"topic0">>, 2},
                {<<"topic1">>, 0},
                {<<"topic1">>, 1}],
               kafe_consumer:topics(<<"test_cg">>)),
  kafe_consumer_store:delete(<<"test_cg">>),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer_group_sup).

update_fetchers_unchange_test() ->
  meck:new(kafe_consumer_group_sup, [passthrough]),
  meck:expect(kafe_consumer_group_sup, start_child, 11, {ok, c:pid(0, 0, 0)}),
  meck:expect(kafe_consumer_group_sup, stop_child, 1, ok),
%  meck:new(kafe_metrics, [passthrough]),
%  meck:expect(kafe_metrics, delete_consumer_partition, 3, ok),
%  meck:expect(kafe_metrics, init_consumer_partition, 3, ok),
  kafe_consumer_store:new(<<"test_cg">>),
  Topics = [{<<"topic0">>, [0, 1, 2]}, {<<"topic1">>, [0, 1]}],
  kafe_consumer_store:insert(<<"test_cg">>, topics, Topics),
  State = #state{group_id = <<"test_cg">>, topics = Topics},
  ?assertMatch({reply, ok, State},
               handle_call({topics, Topics}, from, State)),
  ?assertEqual([{<<"topic0">>, 0},
                {<<"topic0">>, 1},
                {<<"topic0">>, 2},
                {<<"topic1">>, 0},
                {<<"topic1">>, 1}],
               kafe_consumer:topics(<<"test_cg">>)),
  kafe_consumer_store:delete(<<"test_cg">>),
%  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer_group_sup).

update_fetchers_add_test() ->
  meck:new(kafe_consumer_group_sup, [passthrough]),
  meck:expect(kafe_consumer_group_sup, start_child, 11, {ok, c:pid(0, 0, 0)}),
  meck:expect(kafe_consumer_group_sup, stop_child, 1, ok),
  meck:new(kafe_metrics, [passthrough]),
%  meck:expect(kafe_metrics, delete_consumer_partition, 3, ok),
  meck:expect(kafe_metrics, init_consumer_partition, 3, ok),
  kafe_consumer_store:new(<<"test_cg">>),
  Topics = [{<<"topic0">>, [0, 1, 2]}, {<<"topic1">>, [0, 1]}],
  kafe_consumer_store:insert(<<"test_cg">>, topics, Topics),
  State = #state{group_id = <<"test_cg">>, topics = Topics},
  NewTopics = [{<<"topic0">>, [0, 1, 2]}, {<<"topic1">>, [0, 1, 2]}, {<<"topic2">>, [0, 1, 2]}],
  ?assertMatch({reply, ok,
                #state{group_id = <<"test_cg">>,
                       topics = NewTopics,
                       fetchers = [{{<<"topic0">>, 2}, _, _},
                                   {{<<"topic0">>, 1}, _, _},
                                   {{<<"topic0">>, 0}, _, _},
                                   {{<<"topic1">>, 2}, _, _},
                                   {{<<"topic1">>, 1}, _, _},
                                   {{<<"topic1">>, 0}, _, _},
                                   {{<<"topic2">>, 2}, _, _},
                                   {{<<"topic2">>, 1}, _, _},
                                   {{<<"topic2">>, 0}, _, _}]}},
               handle_call({topics, NewTopics}, from, State)),
  ?assertEqual([{<<"topic0">>, 0},
                {<<"topic0">>, 1},
                {<<"topic0">>, 2},
                {<<"topic1">>, 0},
                {<<"topic1">>, 1},
                {<<"topic1">>, 2},
                {<<"topic2">>, 0},
                {<<"topic2">>, 1},
                {<<"topic2">>, 2}],
               kafe_consumer:topics(<<"test_cg">>)),
  kafe_consumer_store:delete(<<"test_cg">>),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer_group_sup).

update_fetchers_delete_test() ->
  meck:new(kafe_consumer_group_sup, [passthrough]),
  meck:expect(kafe_consumer_group_sup, start_child, 11, {ok, c:pid(0, 0, 0)}),
  meck:expect(kafe_consumer_group_sup, stop_child, 1, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, delete_consumer_partition, 3, ok),
  meck:expect(kafe_metrics, init_consumer_partition, 3, ok),
  kafe_consumer_store:new(<<"test_cg">>),
  Topics = [{<<"topic0">>, [0, 1, 2]}, {<<"topic1">>, [0, 1, 2]}, {<<"topic2">>, [0, 1, 2]}],
  kafe_consumer_store:insert(<<"test_cg">>, topics, Topics),
  State = #state{group_id = <<"test_cg">>, topics = Topics},
  NewTopics = [{<<"topic0">>, [0, 1, 2]}, {<<"topic1">>, [0, 1]}],
  ?assertMatch({reply, ok,
                #state{group_id = <<"test_cg">>,
                       topics = NewTopics,
                       fetchers = [{{<<"topic0">>, 2}, _, _},
                                   {{<<"topic0">>, 1}, _, _},
                                   {{<<"topic0">>, 0}, _, _},
                                   {{<<"topic1">>, 1}, _, _},
                                   {{<<"topic1">>, 0}, _, _}]}},
               handle_call({topics, NewTopics}, from, State)),
  ?assertEqual([{<<"topic0">>, 0},
                {<<"topic0">>, 1},
                {<<"topic0">>, 2},
                {<<"topic1">>, 0},
                {<<"topic1">>, 1}],
               kafe_consumer:topics(<<"test_cg">>)),
  kafe_consumer_store:delete(<<"test_cg">>),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer_group_sup).

update_fetchers_update_test() ->
  meck:new(kafe_consumer_group_sup, [passthrough]),
  meck:expect(kafe_consumer_group_sup, start_child, 11, {ok, c:pid(0, 0, 0)}),
  meck:expect(kafe_consumer_group_sup, stop_child, 1, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, delete_consumer_partition, 3, ok),
  meck:expect(kafe_metrics, init_consumer_partition, 3, ok),
  kafe_consumer_store:new(<<"test_cg">>),
  Topics = [{<<"topic0">>, [0, 1, 2]}, {<<"topic1">>, [0, 1]}],
  kafe_consumer_store:insert(<<"test_cg">>, topics, Topics),
  State = #state{group_id = <<"test_cg">>, topics = Topics},
  NewTopics = [{<<"topic1">>, [0, 1, 2]}, {<<"topic2">>, [0, 1, 2]}],
  ?assertMatch({reply, ok,
                #state{group_id = <<"test_cg">>,
                       topics = NewTopics,
                       fetchers = [{{<<"topic1">>, 2}, _, _},
                                   {{<<"topic1">>, 1}, _, _},
                                   {{<<"topic1">>, 0}, _, _},
                                   {{<<"topic2">>, 2}, _, _},
                                   {{<<"topic2">>, 1}, _, _},
                                   {{<<"topic2">>, 0}, _, _}]}},
               handle_call({topics, NewTopics}, from, State)),
  ?assertEqual([{<<"topic1">>, 0},
                {<<"topic1">>, 1},
                {<<"topic1">>, 2},
                {<<"topic2">>, 0},
                {<<"topic2">>, 1},
                {<<"topic2">>, 2}],
               kafe_consumer:topics(<<"test_cg">>)),
  kafe_consumer_store:delete(<<"test_cg">>),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer_group_sup).
-endif.

