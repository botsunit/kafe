% @hidden
-module(kafe_consumer_fetcher).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API.
-export([start_link/10]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
          topic = undefined,
          partition = undefined,
          fetch_interval = undefined,
          timer = undefined,
          offset = -1,
          group_id = undefined,
          autocommit = ?DEFAULT_CONSUMER_AUTOCOMMIT,
          min_bytes = ?DEFAULT_FETCH_MIN_BYTES,
          max_bytes = ?DEFAULT_FETCH_MAX_BYTES,
          max_wait_time = ?DEFAULT_FETCH_MAX_WAIT_TIME,
          callback = undefined,
          processing = ?DEFAULT_CONSUMER_PROCESSING
         }).

start_link(Topic, Partition, FetchInterval,
           GroupID, Autocommit, MinBytes, MaxBytes,
           MaxWaitTime, Callback, Processing) ->
  gen_server:start_link(?MODULE, [
                                  Topic
                                  , Partition
                                  , FetchInterval
                                  , GroupID
                                  , Autocommit
                                  , MinBytes
                                  , MaxBytes
                                  , MaxWaitTime
                                  , Callback
                                  , Processing
                                 ], []).

%% gen_server.

init([Topic, Partition, FetchInterval,
      GroupID, Autocommit, MinBytes, MaxBytes,
      MaxWaitTime, Callback, Processing]) ->
  _ = erlang:process_flag(trap_exit, true),
  case kafe:offset_fetch(GroupID, [{Topic, [Partition]}]) of
    {ok, [#{name := Topic,
            partitions_offset := [#{error_code := none,
                                    offset := Offset,
                                    partition := Partition}]}]} ->
      lager:info("Start fetcher for ~p#~p with offset ~p", [Topic, Partition, Offset]),
      {ok, #state{
              topic = Topic,
              partition = Partition,
              fetch_interval = FetchInterval,
              timer = erlang:send_after(FetchInterval, self(), fetch),
              offset = Offset,
              group_id = GroupID,
              autocommit = Autocommit,
              min_bytes = MinBytes,
              max_bytes = MaxBytes,
              max_wait_time = MaxWaitTime,
              callback = Callback,
              processing = Processing
             }};
    _ ->
      lager:debug("Failed to fetch offset for ~p:~p in group ~p", [Topic, Partition, GroupID]),
      {stop, fetch_offset_faild}
  end.

handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(fetch, State) ->
  {noreply, fetch(State)};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  lager:debug("Stop fetcher ~p", [State]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

fetch(#state{fetch_interval = FetchInterval,
             topic = Topic,
             partition = Partition,
             offset = Offset,
             group_id = GroupID,
             autocommit = Autocommit,
             processing = Processing,
             min_bytes = MinBytes,
             max_bytes = MaxBytes,
             max_wait_time = MaxWaitTime,
             callback = Callback} = State) ->
  OffsetFetch = case kafe_consumer:can_fetch(GroupID) of
                  true ->
                    case kafe:offset([{Topic, [{Partition, -1, 1}]}]) of
                      {ok, [#{name := Topic,
                              partitions := [#{error_code := none,
                                               id := Partition,
                                               offsets := [MaxOffset]}]}]} ->
                        case (Offset + 1 =< MaxOffset - 1) of
                          true ->
                            OffsetOutOfRange = kafe_error:code(1),
                            case kafe:fetch(-1, Topic, #{partition => Partition,
                                                         offset => Offset + 1,
                                                         max_bytes => MaxBytes,
                                                         min_bytes => MinBytes,
                                                         max_wait_time => MaxWaitTime}) of
                              {ok, #{topics :=
                                     [#{name := Topic,
                                        partitions :=
                                        [#{error_code := ErrorCode,
                                           messages := Messages,
                                           partition := Partition}]}]}} when ErrorCode == none ->
                                kafe_metrics:consumer_messages(GroupID, length(Messages)),
                                kafe_metrics:consumer_partition_messages(GroupID, Topic, Partition, length(Messages)),
                                perform_fetch(Messages, Topic, Partition, Autocommit, Processing, GroupID, Callback, Offset, erlang:system_time(milli_seconds));
                              {ok, #{topics :=
                                     [#{name := Topic,
                                        partitions :=
                                        [#{error_code := ErrorCode,
                                           high_watermark_offset := -1,
                                           partition := Partition}]}]}} when ErrorCode == OffsetOutOfRange ->
                                % REMARK: this must never append...
                                Offset;
                              {ok, #{topics :=
                                     [#{name := Topic,
                                        partitions :=
                                        [#{error_code := ErrorCode,
                                           partition := Partition}]}]}} when ErrorCode == OffsetOutOfRange ->
                                Offset + 1; % TODO verify if we still have messages
                              {ok, #{topics :=
                                     [#{name := Topic,
                                        partitions :=
                                        [#{error_code := ErrorCode,
                                           partition := Partition}]}]}} ->
                                lager:debug("Offset #~p topic ~s:~p error: ~s", [Offset + 1, Topic, Partition, kafe_error:message(ErrorCode)]),
                                Offset;
                              Error ->
                                lager:error("Failed to fetch message #~p topic ~p:~p : ~p", [Offset + 1, Topic, Partition, Error]),
                                Offset
                            end;
                          false ->
                            Offset
                        end;
                      {ok, [#{name := Topic,
                              partitions := [#{error_code := Error}]}]} ->
                        lager:error("Get offset for ~p#~p error : ~p", [Topic, Partition, kafe_error:message(Error)]),
                        Offset;
                      {error, Error} ->
                        lager:error("Get offset for ~p#~p error : ~p", [Topic, Partition, Error]),
                        Offset
                    end;
                  false ->
                    Offset
                end,
  State#state{timer = erlang:send_after(FetchInterval, self(), fetch),
              offset = OffsetFetch}.

perform_fetch([], Topic, Partition, _, _, GroupID, _, LastOffset, Start) ->
  kafe_metrics:consumer_partition_duration(GroupID, Topic, Partition, erlang:system_time(milli_seconds) - Start),
  LastOffset;
perform_fetch([#{offset := Offset,
                 key := Key,
                 value := Value}|Messages],
              Topic, Partition,
              Autocommit, Processing,
              GroupID, Callback,
              LastOffset, Start) ->
  CommitRef = kafe_consumer:store_for_commit(GroupID, Topic, Partition, Offset),
  case commit(CommitRef, Autocommit, Processing, at_most_once) of
    {error, Reason} ->
      lager:error("[~p] Commit error for offset ~p of ~p#~p : ~p", [Processing, Offset, Topic, Partition, Reason]),
      LastOffset;
    _ ->
      lager:debug("Perform message offset ~p for ~p#~p with commit ref ~p", [Offset, Topic, Partition, CommitRef]),
      case try
             erlang:apply(Callback, [CommitRef, Topic, Partition, Offset, Key, Value])
           catch
             Class:Reason0 ->
               lager:error(
                 "Callback for message #~p of ~p#~p crash:~s",
                 [Offset, Topic, Partition, lager:pr_stacktrace(erlang:get_stacktrace(), {Class, Reason0})]),
               callback_exception
           end of
        ok ->
          case commit(CommitRef, Autocommit, Processing, at_least_once) of
            {error, Reason} ->
              lager:error("[~p] Commit error for offset ~p of ~p#~p : ~p", [Processing, Offset, Topic, Partition, Reason]),
              LastOffset;
            _ ->
              Next = erlang:system_time(milli_seconds),
              kafe_metrics:consumer_partition_duration(GroupID, Topic, Partition, Next - Start),
              perform_fetch(Messages, Topic, Partition, Autocommit, Processing, GroupID, Callback, Offset, Next)
          end;
        callback_exception ->
          LastOffset;
        {error, Error} ->
          lager:error("Callback for message #~p of ~p#~p return error : ~p", [Offset, Topic, Partition, Error]),
          LastOffset;
        Other ->
          lager:error("Callback for message #~p of ~p#~p invalid response : ~p", [Offset, Topic, Partition, Other]),
          LastOffset
      end
  end.

commit(CommitRef, true, Processing, Processing) when Processing == at_least_once;
                                                     Processing == at_most_once ->
  kafe_consumer:commit(CommitRef, #{retry => 3, delay => 1000});
commit(_, _, Processing1, Processing2) when (Processing1 == at_least_once orelse
                                             Processing1 == at_most_once)
                                            andalso
                                            (Processing2 == at_least_once orelse
                                             Processing2 == at_most_once) ->
  ok;
commit(_, _, _, _) ->
  {error, invalid_processing}.

-ifdef(TEST).
fetch_without_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [102]}]}]}
                            end),
  meck:expect(kafe, fetch, fun(_, Topic, #{partition := Partition,
                                           offset := Offset}) ->
                              {ok, #{topics =>
                                     [#{name => Topic,
                                        partitions =>
                                        [#{error_code => none,
                                           high_watermark_offset => Offset + 2,
                                           messages => [
                                                        #{offset => Offset,
                                                          key => <<"key100">>,
                                                          value => <<"value100">>},
                                                        #{offset => Offset + 1,
                                                          key => <<"key101">>,
                                                          value => <<"value101">>}
                                                       ],
                                           partition => Partition}]}]}}
                           end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_messages, 2, ok),
  meck:expect(kafe_metrics, consumer_partition_messages, 4, ok),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 101,
                      autocommit = false,
                      processing = at_least_once,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            autocommit = false,
                            processing = at_least_once,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe_metrics),
  meck:unload(kafe),
  meck:unload(kafe_consumer).

can_not_fetch_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> false end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      autocommit = false,
                      processing = at_least_once,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            autocommit = false,
                            processing = at_least_once,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe_consumer).

nothing_to_fetch_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [100]}]}]}
                            end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_messages, 2, ok),
  meck:expect(kafe_metrics, consumer_partition_messages, 4, ok),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      autocommit = false,
                      processing = at_least_once,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            autocommit = false,
                            processing = at_least_once,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe_metrics),
  meck:unload(kafe),
  meck:unload(kafe_consumer).

kafka_offset_error_on_fetch_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{_Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => unknown_topic_or_partition}]}]}
                            end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      autocommit = false,
                      processing = at_least_once,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            autocommit = false,
                            processing = at_least_once,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

offset_error_on_fetch_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{_Topic, [{_Partition, -1, 1}]}]) ->
                                {error, test_error}
                            end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      autocommit = false,
                      processing = at_least_once,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            autocommit = false,
                            processing = at_least_once,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

kafka_fetch_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [102]}]}]}
                            end),
  meck:expect(kafe, fetch, fun(_, Topic, #{partition := Partition,
                                           offset := _Offset}) ->
                              {ok, #{topics =>
                                     [#{name => Topic,
                                        partitions =>
                                        [#{error_code => unknown_topic_or_partition,
                                           partition => Partition}]}]}}
                           end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      autocommit = false,
                      processing = at_least_once,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            autocommit = false,
                            processing = at_least_once,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

kafka_fetch_offset_out_of_range_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [102]}]}]}
                            end),
  meck:expect(kafe, fetch, fun(_, Topic, #{partition := Partition,
                                           offset := _Offset}) ->
                              {ok, #{topics =>
                                     [#{name => Topic,
                                        partitions =>
                                        [#{error_code => offset_out_of_range,
                                           high_watermark_offset => -1,
                                           partition => Partition}]}]}}
                           end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      autocommit = false,
                      processing = at_least_once,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            autocommit = false,
                            processing = at_least_once,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

fetch_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [102]}]}]}
                            end),
  meck:expect(kafe, fetch, fun(_, _, _) ->
                               {error, test_error}
                           end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      autocommit = false,
                      processing = at_least_once,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            autocommit = false,
                            processing = at_least_once,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

perform_fetch_without_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 2, ok),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_most_once,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual(102,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>},
                              #{offset => 102, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_most_once,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_least_once,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual(102,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>},
                              #{offset => 102, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_least_once,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

perform_fetch_with_invalid_processing_commit_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 2, ok),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             invalid_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

perform_fetch_with_commit_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 2, {error, test_error}),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_least_once,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_most_once,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

perform_fetch_with_callback_exception_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 2, ok),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_least_once,
                             srv,
                             fun(_, _, _, _, _, <<"bad match">>) -> ok end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_most_once,
                             srv,
                             fun(_, _, _, _, _, <<"bat match">>) -> ok end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

perform_fetch_with_callback_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 2, ok),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_least_once,
                             srv,
                             fun(_, _, _, _, _, _) -> {error, test_error} end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_most_once,
                             srv,
                             fun(_, _, _, _, _, _) -> {error, test_error} end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

perform_fetch_with_invalid_callback_response_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 2, ok),
  meck:expect(kafe_consumer, store_for_commit, fun(_, _, _, Offset) ->
                                                   Offset
                                               end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_least_once,
                             srv,
                             fun(_, _, _, _, _, _) -> invalid_test_response end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             true,
                             at_most_once,
                             srv,
                             fun(_, _, _, _, _, _) -> invalid_test_response end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

commit_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 2, commit_result),

  ?assertEqual(commit_result,
               commit(ref, true, at_most_once, at_most_once)),
  ?assertEqual(commit_result,
               commit(ref, true, at_least_once, at_least_once)),
  ?assertEqual(ok,
               commit(ref, false, at_most_once, at_most_once)),
  ?assertEqual(ok,
               commit(ref, false, at_least_once, at_least_once)),
  ?assertEqual(ok,
               commit(ref, false, at_most_once, at_least_once)),
  ?assertEqual(ok,
               commit(ref, false, at_least_once, at_most_once)),
  ?assertEqual({error, invalid_processing},
               commit(ref, false, invalid_processing, at_most_once)),
  ?assertEqual({error, invalid_processing},
               commit(ref, false, invalid_processing, at_least_once)),
  ?assertEqual({error, invalid_processing},
               commit(ref, false, at_most_once, invalid_processing)),
  ?assertEqual({error, invalid_processing},
               commit(ref, false, at_least_once, invalid_processing)),
  ?assertEqual({error, invalid_processing},
               commit(ref, false, invalid_processing, invalid_processing)),

  meck:unload(kafe_consumer).
-endif.

