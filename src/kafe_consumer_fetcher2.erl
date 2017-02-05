-module(kafe_consumer_fetcher2). % TODO kafe_consumer_fetcher
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API
-export([start_link/7]).

%% gen_server callbacks
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          group_id,
          topics = [],
          from_beginning,
          min_bytes,
          max_bytes,
          max_wait_time,
          fetch_interval,
          timer
         }).

start_link(GroupID, Topics, FromBeginning,
           MinBytes, MaxBytes, MaxWaitTime,
           FetchInterval) ->
  gen_server:start_link(?MODULE, [GroupID, Topics, FromBeginning,
                                  MinBytes, MaxBytes, MaxWaitTime,
                                  FetchInterval], []).

% @hidden
init([GroupID, Topics, FromBeginning,
      MinBytes, MaxBytes, MaxWaitTime,
      FetchInterval]) ->
  case get_start_offsets(GroupID, Topics, FromBeginning, MaxBytes) of
    {ok, UpdatedTopics} ->
      {ok, #state{
              group_id = GroupID,
              topics = UpdatedTopics,
              from_beginning = FromBeginning,
              min_bytes = MinBytes,
              max_bytes = MaxBytes,
              max_wait_time = MaxWaitTime,
              fetch_interval = FetchInterval,
              timer = erlang:send_after(FetchInterval, self(), fetch)
             }};
    {error, Error} ->
      lager:error("Failed to start fetcher for ~s: ~p", [GroupID, Error]),
      {stop, ignore}
  end.

% @hidden
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

% @hidden
handle_cast({topics, Topics}, #state{topics = AssignedTopics,
                                     group_id = GroupID,
                                     from_beginning = FromBeginning,
                                     max_bytes = MaxBytes,
                                     timer = Timer} = State) ->
  {NewTopics, AssignedTopics0} = update_assigned_and_new_topics(AssignedTopics, Topics),
  case merge_assigned(NewTopics, AssignedTopics0, GroupID, FromBeginning, MaxBytes) of
    {ok, AssignedTopics1} ->
      {noreply, State#state{topics = AssignedTopics1}};
    {error, Reason} ->
      {stop, Reason, State}
  end;
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(fetch, #state{topics = Topics,
                          fetch_interval = FetchInterval} = State) ->
  % TODO
  lager:info("Fetch => ~p", [Topics]),
  timer:sleep(10000),
  % TODO
  {noreply, State#state{
              timer = erlang:send_after(FetchInterval, self(), fetch)
             }};
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, _State) ->
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


% Get the start offset for each partition of each topic in the consumer group
get_start_offsets(GroupID, Topics, FromBeginning, MaxBytes) ->
  case kafe:offset_fetch(GroupID, Topics) of
    {ok, TopicsOffsets} ->
      get_topics_offsets(TopicsOffsets, FromBeginning, MaxBytes, {[], []});
    Error ->
      Error
  end.

get_topics_offsets([], _, MaxBytes, {TopicsWithoutOffset, TopicsWithOffset}) ->
  get_offsets(TopicsWithoutOffset, TopicsWithOffset, MaxBytes);
get_topics_offsets([#{name := Topic,
                      partitions_offset := Partitions}|Rest],
                   FromBeginning,
                   MaxBytes,
                   {TopicsWithoutOffset, TopicsWithOffset}) ->
  case get_partitions_offsets(Partitions, FromBeginning, MaxBytes, {[], []}) of
    {ok, {[], []}} ->
      {error, {Topic, missing_partitions}};
    {ok, {[], WithOffset}} ->
      get_topics_offsets(Rest,
                         FromBeginning,
                         MaxBytes,
                         {TopicsWithoutOffset,
                          [{Topic, WithOffset}|TopicsWithOffset]});
    {ok, {WithoutOffset, []}} ->
      get_topics_offsets(Rest,
                         FromBeginning,
                         MaxBytes,
                         {[{Topic, WithoutOffset}|TopicsWithoutOffset],
                          TopicsWithOffset});
    {ok, {WithoutOffset, WithOffset}} ->
      get_topics_offsets(Rest,
                         FromBeginning,
                         MaxBytes,
                         {[{Topic, WithoutOffset}|TopicsWithoutOffset],
                          [{Topic, WithOffset}|TopicsWithOffset]});
    Error ->
      Error
  end.

get_partitions_offsets([], _, _, Acc) ->
  {ok, Acc};
get_partitions_offsets([#{error_code := none,
                          offset := Offset,
                          partition := Partition}|Rest],
                       FromBeginning,
                       MaxBytes,
                       {PartitionsWithoutOffset,
                        PartitionsWithOffset}) when Offset < 0 ->
  Time = case FromBeginning of
           true -> -2;
           false -> -1
         end,
  get_partitions_offsets(Rest,
                         FromBeginning,
                         MaxBytes,
                         {[{Partition, Time, 1}|PartitionsWithoutOffset],
                          PartitionsWithOffset});
get_partitions_offsets([#{error_code := none,
                          offset := Offset,
                          partition := Partition}|Rest],
                       FromBeginning,
                       MaxBytes,
                       {PartitionsWithoutOffset, PartitionsWithOffset}) ->
  get_partitions_offsets(Rest,
                         FromBeginning,
                         MaxBytes,
                         {PartitionsWithoutOffset,
                          [{Partition, Offset, MaxBytes}|PartitionsWithOffset]});
get_partitions_offsets([#{error_code := Error}|_], _, _, _) ->
  {error, Error}.

get_offsets([], Result, _) ->
  {ok, Result};
get_offsets(Topics, Result, MaxBytes) ->
  case kafe:offset(-1, Topics) of
    {ok, TopicsOffsets} ->
      get_missing_offsets(TopicsOffsets, Result, MaxBytes);
    Error ->
      Error
  end.

get_missing_offsets([], Result, _) ->
  {ok, Result};
get_missing_offsets([#{name := Topic,
                       partitions := Partitions}|Rest],
                    Result,
                    MaxBytes) ->
  case get_missing_offsets(Topic, Partitions, Result, MaxBytes) of
    {ok, Result0} ->
      get_missing_offsets(Rest, Result0, MaxBytes);
    Error ->
      Error
  end.

get_missing_offsets(_, [], Result, _) ->
  {ok, Result};
get_missing_offsets(Topic, [PartitionOffset|Rest], Result, MaxBytes) ->
  case PartitionOffset of
    #{error_code := none,
      id := Partition,
      offsets := [Offset]} ->
      get_missing_offsets(Topic,
                          Rest,
                          add_partition(Topic, Partition, Offset, MaxBytes, Result),
                          MaxBytes);
    #{error_code := none,
      id := Partition,
      offset := Offset} ->
      get_missing_offsets(Topic,
                          Rest,
                          add_partition(Topic, Partition, Offset, MaxBytes, Result),
                          MaxBytes);
    #{error_code := Error,
      id := Partition} ->
      {error, {Topic, Partition, Error}}
  end.

add_partition(Topic, Partition, Offset, MaxBytes, Result) ->
  case lists:keyfind(Topic, 1, Result) of
    {Topic, Partitions} ->
      lists:keyreplace(Topic,
                       1,
                       Result,
                       {Topic, [{Partition, Offset, MaxBytes}|Partitions]});
    false ->
      [{Topic, [{Partition, Offset, MaxBytes}]}|Result]
  end.

update_assigned_and_new_topics(AssignedTopics, Topics) ->
  update_assigned_and_new_topics(AssignedTopics, Topics, []).
update_assigned_and_new_topics([], Topics, Acc) ->
  {Topics, lists:reverse(Acc)};
update_assigned_and_new_topics([{Topic, Partitions}|Rest], Topics, Acc) ->
  case lists:keyfind(Topic, 1, Topics) of
    {Topic, AvailablePartitions} ->
      case lists:foldl(fun({Partition, _, _} = Def, {Acc0, Acc1}) ->
                           case lists:member(Partition, Acc1) of
                             true ->
                               {[Def|Acc0], lists:delete(Partition, Acc1)};
                             false ->
                               {Acc0, Acc1}
                           end
                       end, {[], AvailablePartitions}, Partitions) of
        {[], []} ->
          update_assigned_and_new_topics(Rest, lists:keydelete(Topic, 1, Topics), Acc);
        {StillAvailablePartitions, []} ->
          update_assigned_and_new_topics(
            Rest,
            lists:keydelete(Topic, 1, Topics),
            [{Topic, lists:reverse(StillAvailablePartitions)}|Acc]);
        {[], NewPartitions} ->
          update_assigned_and_new_topics(
            Rest,
            lists:keyreplace(Topic, 1, Topics, {Topic, NewPartitions}),
            Acc);
        {StillAvailablePartitions, NewPartitions} ->
          update_assigned_and_new_topics(
            Rest,
            lists:keyreplace(Topic, 1, Topics, {Topic, NewPartitions}),
            [{Topic, lists:reverse(StillAvailablePartitions)}|Acc])
      end;
    false ->
      update_assigned_and_new_topics(Rest, Topics, Acc)
  end.

merge_assigned([], AssignedTopics, _, _, _) ->
  {ok, AssignedTopics};
merge_assigned(NewTopics, AssignedTopics, GroupID, FromBeginning, MaxBytes) ->
  case get_start_offsets(GroupID, NewTopics, FromBeginning, MaxBytes) of
    {ok, UpdatedTopics} ->
      merge_assigned(UpdatedTopics, AssignedTopics);
    Error ->
      Error
  end.
merge_assigned([], AssignedTopics) ->
  {ok, AssignedTopics};
merge_assigned([{Topic, Partitions}|Rest], AssignedTopics) ->
  case lists:keyfind(Topic, 1, AssignedTopics) of
    {Topic, AssignedPartitions} ->
      merge_assigned(Rest, lists:keyreplace(Topic, 1, AssignedTopics, {Topic, Partitions ++ AssignedPartitions}));
    false ->
      merge_assigned(Rest, [{Topic, Partitions}|AssignedTopics])
  end.

