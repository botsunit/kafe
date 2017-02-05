-module(kafe_consumer_fetcher_sup).
-compile([{parse_transform, lager_transform}]).
-behaviour(supervisor).

-export([
         start_link/0
         , update_fetchers/4
         , start_child/1
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

update_fetchers(Topics, GroupID, OnAssignmentChange, StartChildArgs) ->
  case dispatch(Topics) of
    {ok, Dispatch} ->
      Fetchers0 = kafe_consumer_store:value(GroupID, fetchers, []),
      case OnAssignmentChange of
        Fun when is_function(Fun, 3) ->
          {Stop, Start} = find_assignment_change(Dispatch, Fetchers0),
          _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID, Stop, Start]) end);
        {Module, Function} when is_atom(Module),
                                is_atom(Function) ->
          case bucs:function_exists(Module, Function, 3) of
            true ->
              {Stop, Start} = find_assignment_change(Dispatch, Fetchers0),
              _ = erlang:spawn(fun() -> erlang:apply(Module, Function, [GroupID, Stop, Start]) end);
            _ ->
              ok
          end;
        _ ->
          ok
      end,
      Fetchers1 = stop_fetchers(Fetchers0, Dispatch),
      Fetchers2 = start_fetchers(Fetchers1, Dispatch, GroupID, StartChildArgs),
      kafe_consumer_store:insert(GroupID, fetchers, Fetchers2);
    {error, _} = Error ->
      Error
  end.

find_assignment_change(Dispatch, Fetchers) ->
  New = lists:foldl(fun({_, Topics}, Acc) ->
                        Acc ++ flatten_topics(Topics)
                    end, [], Dispatch),
  Old = lists:foldl(fun({_, _, Topics}, Acc) ->
                        Acc ++ flatten_topics(Topics)
                    end, [], Fetchers),
  {Old -- New, New -- Old}.
flatten_topics(Topics) ->
  lists:foldl(fun({T, P}, Acc) ->
                  lists:zip(lists:duplicate(length(P), T), P) ++ Acc
              end, [], Topics).

stop_fetchers(Fetchers, Dispatch) ->
  stop_fetchers(Fetchers, Dispatch, []).
stop_fetchers([], _, Acc) ->
  Acc;
stop_fetchers([{ID, PID, _} = Fetcher|Rest], Dispatch, Acc) ->
  case lists:keyfind(ID, 1, Dispatch) of
    false ->
      stop_child(PID),
      stop_fetchers(Rest, Dispatch, Acc);
    _ ->
      stop_fetchers(Rest, Dispatch, [Fetcher|Acc])
  end.

start_fetchers(Fetchers, Dispatch, GroupID, StartChildArgs) ->
  start_fetchers(Fetchers, Dispatch, GroupID, StartChildArgs, []).
start_fetchers(_, [], _, _, Acc) ->
  Acc;
start_fetchers(Fetchers, [{ID, Topics}|Rest], GroupID, StartChildArgs, Acc) ->
  case lists:keyfind(ID, 1, Fetchers) of
    {ID, PID, _} ->
      gen_server:cast(PID, {topics, Topics}),
      start_fetchers(Fetchers, Rest, GroupID, StartChildArgs, [{ID, PID, Topics}|Acc]);
    false ->
      case erlang:apply(?MODULE, start_child, [[GroupID, Topics|StartChildArgs]]) of
        {ok, PID} ->
          start_fetchers(Fetchers, Rest, GroupID, StartChildArgs, [{ID, PID, Topics}|Acc]);
        Other ->
          lager:error("Can't start fetcher for ~p: ~p", [Topics, Other]),
          start_fetchers(Fetchers, Rest, GroupID, StartChildArgs, Acc)
      end
  end.

stop_child(Pid) when is_pid(Pid) ->
  supervisor:terminate_child(?MODULE, Pid).

start_child(Args) ->
  case supervisor:start_child(?MODULE, Args) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [
      #{id => kafe_consumer_fetcher2, % TODO kafe_consumer_fetcher
        start => {kafe_consumer_fetcher2, start_link, []}, % TODO kafe_consumer_fetcher
        type => worker,
        shutdown => 5000}
     ]
    }}.

dispatch(Topics) ->
  dispatch(Topics, []).

dispatch([], Result) ->
  {ok, Result};
dispatch([{Topic, Partitions}|Rest], Result) when is_binary(Topic),
                                                  is_list(Partitions) ->
  case dispatch(Topic, Partitions, Result) of
    {error, _} = Error ->
      Error;
    Result0 ->
      dispatch(Rest, Result0)
  end;
dispatch([{Topic, Partition}|Rest], Result) when is_binary(Topic),
                                                 is_integer(Partition) ->
  dispatch([{Topic, [Partition]}|Rest], Result).

dispatch(_, [], Result) ->
  Result;
dispatch(Topic, [Partition|Rest], Result) ->
  case kafe_brokers:broker_id_by_topic_and_partition(Topic, Partition) of
    undefined ->
      {error, {Topic, Partition}};
    BrokerID ->
      TopicsForBroker = buclists:keyfind(BrokerID, 1, Result, []),
      PartitionsForTopic = buclists:keyfind(Topic, 1, TopicsForBroker, []),
      dispatch(
        Topic,
        Rest,
        buclists:keyupdate(
          BrokerID,
          1,
          Result,
          {BrokerID,
           buclists:keyupdate(
             Topic,
             1,
             TopicsForBroker,
             {Topic, [Partition|PartitionsForTopic]})}))
  end.

