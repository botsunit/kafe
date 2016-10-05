% @hidden
-module(kafe_consumer_fetcher_commiter_sup).
-behaviour(supervisor).

-export([start_link/10]).
-export([init/1]).

start_link(Topic, Partition, FetchInterval,
           GroupID, Autocommit, MinBytes, MaxBytes,
           MaxWaitTime, Callback, Processing) ->
  supervisor:start_link(?MODULE, [Topic, Partition, FetchInterval,
                                  GroupID, Autocommit, MinBytes, MaxBytes,
                                  MaxWaitTime, Callback, Processing]).

init([Topic, Partition, FetchInterval,
      GroupID, Autocommit, MinBytes, MaxBytes,
      MaxWaitTime, Callback, Processing]) ->
  {ok, {
    #{strategy => one_for_all,
      intensity => 1,
      period => 5},
    [
      #{id => kafe_consumer_fetcher,
        start => {kafe_consumer_fetcher, start_link, [Topic, Partition, FetchInterval,
                                                      GroupID, Autocommit, MinBytes, MaxBytes,
                                                      MaxWaitTime, Callback, Processing]},
        type => worker,
        shutdown => 5000},
      #{id => kafe_consumer_commiter,
        start => {kafe_consumer_commiter, start_link, [Topic, Partition, GroupID]},
        type => worker,
        shutdown => 5000}
    ]
  }}.
