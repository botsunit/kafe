% @hidden
-module(kafe_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(CHILD(I, Type, Shutdown), {I, {I, start_link, []}, permanent, Shutdown, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, {
     #{strategy => one_for_one,
       intensity => 1,
       period => 5},
     [
      #{id => kafe_consumer_sup,
        start => {kafe_consumer_sup, start_link, []},
        restart => permanent,
        type => supervisor,
        shutdown => infinity,
        modules => [kafe_consumer_sup]},
      #{id => kafe_consumer_group_sup,
        start => {kafe_consumer_group_sup, start_link, []},
        restart => permanent,
        type => supervisor,
        shutdown => infinity,
        modules => [kafe_consumer_group_sup]},
      #{id => kafe_rr,
        start => {kafe_rr, start_link, []},
        restart => permanent,
        type => worker,
        shutdown => 5000,
        modules => [kafe_rr]},
      #{id => kafe_brokers,
        start => {kafe_brokers, start_link, []},
        restart => permanent,
        type => worker,
        shutdown => 5000,
        modules => [kafe_brokers]}
     ]}}.

