% @hidden
-module(kafe_consumer_sup).
-behaviour(supervisor).

-include("../include/kafe.hrl").

-export([
         start_link/0
         , start_child/2
         , stop_child/1
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(GroupID, Options) ->
  kafe_metrics:init_consumer(GroupID),
  case kafe_consumer_store:lookup(GroupID, sup_pid) of
    {ok, PID} ->
      {ok, PID};
    _ ->
      case supervisor:start_child(?MODULE, [GroupID, Options]) of
        {ok, Child, _} -> {ok, Child};
        Other -> Other
      end
  end.

stop_child(GroupPID) when is_pid(GroupPID) ->
  supervisor:terminate_child(?MODULE, GroupPID);
stop_child(GroupID) ->
  kafe_metrics:delete_consumer(GroupID),
  case kafe_consumer_store:lookup(GroupID, sup_pid) of
    {ok, PID} ->
      stop_child(PID);
    _ ->
      {error, detached}
  end.

init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [#{id => kafe_consumer,
        start => {kafe_consumer, start_link, []},
        type => supervisor,
        shutdown => 5000}]
    }}.

