% @author Grégoire Lejeune <gregoire.lejeune@botsunit.com>
% @copyright 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit
% @since 2014
% @doc
% A Kafka client for Erlang
%
% To create a consumer, create a function with 6 parameters :
%
% <pre>
% -module(my_consumer).
%
% -export([consume/6]).
%
% consume(GroupID, Topic, Partition, Offset, Key, Value) ->
%   % Do something with Topic/Partition/Offset/Key/Value
%   ok.
% </pre>
%
% The <tt>consume</tt> function must return <tt>ok</tt> if the message was treated, or <tt>{error, term()}</tt> on error.
%
% Then start a new consumer :
%
% <pre>
% ...
% kafe:start(),
% ...
% kafe:start_consumer(my_group, fun my_consumer:consume/6, Options),
% ...
% </pre>
%
% See {@link kafe:start_consumer/3} for the available <tt>Options</tt>.
%
% In the <tt>consume</tt> function, if you didn't start the consumer in autocommit mode (using <tt>before_processing | after_processing</tt> in the <tt>commit</tt> options),
% you need to commit manually when you have finished to treat the message. To do so, use {@link kafe_consumer:commit/4}.
%
% When you are done with your consumer, stop it :
%
% <pre>
% ...
% kafe:stop_consumer(my_group),
% ...
% </pre>
%
% You can also use a <tt>kafe_consumer_subscriber</tt> behaviour instead of a function :
%
% <pre>
% -module(my_consumer).
% -behaviour(kafe_consumer_subscriber).
% -include_lib("kafe/include/kafe_consumer.hrl").
%
% -export([init/4, handle_message/2]).
%
% -record(state, {
%                }).
%
% init(Group, Topic, Partition, Args) ->
%   % Do something with Group, Topic, Partition, Args
%   {ok, #state{}}.
%
% handle_message(Message, State) ->
%   % Do something with Message
%   % And update your State (if needed)
%   {ok, NewState}.
% </pre>
%
% Then start a new consumer :
%
% <pre>
% ...
% kafe:start().
% ...
% kafe:start_consumer(my_group, {my_consumer, Args}, Options).
% % Or
% kafe:start_consumer(my_group, my_consumer, Options).
% ...
% </pre>
%
% To commit a message (if you need to), use {@link kafe_consumer:commit/4}.
%
% <b>Internal :</b>
%
% <pre>
%                                                                 one per consumer group
%                                                       +--------------------^--------------------+
%                                                       |                                         |
%
%                                                                          +--&gt; kafe_consumer_srv +-----------------+
%                  +--&gt; kafe_consumer_sup +------so4o---&gt; kafe_consumer +--+                                        |
%                  |                                                       +--&gt; kafe_consumer_fsm                   |
%                  +--&gt; kafe_consumer_group_sup +--+                                                                m
% kafe_sup +--o4o--+                               |                                                                o
%                  +--&gt; kafe_rr                    s                                                                n
%                  |                               o                                                                |
%                  +--&gt; kafe                       4                                                                |
%                                                  o                                                                |
%                                                  |                                  +--&gt; kafe_consumer_fetcher &lt;--+
%                                                  +--&gt; kafe_consumer_tp_group_sup +--+
%                                                                                     +--&gt; kafe_consumer_committer
%                                                                                     |
%                                                                                     +--&gt; kafe_consumer_subscriber
%
%                                                     |                                                            |
%                                                     +-------------------------------v----------------------------+
%                                                                           one/{topic,partition}
%
% (o4o = one_for_one)
% (so4o = simple_one_for_one)
% (mon = monitor)
% </pre>
% @end
-module(kafe_consumer).
-include("../include/kafe.hrl").
-include("../include/kafe_consumer.hrl").
-compile([{parse_transform, lager_transform}]).
-behaviour(supervisor).

% API
-export([
         start/3
         , stop/1
         , list/0
         , commit/4
         , commit/1
         , remove_commits/3
         , remove_commits/1
         , pending_commits/3
         , pending_commits/1
         , describe/1
         , topics/1
         , member_id/1
         , generation_id/1
         , coordinator/1
        ]).

% Private
-export([
         start_link/2
         , init/1
         , can_fetch/1
        ]).

% @equiv kafe:start_consumer(GroupID, Callback, Options)
start(GroupID, Callback, Options) ->
  kafe:start_consumer(GroupID, Callback, Options).

% @equiv kafe:stop_consumer(GroupID)
stop(GroupID) ->
  kafe:stop_consumer(GroupID).

% @equiv kafe:consumer_groups()
list() ->
  kafe:consumer_groups().

% @doc
% Return consumer group descrition
% @end
-spec describe(GroupID :: binary()) -> {ok, kafe:describe_group()} | {error, term()}.
describe(GroupID) ->
  kafe:describe_group(GroupID).

% @doc
% Return the list of {topic, partition} for the consumer group
% @end
-spec topics(GroupID :: binary()) -> [{Topic :: binary(), Partition :: integer()}].
topics(GroupID) ->
  case kafe_consumer_store:lookup(GroupID, topics) of
    {ok, Topics} ->
      lists:foldl(fun({Topic, Partitions}, Acc) ->
                      Acc ++ lists:zip(
                               lists:duplicate(length(Partitions), Topic),
                               Partitions)
                  end, [], Topics);
    _ ->
      []
  end.

% @doc
% Return the consumer group generation ID
% @end
-spec generation_id(GroupID :: binary()) -> integer().
generation_id(GroupID) ->
  kafe_consumer_store:value(GroupID, generation_id).

% @doc
% Return the consumer group member ID
% @end
-spec member_id(GroupID :: binary()) -> binary().
member_id(GroupID) ->
  kafe_consumer_store:value(GroupID, member_id).

% @doc
% Return the consumer group coordinator
% @end
-spec coordinator(GroupID :: binary()) -> atom() | undefined.
coordinator(GroupID) ->
  kafe_consumer_store:value(GroupID, coordinator).

% @doc
% Commit the <tt>Offset</tt> for the given <tt>GroupID</tt>, <tt>Topic</tt> and <tt>Partition</tt>.
% @end
-spec commit(GroupID :: binary(), Topic :: binary(), Partition :: integer(), Offset :: integer()) -> ok | {error, term()}.
commit(GroupID, Topic, Partition, Offset) ->
  CommitterPID = kafe_consumer_store:value(GroupID, {commit_pid, {Topic, Partition}}),
  case erlang:is_process_alive(CommitterPID) of
    true ->
      gen_server:call(CommitterPID, {commit, Offset});
    false ->
      {error, dead_commit}
  end.

% @doc
% Commit the offset for the given message
% @end
-spec commit(Message :: kafe_consumer_subscriber:message()) -> ok | {error, term()}.
commit(#message{group_id = GroupID, topic = Topic, partition = Partition, offset = Offset}) ->
  commit(GroupID, Topic, Partition, Offset).

% @doc
% Remove pending commits for the given consumer group, topic and partition.
% @end
-spec remove_commits(GroupID :: binary(), Topic :: binary(), Partition :: integer()) -> ok | {error, Reason :: term()}.
remove_commits(GroupID, Topic, Partition) ->
  CommitterPID = kafe_consumer_store:value(GroupID, {commit_pid, {Topic, Partition}}),
  case erlang:is_process_alive(CommitterPID) of
    true ->
      gen_server:call(CommitterPID, remove_commits);
    false ->
      {error, dead_committer}
  end.

% @doc
% Remove pending commits for the given consumer group
% @end
-spec remove_commits(GroupID :: binary()) -> ok.
remove_commits(GroupID) ->
  [[remove_commits(GroupID, Topic, Partition)
    || Partition <- Partitions]
   || {Topic, Partitions} <- kafe_consumer_store:value(GroupID, topics, [])],
  ok.

% @doc
% Return the number or pending commits for the given consumer group, topic and partition.
% @end
-spec pending_commits(GroupID :: binary(), Topic :: binary(), Partition :: integer()) -> integer().
pending_commits(GroupID, Topic, Partition) ->
  CommitterPID = kafe_consumer_store:value(GroupID, {commit_pid, {Topic, Partition}}),
  case erlang:is_process_alive(CommitterPID) of
    true ->
      gen_server:call(CommitterPID, pending_commits);
    false ->
      0
  end.

% @doc
% Return the number of pending commits for the given consumer group
% @end
-spec pending_commits(GroupID :: binary()) -> ok.
pending_commits(GroupID) ->
  lists:sum(
    lists:flatten(
      [[pending_commits(GroupID, Topic, Partition)
        || Partition <- Partitions]
       || {Topic, Partitions} <- kafe_consumer_store:value(GroupID, topics, [])])).

% @hidden
start_link(GroupID, Options) ->
  supervisor:start_link({global, bucs:to_atom(GroupID)}, ?MODULE, [GroupID, Options]).

% @hidden
init([GroupID, Options]) ->
  kafe_consumer_store:new(GroupID),
  kafe_consumer_store:insert(GroupID, sup_pid, self()),
  {ok, {
     #{strategy => one_for_one,
       intensity => 1,
       period => 5},
     [
      #{id => kafe_consumer_fetcher_sup,
        start => {kafe_consumer_fetcher_sup, start_link, []},
        restart => permanent,
        type => supervisor,
        shutdown => infinity,
        modules => [kafe_consumer_fetcher_sup]},
      #{id => kafe_consumer_srv,
        start => {kafe_consumer_srv, start_link, [GroupID, Options]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafe_consumer_srv]},
      #{id => kafe_consumer_fsm,
        start => {kafe_consumer_fsm, start_link, [GroupID, Options]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafe_consumer_fsm]}
     ]
    }}.

% @hidden
can_fetch(GroupID) ->
  case kafe_consumer_store:lookup(GroupID, can_fetch) of
    {ok, true} ->
      case kafe_consumer_store:lookup(GroupID, can_fetch_fun) of
        {ok, Fun} when is_function(Fun, 0) ->
          try erlang:apply(Fun, []) of
            true -> true;
            _ -> false
          catch
            Class:Error ->
              lager:error("can_fetch function error: ~p:~p", [Class, Error]),
              false
          end;
        {ok, {Module, Function}} when is_atom(Module),
                                      is_atom(Function) ->
          case bucs:function_exists(Module, Function, 0) of
            true ->
              try erlang:apply(Module, Function, []) of
                true ->
                  true;
                _ ->
                  false
              catch
                Class:Error ->
                  lager:error("can_fetch function error: ~p:~p", [Class, Error]),
                  false
              end;
            _ ->
              ok
          end;
        _ ->
          true
      end;
    _ ->
      false
  end.

