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
% consume(CommitID, Topic, Partition, Offset, Key, Value) ->
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
% In the <tt>consume</tt> function, if you didn't start the consumer with <tt>autocommit</tt> set to <tt>true</tt>, you need to commit manually when you
% have finished to treat the message. To do so, use {@link kafe_consumer:commit/1} with the <tt>CommitID</tt> as parameter.
%
% When you are done with your consumer, stop it :
%
% <pre>
% ...
% kafe:stop_consumer(my_group),
% ...
% </pre>
%
% <b>Internal :</b>
%
% <pre>
%                                                                    one per consumer group
%                                                          +--------------------^--------------------+
%                                                          |                                         |
%
%                                                                             +--&gt; kafe_consumer_srv +------------------------+
%                  +--&gt; kafe_consumer_sup +------so4o------&gt; kafe_consumer +--+                                               |
%                  |                                                          +--&gt; kafe_consumer_fsm                          |
%                  +--&gt; kafe_consumer_fetcher_sup +--+                                                                        m
% kafe_sup +--o4o--+                                 |                                                                        o
%                  +--&gt; kafe_rr                      s                                                                        n
%                  |                                 o                                                                        |
%                  +--&gt; kafe                         4                                                                        |
%                                                    o                                                                        |
%                                                    |                                          +--&gt; kafe_consumer_fetcher &lt;--+
%                                                    +--&gt; kafe_consumer_fetcher_commiter_sup +--+
%                                                                                               +--&gt; kafe_consumer_commiter
%                                                       |                                                                  |
%                                                       +----------------------------------v-------------------------------+
%                                                                                one/{topic,partition}
%
% (o4o = one_for_one)
% (so4o = simple_one_for_one)
% (mon = monitor)
% </pre>
% @end
-module(kafe_consumer).
-include("../include/kafe.hrl").
-behaviour(supervisor).

% API
-export([
         start/3
         , stop/1
         , commit/1
         , commit/2
         , remove_commits/1
         , remove_commit/1
         , pending_commits/1
         , pending_commits/2
         , describe/1
         , topics/1
         , member_id/1
         , generation_id/1
        ]).

% Private
-export([
         start_link/2
         , init/1
         , can_fetch/1
         , store_for_commit/4
         , encode_group_commit_identifier/7
         , decode_group_commit_identifier/1
        ]).

% @equiv kafe:start_consumer(GroupID, Callback, Options)
start(GroupID, Callback, Options) ->
  kafe:start_consumer(GroupID, Callback, Options).

% @equiv kafe:stop_consumer(GroupID)
stop(GroupID) ->
  kafe:stop_consumer(GroupID).

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

% @equiv commit(GroupCommitIdentifier, #{})
-spec commit(GroupCommitIdentifier :: kafe:group_commit_identifier()) -> ok | {error, term()} | delayed.
commit(GroupCommitIdentifier) ->
  commit(GroupCommitIdentifier, #{}).

% @doc
% Commit the offset (in Kafka) for the given <tt>GroupCommitIdentifier</tt> received in the <tt>Callback</tt> specified when starting the
% consumer group (see {@link kafe:start_consumer/3}
%
% If the <tt>GroupCommitIdentifier</tt> is not the lowerest offset to commit in the group :
% <ul>
% <li>If the consumer was created with <tt>allow_unordered_commit</tt>, the commit is delayed</li>
% <li>Otherwise this function return <tt>{error, cant_commit}</tt></li>
% </ul>
%
% Available options:
%
% <ul>
% <li><tt>retry :: integer()</tt> : max retry (default 0).</li>
% <li><tt>delay :: integer()</tt> : Time (in ms) between each retry.</li>
% </ul>
% @end
-spec commit(GroupCommitIdentifier :: kafe:group_commit_identifier(), Options :: map()) -> ok | {error, term()} | delayed.
commit(GroupCommitIdentifier, Options) ->
  case decode_group_commit_identifier(GroupCommitIdentifier) of
    {Pid, Topic, Partition, Offset, GroupID, GenerationID, MemberID} ->
      gen_server:call(Pid, {commit, Topic, Partition, Offset, GroupID, GenerationID, MemberID, Options});
    _ ->
      {error, invalid_group_commit_identifier}
  end.

% @doc
% Remove all pending commits for the given consumer group.
% @end
-spec remove_commits(GroupID :: binary()) -> ok.
remove_commits(GroupID) ->
  [begin
     CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
     gen_server:call(kafe_consumer_store:value(GroupID, {commit_pid, CommitStoreKey}),
                     remove_commits)
   end || {Topic, Partition} <- topics(GroupID)],
  ok.

% @doc
% Remove the given commit
% @end
-spec remove_commit(GroupCommitIdentifier :: kafe:group_commit_identifier()) -> ok | {error, term()}.
remove_commit(GroupCommitIdentifier) ->
  case decode_group_commit_identifier(GroupCommitIdentifier) of
    {Pid, Topic, Partition, Offset, GroupID, GenerationID, MemberID} ->
      gen_server:call(Pid, {remove_commit, Topic, Partition, Offset, GroupID, GenerationID, MemberID});
    _ ->
      {error, invalid_group_commit_identifier}
  end.

% @doc
% Return the list of all pending commits for the given consumer group.
% @end
-spec pending_commits(GroupID :: binary()) -> [kafe:group_commit_identifier()].
pending_commits(GroupID) ->
  pending_commits(GroupID, topics(GroupID)).

% @doc
% Return the list of pending commits for the given topics (and partitions) for the given consumer group.
% @end
-spec pending_commits(GroupID :: binary(), [binary() | {binary(), [integer()]}]) -> [kafe:group_commit_identifier()].
pending_commits(GroupID, Topics) ->
  lists:flatten(
    [begin
       CommitStoreKey = erlang:term_to_binary(TP),
       case kafe_consumer_store:value(GroupID, {commit_pid, CommitStoreKey}) of
         undefined ->
           [];
         PID ->
           gen_server:call(PID, pending_commits)
       end
     end || TP <- Topics]).

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
      true;
    _ ->
      false
  end.

% @hidden
store_for_commit(GroupID, Topic, Partition, Offset) ->
  CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
  gen_server:call(kafe_consumer_store:value(GroupID, {commit_pid, CommitStoreKey}), {store_for_commit, Offset}).

% @hidden
encode_group_commit_identifier(Pid, Topic, Partition, Offset, GroupID, GenerationID, MemberID) ->
  base64:encode(erlang:term_to_binary({Pid, Topic, Partition, Offset, GroupID, GenerationID, MemberID}, [compressed])).

% @hidden
decode_group_commit_identifier(GroupCommitIdentifier) ->
  erlang:binary_to_term(base64:decode(GroupCommitIdentifier)).

