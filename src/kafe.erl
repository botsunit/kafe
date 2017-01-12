% @author Grégoire Lejeune <gl@finexkap.com>
% @author Grégoire Lejeune <greg@g-corp.io>
% @author Grégoire Lejeune <gregoire.lejeune@botsunit.com>
% @copyright 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit
% @since 2014
% @doc
% A Kafka client for Erlang
%
% This module only implement the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol">Kafak Protocol</a>.
% @end
-module(kafe).
-compile([{parse_transform, bristow_transform},
          {parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-include_lib("kernel/include/inet.hrl").
-define(SERVER, ?MODULE).

% Public API
-export([
         start/0,
         brokers/0,
         metadata/0,
         metadata/1,
         offset/0,
         offset/1,
         offset/2,
         produce/2,
         produce/3,
         default_key_to_partition/2,
         fetch/1,
         fetch/2,
         fetch/3,
         list_groups/0,
         list_groups/1,
         group_coordinator/1,
         join_group/1,
         join_group/2,
         sync_group/4,
         heartbeat/3,
         leave_group/2,
         describe_group/1,
         default_protocol/4,
         offset_fetch/1,
         offset_fetch/2,
         offset_commit/2,
         offset_commit/4,
         offset_commit/5
        ]).

-export([
         start_consumer/3,
         stop_consumer/1,
         offsets/2,
         offsets/3
        ]).

% Internal API
-export([
         number_of_brokers/0,
         start_link/0,
         first_broker/0,
         release_broker/1,
         broker_id_by_topic_and_partition/2,
         broker_by_name/1,
         broker_by_host_and_port/2,
         broker_by_id/1,
         topics/0,
         partitions/1,
         max_offset/1,
         max_offset/2,
         partition_for_offset/2,
         api_version/0,
         update_brokers/0,
         state/0
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([describe_group/0, group_commit_identifier/0]).

-type error_code() :: no_error
| unknown
| offset_out_of_range
| invalid_message
| unknown_topic_or_partition
| invalid_message_size
| leader_not_available
| not_leader_for_partition
| request_timed_out
| broker_not_available
| replica_not_available
| message_size_too_large
| stale_controller_epoch
| offset_metadata_too_large
| offsets_load_in_progress
| consumer_coordinator_not_available
| not_coordinator_for_consumer.
-type metadata() :: #{brokers => [#{host => binary(),
                                    id => integer(),
                                    port => port()}],
                      topics => [#{error_code => error_code(),
                                   name => binary(),
                                   partitions => [#{error_code => error_code(),
                                                    id => integer(),
                                                    isr => [integer()],
                                                    leader => integer(),
                                                    replicas => [integer()]}]}]}.
-type topics() :: [binary() | string() | atom()] | [{binary() | string() | atom(), [{integer(), integer(), integer()}]}].
-type topic_partition_info() :: #{name => binary(),
                                  partitions => [#{error_code => error_code(), id => integer(), offsets => [integer()]}]}.
-type message() :: binary() | {binary(), binary()}.
-type produce_options() :: #{timeout => integer(),
                             required_acks => integer(),
                             partition => integer(),
                             key_to_partition => fun((binary(), term()) -> integer())}.
-type fetch_options() :: #{partition => integer(),
                           offset => integer(),
                           max_bytes => integer(),
                           min_bytes => integer(),
                           max_wait_time => integer(),
                           retrieve => first | all}.
-type message_set() :: #{name => binary(),
                         partitions => [#{partition => integer(),
                                          error_code => error_code(),
                                          high_watermark_offset => integer(),
                                          messages => [#{offset => integer(),
                                                        crc => integer(),
                                                        magic_byte => 0 | 1,
                                                        attributes => integer(),
                                                        timestamp => integer(),
                                                        key => binary(),
                                                        value => binary()}]}]}.
-type group_coordinator() :: #{error_code => error_code(),
                               coordinator_id => integer(),
                               coordinator_host => binary(),
                               coordinator_port => port()}.
-type offset_fetch_options() :: [binary()] | [{binary(), [integer()]}].
-type offset_fetch_set() :: #{name => binary(),
                              partitions_offset => [#{partition => integer(),
                                                      offset => integer(),
                                                      metadata_info => binary(),
                                                      error_code => error_code()}]}.
-type offset_commit_set() :: [#{name => binary(),
                                partitions => [#{partition => integer(),
                                                 error_code => error_code()}]}].
-type offset_commit_option() :: [{binary(), [{integer(), integer(), binary()}]}].
-type offset_commit_option_v1() :: [{binary(), [{integer(), integer(), integer(), binary()}]}].
-type broker_id() :: atom().
-type group() :: #{group_id => binary(), protocol_type => binary()}.
-type groups() :: #{error_code => error_code(),
                    groups => [group()]}.
-type groups_list() :: [#{broker => broker_id(),
                          groups => groups()}].
-type group_member() :: #{member_id => binary(),
                          member_metadata => binary()}.
-type group_join() :: #{error_code => error_code(),
                        generation_id => integer(),
                        protocol_group => binary(),
                        leader_id => binary(),
                        member_id => binary(),
                        members => [group_member()]}.
-type protocol() :: binary().
-type join_group_options() :: #{session_timeout => integer(),
                                member_id => binary(),
                                protocol_type => binary(),
                                protocols => [protocol()]}.
-type partition_assignment() :: #{topic => binary(),
                                  partitions => [integer()]}.
-type member_assignment() :: #{version => integer(),
                               partition_assignment => [partition_assignment()],
                               user_data => binary()}.
-type group_assignment() :: #{member_id => binary(),
                              member_assignment => member_assignment()}.
-type sync_group() :: #{error_code => error_code(),
                        version => integer(),
                        partition_assignment => [partition_assignment()],
                        user_data => binary()}.
-type response_code() :: #{error_code => error_code()}.
-type group_member_ex() :: #{client_host => binary(),
                             client_id => binary(),
                             member_id => binary(),
                             member_metadata => binary(),
                             member_assignment => member_assignment()}.
-type describe_group() :: [#{error_code => error_code(),
                             group_id => binary(),
                             members => [group_member_ex()],
                             protocol => binary(),
                             protocol_type => binary(),
                             state => binary()}].
-type consumer_options() :: #{session_timeout => integer(),
                              member_id => binary(),
                              topics => [binary() | {binary(), [integer()]}],
                              fetch_interval => integer(),
                              fetch_size => integer(),
                              max_bytes => integer(),
                              min_bytes => integer(),
                              max_wait_time => integer(),
                              on_start_fetching => fun((binary()) -> any()) | {atom(), atom()} | undefined,
                              on_stop_fetching => fun((binary()) -> any()) | {atom(), atom()} | undefined,
                              on_assignment_change => fun((binary(), [{binary(), integer()}], [{binary(), integer()}]) -> any()) | {atom(), atom()} | undefined,
                              can_fetch => fun(() -> true | false) | {atom(), atom()} | undefined,
                              from_beginning => true | false,
                              commit => [commit()]}.
-type commit() :: processing() | {interval, integer()} | {message, integer()}.
-type processing() :: before_processing | after_processing.
-type group_commit_identifier() :: binary().

% @hidden
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

% @hidden
first_broker() ->
  gen_server:call(?SERVER, first_broker, ?TIMEOUT).

% @hidden
number_of_brokers() ->
  gen_server:call(?SERVER, number_of_brokers, ?TIMEOUT).

% @hidden
release_broker(Broker) ->
  case poolgirl:checkin(Broker) of
    ok -> ok;
    {error, Error} ->
      lager:error("Checkin broker ~p failed: ~p", [Broker, Error]),
      ok
  end.

% @hidden
broker_id_by_topic_and_partition(Topic, Partition) ->
  gen_server:call(?SERVER, {broker_id_by_topic_and_partition, bucs:to_binary(Topic), Partition}, ?TIMEOUT).

% @hidden
broker_by_name(BrokerName) ->
  gen_server:call(?SERVER, {broker_by_name, BrokerName}, ?TIMEOUT).

% @hidden
broker_by_host_and_port(Host, Port) ->
  broker_by_name(kafe_utils:broker_name(Host, Port)).

% @hidden
broker_by_id(BrokerID) ->
  case poolgirl:checkout(BrokerID) of
    {ok, BrokerPID} -> BrokerPID;
    _ -> undefined
  end.

% @hidden
topics() ->
  gen_server:call(?SERVER, topics, ?TIMEOUT).

% @hidden
partitions(Topic) ->
  gen_server:call(?SERVER, {partitions, Topic}, ?TIMEOUT).

% @hidden
max_offset(TopicName) ->
  case offset([TopicName]) of
    {ok, [#{partitions := Partitions}]} ->
      lists:foldl(fun(#{id := P, offsets := [O|_]}, {_, Offset} = Acc) ->
                      if
                        O > Offset -> {P, O};
                        true -> Acc
                      end
                  end, {?DEFAULT_OFFSET_PARTITION, 0}, Partitions);
    {ok, _} ->
      {?DEFAULT_OFFSET_PARTITION, 0}
  end.

% @hidden
max_offset(TopicName, Partition) ->
  case offset([{TopicName, [{Partition, ?DEFAULT_OFFSET_TIMESTAMP, ?DEFAULT_OFFSET_MAX_NUM_OFFSETS}]}]) of
    {ok,
     [#{partitions := [#{id := Partition,
                         offsets := [Offset|_]}]}]
    } ->
      {Partition, Offset};
    {ok, _} ->
      {Partition, 0}
  end.

% @hidden
partition_for_offset(TopicName, Offset) ->
  case offset([TopicName]) of
    {ok, [#{partitions := Partitions}]} ->
      lists:foldl(fun(#{id := P, offsets := [O|_]}, {_, Offset1} = Acc) ->
                      if
                        O >= Offset1 -> {P, Offset1};
                        true -> Acc
                      end
                  end, {0, Offset}, Partitions);
    {ok, _} ->
      {?DEFAULT_OFFSET_PARTITION, Offset}
  end.

% @hidden
update_brokers() ->
  gen_server:cast(?SERVER, update_brokers).

% @hidden
api_version() ->
  gen_server:call(?SERVER, api_version, ?TIMEOUT).

% @hidden
state() ->
  gen_server:call(?SERVER, state, ?TIMEOUT).

% @doc
% Start kafe application
% @end
start() ->
  application:ensure_all_started(?MODULE).

% @doc
% Return the list of availables brokers
% @end
brokers() ->
  gen_server:call(?SERVER, brokers, ?TIMEOUT).

% @equiv metadata([])
metadata() ->
  metadata([]).

% @doc
% Return metadata for the given topics
%
% Example:
% <pre>
% Metadata = kafe:metadata([&lt;&lt;"topic1"&gt;&gt;, &lt;&lt;"topic2"&gt;&gt;]).
% </pre>
%
% This example return all metadata for <tt>topic1</tt> and <tt>topic2</tt>
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TopicMetadataRequest">Kafka protocol documentation</a>.
% @end
-spec metadata([binary()|string()|atom()]) -> {ok, metadata()} | {error, term()}.
metadata(Topics) when is_list(Topics) ->
  kafe_protocol_metadata:run(Topics).

% @equiv offset(-1, [])
offset() ->
  offset(-1, []).

% @equiv offset(-1, Topics)
offset(Topics) when is_list(Topics) ->
  offset(-1, Topics).

% @doc
% Get offet for the given topics and replicat
%
% Example:
% <pre>
% Offset = kafe:offet(-1, [&lt;&lt;"topic1"&gt;&gt;, {&lt;&lt;"topic2"&gt;&gt;, [{0, -1, 1}, {2, -1, 1}]}]).
% </pre>
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest">Kafka protocol documentation</a>.
% @end
-spec offset(integer(), topics()) -> {ok, [topic_partition_info()]} | {error, term()}.
offset(ReplicatID, Topics) when is_integer(ReplicatID), is_list(Topics) ->
  kafe_protocol_offset:run(ReplicatID, Topics).

% @equiv produce(Topic, Message, #{})
produce(Topic, Message) ->
  produce(Topic, Message, #{}).

% @doc
% Send a message
%
% Options:
% <ul>
% <li><tt>timeout :: integer()</tt> : This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in
% RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include network latency, (2) the timer begins at the
% beginning of the processing of this request so if many requests are queued due to server overload that wait time will not be included, (3) we will not
% terminate a local write so if the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client should use the
% socket timeout. (default: 5000)</li>
% <li><tt>required_acks :: integer()</tt> : This field indicates how many acknowledgements the servers should receive before responding to the request. If it is
% 0 the server will not send any response (this is the only case where the server will not reply to a request) and this function will return ok.
% If it is 1, the server will wait the data is written to the local log before sending a response. If it is -1 the server will block until the message is committed
% by all in sync replicas before sending a response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server
% will never wait for more acknowledgements than there are in-sync replicas). (default: -1)</li>
% <li><tt>partition :: integer()</tt> : The partition that data is being published to.</li>
% <li><tt>key_to_partition :: fun((binary(), term()) -&gt; integer())</tt> : Hash function to do partition assignment from the message key. (default:
% kafe:default_key_to_partition/2)</li>
% </ul>
%
% If the partition is specified (option <tt>partition</tt>) and there is a message' key, the message will be produce on the specified partition. If no partition
% is specified, and there is a message key, the partition will be calculated using the <tt>key_to_partition</tt> function (or an internal function if this
% option is not specified). If there is no key and no partition specified, the partition will be choosen using a round robin algorithm.
%
% Example:
% <pre>
% Response = kafe:product(&lt;&lt;"topic"&gt;&gt;, &lt;&lt;"a simple message"&gt;&gt;, #{timeout =&gt; 1000, partition =&gt; 0}).
% Response1 = kafe:product(&lt;&lt;"topic"&gt;&gt;, {&lt;&lt;"key"&gt;&gt;, &lt;&lt;"Another simple message"&gt;&gt;}).
% </pre>
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceAPI">Kafka protocol documentation</a>.
% @end
-spec produce(binary(), message(), produce_options()) -> ok | {ok, [topic_partition_info()]} | {error,  term()}.
produce(Topic, Message, Options) ->
  kafe_protocol_produce:run(Topic, Message, Options).

% @doc
% Default fonction used to do partition assignment from the message key.
% @end
-spec default_key_to_partition(Topic :: binary(), Key :: term()) -> integer().
default_key_to_partition(Topic, Key) ->
  erlang:crc32(term_to_binary(Key)) rem erlang:length(kafe:partitions(Topic)).

% @equiv fetch(-1, TopicName, #{})
fetch(TopicName) when is_binary(TopicName) orelse is_list(TopicName) orelse is_atom(TopicName) ->
  fetch(-1, TopicName, #{}).

% @equiv fetch(ReplicatID, TopicName, #{})
fetch(ReplicatID, TopicName) when is_integer(ReplicatID), (is_binary(TopicName) orelse is_list(TopicName) orelse is_atom(TopicName)) ->
  fetch(ReplicatID, TopicName, #{});
% @equiv fetch(-1, TopicName, Options)
fetch(TopicName, Options) when is_map(Options), (is_binary(TopicName) orelse is_list(TopicName) orelse is_atom(TopicName)) ->
  fetch(-1, TopicName, Options).


% @doc
% Fetch messages
%
% Options:
% <ul>
% <li><tt>partition :: integer()</tt> : The id of the partition the fetch is for (default : partition with the highiest offset).</li>
% <li><tt>offset :: integer()</tt> : The offset to begin this fetch from (default : next offset for the partition)</li>
% <li><tt>max_bytes :: integer()</tt> : The maximum bytes to include in the message set for this partition. This helps bound the size of the response (default :
% 1024*1024)</li>
% <li><tt>min_bytes :: integer()</tt> : This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0
% the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is
% set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in
% combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting
% MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding) (default :
% 1).</li>
% <li><tt>max_wait_time :: integer()</tt> : The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available
% at the time the request is issued (default : 100).</li>
% <li><tt>retrieve :: all | first</tt> : if the Kafka's response buffer contains more than one complete message ; with <tt>first</tt> we will ignore the
% remaining data ; with <tt>all</tt> we will parse all complete messages in the buffer (default : first).</li>
% </ul>
%
% ReplicatID must <b>always</b> be -1.
%
% Example:
% <pre>
% Response = kafe:fetch(&lt;&lt;"topic"&gt;&gt;)
% Response1 = kafe:fetch(&lt;&lt;"topic"&gt;&gt;, #{offset =&gt; 2, partition =&gt; 3}).
% </pre>
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI">Kafka protocol documentation</a>.
% @end
-spec fetch(integer(), binary(), fetch_options()) -> {ok, [message_set()]} | {ok, #{topics => [message_set()], throttle_time => integer()}} | {error, term()}.
fetch(ReplicatID, TopicName, Options) when is_integer(ReplicatID), (is_binary(TopicName) orelse is_list(TopicName) orelse is_atom(TopicName)), is_map(Options) ->
  case kafe_protocol_fetch:run(ReplicatID, TopicName, Options) of
    {ok, #{topics :=
           [#{partitions :=
              [#{error_code := ErrorCode}]}]}} = Result when ErrorCode =:= not_leader_for_partition ->
      update_brokers(),
      Result;
    Other ->
      Other
  end.

% @doc
% Find groups managed by all brokers.
% @end
-spec list_groups() -> {ok, groups_list()} | {error, term()}.
list_groups() ->
  {ok, lists:map(fun(Broker) ->
                     case list_groups(Broker) of
                       {ok, Groups} ->
                         #{broker => Broker,
                           groups => Groups};
                       _ ->
                         #{broker => Broker,
                           groups => #{error_code => kafe_error:code(8),
                                       groups => []}}
                     end
                 end, brokers())}.

% @doc
% Find groups managed by a broker.
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ListGroupsRequest">Kafka protocol documentation</a>
% @end
-spec list_groups(Broker :: broker_id()) -> {ok, groups()} | {error, term()}.
list_groups(Broker) when is_atom(Broker) ->
  kafe_protocol_list_groups:run(Broker).

% @doc
% Group coordinator Request
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ConsumerMetadataRequest">Kafka protocol documentation</a>.
%
% For compatibility, this function as an alias : <tt>consumer_metadata</tt>.
% @end
-spec group_coordinator(binary()) -> {ok, group_coordinator()} | {error,  term()}.
group_coordinator(ConsumerGroup) ->
  kafe_protocol_group_coordinator:run(ConsumerGroup).

-alias consumer_metadata.

% @equiv join_group(GroupID, #{})
join_group(GroupID) ->
  join_group(GroupID, #{}).

% @doc
% Join Group
%
% Options:
% <ul>
% <li><tt>session_timeout :: integer()</tt> : The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms. (default: 10000)</li>
% <li><tt>member_id :: binary()</tt> : The assigned consumer id or an empty string for a new consumer. When a member first joins the group, the memberID must be
% empty (i.e. &lt;&lt;&gt;&gt;, default), but a rejoining member should use the same memberID from the previous generation.</li>
% <li><tt>protocol_type :: binary()</tt> : Unique name for class of protocols implemented by group (default &lt;&lt;"consumer"&gt;&gt;).</li>
% <li><tt>protocols :: [protocol()]</tt> : List of protocols.</li>
% </ul>
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-JoinGroupRequest">Kafka protocol documentation</a>.
% @end
-spec join_group(binary(), join_group_options()) -> {error, term()} | {ok, group_join()}.
join_group(GroupID, Options) ->
  kafe_protocol_join_group:run(GroupID, Options).

% @doc
% Create a default protocol as defined in the <a
% href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-JoinGroupRequest">Kafka Protocol Guide</a>.
% @end
-spec default_protocol(Name :: binary(), Version :: integer(), Topics :: topics(), UserData :: binary()) -> protocol().
default_protocol(Name, Version, Topics, UserData) when is_binary(Name),
                                                       is_integer(Version),
                                                       is_list(Topics),
                                                       is_binary(UserData) ->
  EncodedTopics = lists:map(fun(E) ->
                                kafe_protocol:encode_string(bucs:to_binary(E))
                            end, Topics),
  <<(kafe_protocol:encode_string(Name))/binary,
    Version:16/signed,
    (kafe_protocol:encode_array(EncodedTopics))/binary,
    (kafe_protocol:encode_bytes(UserData))/binary>>.

% @doc
% The sync group request is used by the group leader to assign state (e.g. partition assignments) to all members of the current generation. All members send
% SyncGroup immediately after joining the group, but only the leader provides the group's assignment.
%
% Example:
%
% <pre>
% kafe:sync_group(&lt;&lt;"my_group"&gt;&gt;, 1, &lt;&lt;"kafka-6dbb08f4-a0dc-4f4c-a0b9-dccb4d03ff2c"&gt;&gt;,
%                 [#{member_id =&gt; &lt;&lt;"kafka-6dbb08f4-a0dc-4f4c-a0b9-dccb4d03ff2c"&gt;&gt;,
%                    member_assignment =&gt; #{version =&gt; 0,
%                                           user_data =&gt; &lt;&lt;"my user data"&gt;&gt;,
%                                           partition_assignment =&gt; [#{topic =&gt; &lt;&lt;"topic0"&gt;&gt;,
%                                                                      partitions =&gt; [0, 1, 2]},
%                                                                    #{topic =&gt; &lt;&lt;"topic1"&gt;&gt;,
%                                                                      partitions =&gt; [0, 1, 2]}]}},
%                  #{member_id =&gt; &lt;&lt;"kafka-0b7e179d-3ff9-46d2-b652-e0d041e4264a"&gt;&gt;,
%                    member_assignment =&gt; #{version =&gt; 0,
%                                           user_data =&gt; &lt;&lt;"my user data"&gt;&gt;,
%                                           partition_assignment =&gt; [#{topic =&gt; &lt;&lt;"topic0"&gt;&gt;,
%                                                                      partitions =&gt; [0, 1, 2]},
%                                                                    #{topic =&gt; &lt;&lt;"topic1"&gt;&gt;,
%                                                                      partitions =&gt; [0, 1, 2]}]}}]).
% </pre>
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-SyncGroupRequest">Kafka protocol documentation</a>.
% @end
-spec sync_group(binary(), integer(), binary(), [group_assignment()]) -> {error, term()} | {ok, sync_group()}.
sync_group(GroupID, GenerationID, MemberID, Assignments) ->
  kafe_protocol_sync_group:run(GroupID, GenerationID, MemberID, Assignments).

% @doc
% Once a member has joined and synced, it will begin sending periodic heartbeats to keep itself in the group. If not heartbeat has been received by the
% coordinator with the configured session timeout, the member will be kicked out of the group.
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-HeartbeatRequest">Kafka protocol documentation</a>.
% @end
-spec heartbeat(binary(), integer(), binary()) -> {error, term()} | {ok, response_code()}.
heartbeat(GroupID, GenerationID, MemberID) ->
  kafe_protocol_heartbeat:run(GroupID, GenerationID, MemberID).

% @doc
% To explicitly leave a group, the client can send a leave group request. This is preferred over letting the session timeout expire since it allows the group to
% rebalance faster, which for the consumer means that less time will elapse before partitions can be reassigned to an active member.
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-LeaveGroupRequest">Kafka protocol documentation</a>.
% @end
-spec leave_group(binary(), binary()) -> {error, term()} | {ok, response_code()}.
leave_group(GroupID, MemberID) ->
  kafe_protocol_leave_group:run(GroupID, MemberID).

% @doc
% Return the description of the given consumer group.
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-DescribeGroupsRequest">Kafka protocol documentation</a>.
% @end
-spec describe_group(binary()) -> {error, term()} | {ok, describe_group()}.
describe_group(GroupID) when is_binary(GroupID) ->
  kafe_protocol_describe_group:run(GroupID).

% @doc
% Offset commit v0
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest">Kafka protocol documentation</a>.
% @end
-spec offset_commit(binary(), offset_commit_option()) -> {ok, [offset_commit_set()]} | {error, term()}.
offset_commit(ConsumerGroup, Topics) ->
  kafe_protocol_consumer_offset_commit:run_v0(ConsumerGroup, Topics).

% @doc
% Offset commit v1
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest">Kafka protocol documentation</a>.
% @end
-spec offset_commit(binary(), integer(), binary(), offset_commit_option_v1()) -> {ok, [offset_commit_set()]} | {error, term()}.
offset_commit(ConsumerGroup, ConsumerGroupGenerationID, ConsumerID, Topics) ->
  kafe_protocol_consumer_offset_commit:run_v1(ConsumerGroup,
                                              ConsumerGroupGenerationID,
                                              ConsumerID,
                                              Topics).

% @doc
% Offset commit v2
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest">Kafka protocol documentation</a>.
% @end
-spec offset_commit(binary(), integer(), binary(), integer(), offset_commit_option()) -> {ok, [offset_commit_set()]} | {error, term()}.
offset_commit(ConsumerGroup, ConsumerGroupGenerationID, ConsumerID, RetentionTime, Topics) ->
  kafe_protocol_consumer_offset_commit:run_v2(ConsumerGroup,
                                              ConsumerGroupGenerationID,
                                              ConsumerID,
                                              RetentionTime,
                                              Topics).

% @equiv offset_fetch(ConsumerGroup, [])
-spec offset_fetch(binary()) -> {ok, [offset_fetch_set()]}.
offset_fetch(ConsumerGroup) ->
  offset_fetch(ConsumerGroup, []).

% @doc
% Offset fetch
%
% For more informations, see the
% <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest">Kafka protocol documentation</a>.
% @end
-spec offset_fetch(binary(), offset_fetch_options()) -> {ok, [offset_fetch_set()]} | {error, term()}.
offset_fetch(ConsumerGroup, Options) when is_binary(ConsumerGroup), is_list(Options) ->
  kafe_protocol_consumer_offset_fetch:run(ConsumerGroup, Options);
offset_fetch(ConsumerGroup, Options) when is_list(Options) ->
  offset_fetch(bucs:to_binary(ConsumerGroup), Options).

% @doc
% Return the list of the next Nth unread offsets for a given topic and consumer group
% @end
-spec offsets(binary() | {binary(), [integer()]}, binary(), integer()) -> [{integer(), integer()}] | error.
offsets(TopicName, ConsumerGroup, Nth) when is_binary(TopicName) ->
  offsets({TopicName, partitions(TopicName)}, ConsumerGroup, Nth);
offsets({TopicName, PartitionsList}, ConsumerGroup, Nth) ->
  case offset([TopicName]) of
    {ok, [#{name := TopicName, partitions := Partitions}]} ->
      {Offsets, PartitionsID} = lists:foldl(fun
                                              (#{id := PartitionID,
                                                 offsets := [Offset|_],
                                                 error_code := none},
                                               {AccOffs, AccParts} = Acc) ->
                                                case lists:member(PartitionID, PartitionsList) of
                                                  true ->
                                                    {[{PartitionID, Offset - 1}|AccOffs], [PartitionID|AccParts]};
                                                  false ->
                                                    Acc
                                                end;
                                              (_, Acc) ->
                                                Acc
                                            end, {[], []}, Partitions),
      case offset_fetch(ConsumerGroup, [{TopicName, PartitionsID}]) of
        {ok, [#{name := TopicName, partitions_offset := PartitionsOffset}]} ->
          CurrentOffsets = lists:foldl(fun
                                         (#{offset := Offset1,
                                            partition := PartitionID1},
                                          Acc1) ->
                                           [{PartitionID1, Offset1 + 1}|Acc1];
                                         (_, Acc1) ->
                                           Acc1
                                       end, [], PartitionsOffset),
          CombinedOffsets = lists:foldl(fun({P, O}, Acc) ->
                                            case  lists:keyfind(P, 1, CurrentOffsets) of
                                              {P, C} when C =< O -> [{P, O, C}|Acc];
                                              _ -> Acc
                                            end
                                        end, [], Offsets),
          lager:debug("Offsets = ~p / CurrentOffsets = ~p / CombinedOffsets = ~p", [Offsets, CurrentOffsets, CombinedOffsets]),
          {NewOffsets, Result} = get_offsets_list(CombinedOffsets, [], [], Nth),
          lists:foldl(fun({PartitionID, NewOffset}, Acc) ->
                          case offset_commit(ConsumerGroup,
                                             [{TopicName, [{PartitionID, NewOffset, <<>>}]}]) of
                            {ok, [#{name := TopicName,
                                    partitions := [#{partition := PartitionID,
                                                     error_code := none}]}]} ->
                              Acc;
                            _ ->
                              delete_offset_for_partition(PartitionID, Acc)
                          end
                      end, Result, NewOffsets);
        _ ->
          lager:error("Can't retrieve offsets for consumer group ~s on topic ~s", [ConsumerGroup, TopicName]),
          error
      end;
    _ ->
      lager:error("Can't retrieve offsets for topic ~s", [TopicName]),
      error
  end.

% @doc
% Return the list of all unread offsets for a given topic and consumer group
% @end
-spec offsets(binary(), binary()) -> [{integer(), integer()}] | error.
offsets(TopicName, ConsumerGroup) ->
  offsets(TopicName, ConsumerGroup, -1).

get_offsets_list(Offsets, Result, Final, Nth) when Offsets =/= [], length(Result) =/= Nth ->
  [{PartitionID, MaxOffset, CurrentOffset}|SortedOffsets] = lists:sort(fun({_, O1, C1}, {_, O2, C2}) -> (C1 < C2) and (O1 < O2) end, Offsets),
  Offsets1 = if
               CurrentOffset + 1 > MaxOffset -> SortedOffsets;
               true  -> [{PartitionID, MaxOffset, CurrentOffset + 1}|SortedOffsets]
             end,
  Final1 = lists:keystore(PartitionID, 1, Final, {PartitionID, CurrentOffset}),
  get_offsets_list(Offsets1, [{PartitionID, CurrentOffset}|Result], Final1, Nth);
get_offsets_list(_, Result, Final, _) -> {Final, lists:reverse(Result)}.

delete_offset_for_partition(PartitionID, Offsets) ->
  case lists:keyfind(PartitionID, 1, Offsets) of
    false -> Offsets;
    _ -> delete_offset_for_partition(PartitionID, lists:keydelete(PartitionID, 1, Offsets))
  end.

% @doc
% Start a new consumer.
%
% Options:
% <ul>
% <li><tt>session_timeout :: integer()</tt> : The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms. (default: 10000)</li>
% <li><tt>member_id :: binary()</tt> : The assigned consumer id or an empty string for a new consumer. When a member first joins the group, the memberID must be
% empty (i.e. &lt;&lt;&gt;&gt;, default), but a rejoining member should use the same memberID from the previous generation.</li>
% <li><tt>topics :: [binary() | {binary(), [integer()]}]</tt> : List or topics (and partitions).</li>
% <li><tt>fetch_interval :: integer()</tt> : Fetch interval in ms (default : 10)</li>
% <li><tt>max_bytes :: integer()</tt> : The maximum bytes to include in the message set for this partition. This helps bound the size of the response (default :
% 1024*1024)</li>
% <li><tt>min_bytes :: integer()</tt> : This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0
% the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is
% set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in
% combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting
% MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding) (default :
% 1).</li>
% <li><tt>max_wait_time :: integer()</tt> : The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available
% at the time the request is issued (default : 100).</li>
% <li><tt>commit :: commit()</tt> : Commit configuration (default: [after_processing, {interval, 1000}]).</li>
% <li><tt>on_start_fetching :: fun((GroupID :: binary()) -> any()) | {atom(), atom()}</tt> : Function called when the fetcher start/restart fetching. (default: undefined).</li>
% <li><tt>on_stop_fetching :: fun((GroupID :: binary()) -> any()) | {atom(), atom()}</tt> : Function called when the fetcher stop fetching. (default: undefined).</li>
% <li><tt>can_fetch :: fun(() -> true | false) | {atom(), atom()}</tt> : Messages are fetched, only if this function returns <tt>true</tt> or is undefined.
% (default: undefined).</li>
% <li><tt>on_assignment_change :: fun((GroupID :: binary(), [{binary(), integer()}], [{binary(), integer()}]) -> any()) | {atom(), atom()}</tt> : Function called when the
% partitions' assignments change. The first parameter is the consumer group ID, the second is the list of {topic, partition} that were unassigned, the third
% parameter is the list of {topic, partition} that were reassigned. (default: undefined).</li>
% <li><tt>from_beginning :: true | false</tt> : Start consuming method. If it's set to <tt>true</tt>, the consumer will start to consume from the offset next to the
% last committed one. If it's set to <tt>false</tt>, the consumer will start to consume next to the last offset. (default: true).</li>
% <li><tt>errors_actions :: map()</tt> : </li>
% </ul>
% @end
-spec start_consumer(GroupID :: binary(),
                     Callback :: fun((GroupID :: binary(),
                                      Topic :: binary(),
                                      PartitionID :: integer(),
                                      Offset :: integer(),
                                      Key :: binary(),
                                      Value :: binary()) -> ok | {error, term()})
                                   | fun((Message :: kafe_consumer_subscriber:message()) -> ok | {error, term()})
                                   | atom()
                                   | {atom(), list(term())},
                     Options :: consumer_options()) -> {ok, GroupPID :: pid()} | {error, term()}.
start_consumer(GroupID, Callback, Options) when is_function(Callback, 6);
                                                is_function(Callback, 1);
                                                is_atom(Callback);
                                                is_tuple(Callback) ->
  kafe_consumer_sup:start_child(GroupID, Options#{callback => Callback}).

% @doc
% Stop the given consumer
% @end
-spec stop_consumer(GroupID :: binary()) -> ok | {error, not_found | simple_one_for_one | detached}.
stop_consumer(GroupID) ->
  kafe_consumer_sup:stop_child(GroupID).

% Private

% @hidden
init(_) ->
  process_flag(trap_exit, true),
  ApiVersion = doteki:get_env([kafe, api_version], ?DEFAULT_API_VERSION),
  CorrelationID = doteki:get_env([kafe, correlation_id], ?DEFAULT_CORRELATION_ID),
  ClientID = doteki:get_env([kafe, client_id], ?DEFAULT_CLIENT_ID),
  Offset = doteki:get_env([kafe, offset], ?DEFAULT_OFFSET),
  BrokersUpdateFreq = doteki:get_env([kafe, brokers_update_frequency], ?DEFAULT_BROKER_UPDATE),
  PoolSize = doteki:get_env([kafe, pool_size], ?DEFAULT_POOL_SIZE),
  ChunkPoolSize = doteki:get_env([kafe, chunk_pool_size], ?DEFAULT_CHUNK_POOL_SIZE),
  State = #{brokers => #{},
            brokers_list => [],
            topics => #{},
            brokers_update_frequency => BrokersUpdateFreq,
            api_version => ApiVersion,
            correlation_id => CorrelationID,
            client_id => ClientID,
            offset => Offset,
            pool_size => PoolSize,
            chunk_pool_size => ChunkPoolSize},
  State1 = update_state_with_metadata(init_connexions(State)),
  {ok, State1#{
         brokers_update_timer => erlang:send_after(BrokersUpdateFreq, self(), update_brokers)
        }}.

% @hidden
handle_call(number_of_brokers, _From, #{brokers_list := Brokers} = State) ->
  {reply, length(Brokers), State};
handle_call(first_broker, _From, State) ->
  {reply, get_first_broker(State), State};
handle_call({broker_id_by_topic_and_partition, Topic, Partition}, _From, #{topics := Topics, brokers := BrokersAddr} = State) ->
  case maps:get(Topic, Topics, undefined) of
    undefined ->
      {reply, undefined, State};
    Brokers ->
      case maps:get(Partition, Brokers, undefined) of
        undefined ->
          {reply, undefined, State};
        Broker ->
          {reply, maps:get(Broker, BrokersAddr, undefined), State}
      end
  end;
handle_call({broker_by_name, BrokerName}, _From, #{brokers := BrokersAddr} = State) ->
  case maps:get(bucs:to_string(BrokerName), BrokersAddr, undefined) of
    undefined ->
      {reply, undefined, State};
    BrokerID ->
      case poolgirl:checkout(BrokerID) of
        {ok, BrokerPID} -> {reply, BrokerPID, State};
        _ -> {reply, undefined, State}
      end
  end;
handle_call(topics, _From, #{topics := Topics} = State) ->
  {reply, Topics, State};
handle_call({partitions, Topic}, _From, #{topics := Topics} = State) ->
  {reply, maps:keys(maps:get(Topic, Topics, #{})), State};
handle_call(api_version, _From, #{api_version := Version} = State) ->
  {reply, Version, State};
handle_call(state, _From, State) ->
  {reply, State, State};
% Public
handle_call(brokers, _From, #{brokers := Brokers} = State) ->
  {reply, maps:values(Brokers), State};
% RIP
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

% @hidden
handle_cast(update_brokers, State) ->
  handle_info(update_brokers, State);
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(update_brokers, #{brokers_update_frequency := Frequency,
                              brokers_update_timer := Timer} = State) ->
  lager:debug("Update brokers list..."),
  erlang:cancel_timer(Timer),
  State1 = update_state_with_metadata(remove_dead_brokers(State)),
  {noreply, State1#{
              brokers_update_timer => erlang:send_after(Frequency, self(), update_brokers)
             }};
% @hidden
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, #{brokers := Brokers} ) ->
  _ = poolgirl:remove_pools(maps:values(Brokers)),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% @hidden
init_connexions(State) ->
  KafkaBrokers = doteki:get_env(
                   [kafe, brokers],
                   [{doteki:get_env([kafe, host], ?DEFAULT_IP),
                     doteki:get_env([kafe, port], ?DEFAULT_PORT)}]),
  get_connection(KafkaBrokers, State).

% @hidden
get_connection([], State) ->
  lager:debug("No more brockers..."),
  State;
get_connection([{Host, Port}|Rest], #{brokers_list := BrokersList,
                                      brokers := Brokers,
                                      pool_size := PoolSize,
                                      chunk_pool_size := ChunkPoolSize} = State) ->
  lager:debug("Get connection for ~s:~p", [Host, Port]),
  try
    case inet:gethostbyname(bucs:to_string(Host)) of
      {ok, #hostent{h_name = Hostname,
                    h_addrtype = AddrType,
                    h_addr_list = AddrsList}} ->
        case get_host(AddrsList, Hostname, AddrType) of
          undefined ->
            lager:error("Can't retrieve host for ~s:~p", [Host, Port]),
            get_connection(Rest, State);
          {BrokerAddr, BrokerHostList} ->
            case lists:foldl(fun(E, Acc) ->
                                 BrokerFullName = kafe_utils:broker_name(E, Port),
                                 case lists:member(BrokerFullName, BrokersList) of
                                   true -> Acc;
                                   _ -> [BrokerFullName|Acc]
                                 end
                             end, [], BrokerHostList) of
              [] ->
                lager:debug("All hosts already registered for ~s:~p", [bucinet:ip_to_string(BrokerAddr), Port]),
                get_connection(Rest, State);
              BrokerHostList1 ->
                IP = bucinet:ip_to_string(BrokerAddr),
                BrokerID = kafe_utils:broker_id(IP, Port),
                case poolgirl:size(BrokerID) of
                  {ok, N, A} when N > 0 ->
                    lager:debug("Pool ~s size ~p/~p", [BrokerID, N, A]),
                    get_connection(Rest, State);
                  _ ->
                    case poolgirl:add_pool(BrokerID,
                                           {kafe_conn, start_link, [BrokerAddr, Port]},
                                           #{size => PoolSize,
                                             chunk_size => ChunkPoolSize,
                                             allow_empty_pool => false}) of
                      {ok, PoolSize1} ->
                        lager:info("Broker pool ~s (size ~p) reference ~p", [BrokerID, PoolSize1, BrokerHostList1]),
                        Brokers1 = lists:foldl(fun(BrokerHost, Acc) ->
                                                   maps:put(BrokerHost, BrokerID, Acc)
                                               end, Brokers, BrokerHostList1),
                        get_connection(Rest, State#{brokers => Brokers1,
                                                    brokers_list => BrokerHostList1 ++ BrokersList});
                      {error, Reason} ->
                        lager:error("Connection failed to ~p:~p : ~p", [bucinet:ip_to_string(BrokerAddr), Port, Reason]),
                        get_connection(Rest, State)
                    end
                end
            end
        end;
      {error, Reason} ->
        lager:error("Can't retrieve host by name for ~s:~p : ~p", [Host, Port, Reason]),
        get_connection(Rest, State)
    end
  catch
    Type:Reason1 ->
      lager:error("Error while getting connection for ~s:~p : ~p:~p", [Host, Port, Type, Reason1]),
      get_connection(Rest, State)
  end.

% @hidden
update_state_with_metadata(State) ->
  {State2, FirstBroker} = case get_first_broker(State) of
                            undefined ->
                              State1 = init_connexions(State),
                              {State1, get_first_broker(State1)};
                            Broker -> {State, Broker}
                          end,
  case FirstBroker of
    undefined ->
      State2;
    _ ->
      case kafe_protocol:run(FirstBroker,
                             {call,
                              fun kafe_protocol_metadata:request/2, [[]],
                              fun kafe_protocol_metadata:response/2}) of
        {ok, #{brokers := Brokers,
               topics := Topics}} ->
          {Brokers1, State3} = lists:foldl(fun(#{host := Host, id := ID, port := Port}, {Acc, StateAcc}) ->
                                               {maps:put(ID, kafe_utils:broker_name(Host, Port), Acc),
                                                get_connection([{bucs:to_string(Host), Port}], StateAcc)}
                                           end, {#{}, State2}, Brokers),
          State4 = remove_unlisted_brokers(maps:values(Brokers1), State3),
          case update_topics(Topics, Brokers1) of
            leader_election ->
              timer:sleep(1000),
              update_state_with_metadata(State3);
            Topics1 ->
              maps:put(topics, Topics1, State4)
          end;
        _ ->
          State2
      end
  end.

update_topics(Topics, Brokers1) ->
  update_topics(Topics, Brokers1, #{}).
update_topics([], _, Acc) ->
  Acc;
update_topics([#{name := Topic, partitions := Partitions}|Rest], Brokers1, Acc) ->
  case brokers_for_partitions(Partitions, Brokers1, Topic, #{}) of
    leader_election ->
      leader_election;
    BrokersForPartitions ->
      AccUpdate = maps:put(Topic, BrokersForPartitions, Acc),
      update_topics(Rest, Brokers1, AccUpdate)
  end.

brokers_for_partitions([], _, _, Acc) ->
  Acc;
brokers_for_partitions([#{id := ID, leader := -1}|_], _, Topic, _) ->
  lager:info("Leader election in progress for topic ~s, partition ~p", [Topic, ID]),
  leader_election;
brokers_for_partitions([#{id := ID, leader := Leader}|Rest], Brokers1, Topic, Acc) ->
  brokers_for_partitions(Rest, Brokers1, Topic,
                         maps:put(ID, maps:get(Leader, Brokers1), Acc)).

% @hidden
remove_unlisted_brokers(BrokersList, #{brokers := Brokers} = State) ->
  UnkillID = lists:foldl(fun(Broker, Acc) ->
                             case maps:get(Broker, Brokers, undefined) of
                               undefined -> Acc;
                               ID -> [ID|Acc]
                             end
                         end, [], BrokersList),
  Brokers1 = maps:fold(fun(BrokerName, BrokerID, Acc) ->
                           case lists:member(BrokerName, BrokersList) of
                             true ->
                               maps:put(BrokerName, BrokerID, Acc);
                             false ->
                               case lists:member(BrokerID, UnkillID) of
                                 true ->
                                   ok;
                                 false ->
                                   _ = poolgirl:remove_pool(BrokerID)
                               end,
                               Acc
                           end
                       end, #{}, Brokers),
  State#{brokers => Brokers1, brokers_list => BrokersList}.


% @hidden
remove_dead_brokers(#{brokers_list := BrokersList} = State) ->
  lists:foldl(fun(Broker, #{brokers := Brokers1, brokers_list := BrokersList1} = State1) ->
                  case maps:get(Broker, Brokers1, undefined) of
                    undefined ->
                      maps:put(brokers_list,
                               lists:delete(Broker, BrokersList1),
                               State1);
                    BrokerID ->
                      case poolgirl:checkout(BrokerID) of
                        {ok, BrokerPID} ->
                          case check_if_broker_is_alive(BrokerPID) of
                            ok ->
                              _ = poolgirl:checkin(BrokerPID),
                              State1;
                            {error, Reason} ->
                              _ = poolgirl:checkin(BrokerPID),
                              _ = poolgirl:remove_pool(BrokerID),
                              lager:warning("Broker ~s (from ~s) not alive : ~p", [Broker, BrokerID, Reason]),
                              maps:put(brokers_list,
                                       lists:delete(Broker, BrokersList1),
                                       maps:put(brokers, maps:remove(Broker, Brokers1), State1))
                          end;
                        _ ->
                          maps:put(brokers_list,
                                   lists:delete(Broker, BrokersList1),
                                   State1)
                      end
                  end
              end, State, BrokersList).

% @hidden
get_host([], _, _) -> undefined;
get_host([Addr|Rest], Hostname, AddrType) ->
  case inet:getaddr(Hostname, AddrType) of
    {ok, Addr} ->
      case inet:gethostbyaddr(Addr) of
        {ok, #hostent{h_name = Hostname1, h_aliases = HostAlias}} ->
          {Addr, lists:usort([Hostname|[Hostname1|HostAlias]])};
        _ ->
          {Addr, [Hostname]}
      end;
    _ -> get_host(Rest, Hostname, AddrType)
  end.

% @hidden
get_first_broker(#{brokers := Brokers}) ->
  get_first_broker(maps:values(Brokers));
get_first_broker([]) -> undefined;
get_first_broker([BrokerID|Rest]) ->
  case poolgirl:checkout(BrokerID) of
    {ok, Broker} ->
      case check_if_broker_is_alive(Broker) of
        ok ->
          Broker;
        {error, Reason} ->
          _ = poolgirl:checkin(Broker),
          lager:warning("Broker ~s is not alive: ~p", [Broker, Reason]),
          get_first_broker(Rest)
      end;
    {error, Reason} ->
      lager:error("Can't checkout broker from pool ~s: ~p", [BrokerID, Reason]),
      get_first_broker(Rest)
  end.

check_if_broker_is_alive(BrokerPid) ->
  try
    gen_server:call(BrokerPid, alive, ?TIMEOUT)
  catch
    Type:Error ->
      {error, {Type, Error}}
  end.
