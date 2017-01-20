

# Module kafe #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)


A Kafka client for Erlang.

Copyright (c) 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit

__Introduced in:__ 2014

__Authors:__ Grégoire Lejeune ([`gl@finexkap.com`](mailto:gl@finexkap.com)), Grégoire Lejeune ([`greg@g-corp.io`](mailto:greg@g-corp.io)), Grégoire Lejeune ([`gregoire.lejeune@botsunit.com`](mailto:gregoire.lejeune@botsunit.com)).

<a name="description"></a>

## Description ##
This module only implement the [Kafak Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol).
<a name="types"></a>

## Data Types ##




### <a name="type-broker_id">broker_id()</a> ###


<pre><code>
broker_id() = atom()
</code></pre>




### <a name="type-commit">commit()</a> ###


<pre><code>
commit() = <a href="#type-processing">processing()</a> | {interval, integer()} | {message, integer()}
</code></pre>




### <a name="type-consumer_options">consumer_options()</a> ###


<pre><code>
consumer_options() = #{session_timeout =&gt; integer(), member_id =&gt; binary(), topics =&gt; [binary() | {binary(), [integer()]}], fetch_interval =&gt; integer(), fetch_size =&gt; integer(), max_bytes =&gt; integer(), min_bytes =&gt; integer(), max_wait_time =&gt; integer(), on_start_fetching =&gt; fun((binary()) -&gt; any()) | {atom(), atom()} | undefined, on_stop_fetching =&gt; fun((binary()) -&gt; any()) | {atom(), atom()} | undefined, on_assignment_change =&gt; fun((binary(), [{binary(), integer()}], [{binary(), integer()}]) -&gt; any()) | {atom(), atom()} | undefined, can_fetch =&gt; fun(() -&gt; true | false) | {atom(), atom()} | undefined, from_beginning =&gt; true | false, commit =&gt; [<a href="#type-commit">commit()</a>]}
</code></pre>




### <a name="type-describe_group">describe_group()</a> ###


<pre><code>
describe_group() = [#{error_code =&gt; <a href="#type-error_code">error_code()</a>, group_id =&gt; binary(), members =&gt; [<a href="#type-group_member_ex">group_member_ex()</a>], protocol =&gt; binary(), protocol_type =&gt; binary(), state =&gt; binary()}]
</code></pre>




### <a name="type-error_code">error_code()</a> ###


<pre><code>
error_code() = no_error | unknown | offset_out_of_range | invalid_message | unknown_topic_or_partition | invalid_message_size | leader_not_available | not_leader_for_partition | request_timed_out | broker_not_available | replica_not_available | message_size_too_large | stale_controller_epoch | offset_metadata_too_large | offsets_load_in_progress | consumer_coordinator_not_available | not_coordinator_for_consumer
</code></pre>




### <a name="type-fetch_options">fetch_options()</a> ###


<pre><code>
fetch_options() = #{partition =&gt; integer(), offset =&gt; integer(), max_bytes =&gt; integer(), min_bytes =&gt; integer(), max_wait_time =&gt; integer(), retrieve =&gt; first | all}
</code></pre>




### <a name="type-group">group()</a> ###


<pre><code>
group() = #{group_id =&gt; binary(), protocol_type =&gt; binary()}
</code></pre>




### <a name="type-group_assignment">group_assignment()</a> ###


<pre><code>
group_assignment() = #{member_id =&gt; binary(), member_assignment =&gt; <a href="#type-member_assignment">member_assignment()</a>}
</code></pre>




### <a name="type-group_commit_identifier">group_commit_identifier()</a> ###


<pre><code>
group_commit_identifier() = binary()
</code></pre>




### <a name="type-group_coordinator">group_coordinator()</a> ###


<pre><code>
group_coordinator() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, coordinator_id =&gt; integer(), coordinator_host =&gt; binary(), coordinator_port =&gt; port()}
</code></pre>




### <a name="type-group_join">group_join()</a> ###


<pre><code>
group_join() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, generation_id =&gt; integer(), protocol_group =&gt; binary(), leader_id =&gt; binary(), member_id =&gt; binary(), members =&gt; [<a href="#type-group_member">group_member()</a>]}
</code></pre>




### <a name="type-group_member">group_member()</a> ###


<pre><code>
group_member() = #{member_id =&gt; binary(), member_metadata =&gt; binary()}
</code></pre>




### <a name="type-group_member_ex">group_member_ex()</a> ###


<pre><code>
group_member_ex() = #{client_host =&gt; binary(), client_id =&gt; binary(), member_id =&gt; binary(), member_metadata =&gt; binary(), member_assignment =&gt; <a href="#type-member_assignment">member_assignment()</a>}
</code></pre>




### <a name="type-groups">groups()</a> ###


<pre><code>
groups() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, groups =&gt; [<a href="#type-group">group()</a>]}
</code></pre>




### <a name="type-groups_list">groups_list()</a> ###


<pre><code>
groups_list() = [#{broker =&gt; <a href="#type-broker_id">broker_id()</a>, groups =&gt; <a href="#type-groups">groups()</a>}]
</code></pre>




### <a name="type-join_group_options">join_group_options()</a> ###


<pre><code>
join_group_options() = #{session_timeout =&gt; integer(), member_id =&gt; binary(), protocol_type =&gt; binary(), protocols =&gt; [<a href="#type-protocol">protocol()</a>]}
</code></pre>




### <a name="type-member_assignment">member_assignment()</a> ###


<pre><code>
member_assignment() = #{version =&gt; integer(), partition_assignment =&gt; [<a href="#type-partition_assignment">partition_assignment()</a>], user_data =&gt; binary()}
</code></pre>




### <a name="type-message">message()</a> ###


<pre><code>
message() = binary() | {binary(), binary()}
</code></pre>




### <a name="type-message_set">message_set()</a> ###


<pre><code>
message_set() = #{name =&gt; binary(), partitions =&gt; [#{partition =&gt; integer(), error_code =&gt; <a href="#type-error_code">error_code()</a>, high_watermark_offset =&gt; integer(), messages =&gt; [#{offset =&gt; integer(), crc =&gt; integer(), magic_byte =&gt; 0 | 1, attributes =&gt; integer(), timestamp =&gt; integer(), key =&gt; binary(), value =&gt; binary()}]}]}
</code></pre>




### <a name="type-metadata">metadata()</a> ###


<pre><code>
metadata() = #{brokers =&gt; [#{host =&gt; binary(), id =&gt; integer(), port =&gt; port()}], topics =&gt; [#{error_code =&gt; <a href="#type-error_code">error_code()</a>, name =&gt; binary(), partitions =&gt; [#{error_code =&gt; <a href="#type-error_code">error_code()</a>, id =&gt; integer(), isr =&gt; [integer()], leader =&gt; integer(), replicas =&gt; [integer()]}]}]}
</code></pre>




### <a name="type-offset_commit_option">offset_commit_option()</a> ###


<pre><code>
offset_commit_option() = [{binary(), [{integer(), integer(), binary()}]}]
</code></pre>




### <a name="type-offset_commit_option_v1">offset_commit_option_v1()</a> ###


<pre><code>
offset_commit_option_v1() = [{binary(), [{integer(), integer(), integer(), binary()}]}]
</code></pre>




### <a name="type-offset_commit_set">offset_commit_set()</a> ###


<pre><code>
offset_commit_set() = [#{name =&gt; binary(), partitions =&gt; [#{partition =&gt; integer(), error_code =&gt; <a href="#type-error_code">error_code()</a>}]}]
</code></pre>




### <a name="type-offset_fetch_options">offset_fetch_options()</a> ###


<pre><code>
offset_fetch_options() = [binary()] | [{binary(), [integer()]}]
</code></pre>




### <a name="type-offset_fetch_set">offset_fetch_set()</a> ###


<pre><code>
offset_fetch_set() = #{name =&gt; binary(), partitions_offset =&gt; [#{partition =&gt; integer(), offset =&gt; integer(), metadata_info =&gt; binary(), error_code =&gt; <a href="#type-error_code">error_code()</a>}]}
</code></pre>




### <a name="type-partition_assignment">partition_assignment()</a> ###


<pre><code>
partition_assignment() = #{topic =&gt; binary(), partitions =&gt; [integer()]}
</code></pre>




### <a name="type-processing">processing()</a> ###


<pre><code>
processing() = before_processing | after_processing
</code></pre>




### <a name="type-produce_options">produce_options()</a> ###


<pre><code>
produce_options() = #{timeout =&gt; integer(), required_acks =&gt; integer(), partition =&gt; integer(), key_to_partition =&gt; fun((binary(), term()) -&gt; integer())}
</code></pre>




### <a name="type-protocol">protocol()</a> ###


<pre><code>
protocol() = binary()
</code></pre>




### <a name="type-response_code">response_code()</a> ###


<pre><code>
response_code() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>}
</code></pre>




### <a name="type-sync_group">sync_group()</a> ###


<pre><code>
sync_group() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, version =&gt; integer(), partition_assignment =&gt; [<a href="#type-partition_assignment">partition_assignment()</a>], user_data =&gt; binary()}
</code></pre>




### <a name="type-topic_partition_info">topic_partition_info()</a> ###


<pre><code>
topic_partition_info() = #{name =&gt; binary(), partitions =&gt; [#{error_code =&gt; <a href="#type-error_code">error_code()</a>, id =&gt; integer(), offsets =&gt; [integer()]}]}
</code></pre>




### <a name="type-topics">topics()</a> ###


<pre><code>
topics() = [binary() | string() | atom()] | [{binary() | string() | atom(), [{integer(), integer(), integer()}]}]
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#brokers-0">brokers/0</a></td><td>
Return the list of availables brokers.</td></tr><tr><td valign="top"><a href="#default_key_to_partition-2">default_key_to_partition/2</a></td><td>
Default fonction used to do partition assignment from the message key.</td></tr><tr><td valign="top"><a href="#default_protocol-4">default_protocol/4</a></td><td>
Create a default protocol as defined in the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-JoinGroupRequest">Kafka Protocol Guide</a>.</td></tr><tr><td valign="top"><a href="#describe_group-1">describe_group/1</a></td><td> 
Return the description of the given consumer group.</td></tr><tr><td valign="top"><a href="#fetch-1">fetch/1</a></td><td>Equivalent to <a href="#fetch-3"><tt>fetch(-1, TopicName, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#fetch-2">fetch/2</a></td><td>Equivalent to <a href="#fetch-3"><tt>fetch(ReplicatID, TopicName, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#fetch-3">fetch/3</a></td><td> 
Fetch messages.</td></tr><tr><td valign="top"><a href="#group_coordinator-1">group_coordinator/1</a></td><td> 
Group coordinator Request.</td></tr><tr><td valign="top"><a href="#heartbeat-3">heartbeat/3</a></td><td> 
Once a member has joined and synced, it will begin sending periodic heartbeats to keep itself in the group.</td></tr><tr><td valign="top"><a href="#join_group-1">join_group/1</a></td><td>Equivalent to <a href="#join_group-2"><tt>join_group(GroupID, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#join_group-2">join_group/2</a></td><td> 
Join Group.</td></tr><tr><td valign="top"><a href="#leave_group-2">leave_group/2</a></td><td> 
To explicitly leave a group, the client can send a leave group request.</td></tr><tr><td valign="top"><a href="#list_groups-0">list_groups/0</a></td><td>
Find groups managed by all brokers.</td></tr><tr><td valign="top"><a href="#list_groups-1">list_groups/1</a></td><td> 
Find groups managed by a broker.</td></tr><tr><td valign="top"><a href="#metadata-0">metadata/0</a></td><td>Equivalent to <a href="#metadata-1"><tt>metadata([])</tt></a>.</td></tr><tr><td valign="top"><a href="#metadata-1">metadata/1</a></td><td> 
Return metadata for the given topics.</td></tr><tr><td valign="top"><a href="#offset-0">offset/0</a></td><td>Equivalent to <a href="#offset-2"><tt>offset(-1, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#offset-1">offset/1</a></td><td>Equivalent to <a href="#offset-2"><tt>offset(-1, Topics)</tt></a>.</td></tr><tr><td valign="top"><a href="#offset-2">offset/2</a></td><td> 
Get offet for the given topics and replicat.</td></tr><tr><td valign="top"><a href="#offset_commit-2">offset_commit/2</a></td><td> 
Offset commit v0.</td></tr><tr><td valign="top"><a href="#offset_commit-4">offset_commit/4</a></td><td> 
Offset commit v1.</td></tr><tr><td valign="top"><a href="#offset_commit-5">offset_commit/5</a></td><td> 
Offset commit v2.</td></tr><tr><td valign="top"><a href="#offset_fetch-1">offset_fetch/1</a></td><td>Equivalent to <a href="#offset_fetch-2"><tt>offset_fetch(ConsumerGroup, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#offset_fetch-2">offset_fetch/2</a></td><td> 
Offset fetch.</td></tr><tr><td valign="top"><a href="#offsets-2">offsets/2</a></td><td>
Return the list of all unread offsets for a given topic and consumer group.</td></tr><tr><td valign="top"><a href="#offsets-3">offsets/3</a></td><td>
Return the list of the next Nth unread offsets for a given topic and consumer group.</td></tr><tr><td valign="top"><a href="#produce-2">produce/2</a></td><td>Equivalent to <a href="#produce-3"><tt>produce(Topic, Message, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#produce-3">produce/3</a></td><td> 
Send a message.</td></tr><tr><td valign="top"><a href="#start-0">start/0</a></td><td>
Start kafe application.</td></tr><tr><td valign="top"><a href="#start_consumer-3">start_consumer/3</a></td><td> 
Start a new consumer.</td></tr><tr><td valign="top"><a href="#stop_consumer-1">stop_consumer/1</a></td><td>
Stop the given consumer.</td></tr><tr><td valign="top"><a href="#sync_group-4">sync_group/4</a></td><td> 
The sync group request is used by the group leader to assign state (e.g.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="brokers-0"></a>

### brokers/0 ###

`brokers() -> any()`

Return the list of availables brokers

<a name="default_key_to_partition-2"></a>

### default_key_to_partition/2 ###

<pre><code>
default_key_to_partition(Topic::binary(), Key::term()) -&gt; integer()
</code></pre>
<br />

Default fonction used to do partition assignment from the message key.

<a name="default_protocol-4"></a>

### default_protocol/4 ###

<pre><code>
default_protocol(Name::binary(), Version::integer(), Topics::<a href="#type-topics">topics()</a>, UserData::binary()) -&gt; <a href="#type-protocol">protocol()</a>
</code></pre>
<br />

Create a default protocol as defined in the [Kafka Protocol Guide](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-JoinGroupRequest).

<a name="describe_group-1"></a>

### describe_group/1 ###

<pre><code>
describe_group(GroupID::binary()) -&gt; {error, term()} | {ok, <a href="#type-describe_group">describe_group()</a>}
</code></pre>
<br />


Return the description of the given consumer group.

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-DescribeGroupsRequest).

<a name="fetch-1"></a>

### fetch/1 ###

`fetch(TopicName) -> any()`

Equivalent to [`fetch(-1, TopicName, #{})`](#fetch-3).

<a name="fetch-2"></a>

### fetch/2 ###

`fetch(ReplicatID, TopicName) -> any()`

Equivalent to [`fetch(ReplicatID, TopicName, #{})`](#fetch-3).

<a name="fetch-3"></a>

### fetch/3 ###

<pre><code>
fetch(ReplicatID::integer(), TopicName::binary(), Options::<a href="#type-fetch_options">fetch_options()</a>) -&gt; {ok, [<a href="#type-message_set">message_set()</a>]} | {ok, #{topics =&gt; [<a href="#type-message_set">message_set()</a>], throttle_time =&gt; integer()}} | {error, term()}
</code></pre>
<br />


Fetch messages

Options:

* `partition :: integer()` : The id of the partition the fetch is for (default : partition with the highiest offset).

* `offset :: integer()` : The offset to begin this fetch from (default : next offset for the partition)

* `max_bytes :: integer()` : The maximum bytes to include in the message set for this partition. This helps bound the size of the response (default :
1024*1024)

* `min_bytes :: integer()` : This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0
the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is
set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in
combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting
MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding) (default :
1).

* `max_wait_time :: integer()` : The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available
at the time the request is issued (default : 100).

* `retrieve :: all | first` : if the Kafka's response buffer contains more than one complete message ; with `first` we will ignore the
remaining data ; with `all` we will parse all complete messages in the buffer (default : first).


ReplicatID must __always__ be -1.

Example:

```

 Response = kafe:fetch(<<"topic">>)
 Response1 = kafe:fetch(<<"topic">>, #{offset => 2, partition => 3}).
```

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI).

<a name="group_coordinator-1"></a>

### group_coordinator/1 ###

<pre><code>
group_coordinator(ConsumerGroup::binary()) -&gt; {ok, <a href="#type-group_coordinator">group_coordinator()</a>} | {error, term()}
</code></pre>
<br />


Group coordinator Request

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ConsumerMetadataRequest).

For compatibility, this function as an alias : `consumer_metadata`.

<a name="heartbeat-3"></a>

### heartbeat/3 ###

<pre><code>
heartbeat(GroupID::binary(), GenerationID::integer(), MemberID::binary()) -&gt; {error, term()} | {ok, <a href="#type-response_code">response_code()</a>}
</code></pre>
<br />


Once a member has joined and synced, it will begin sending periodic heartbeats to keep itself in the group. If not heartbeat has been received by the 
coordinator with the configured session timeout, the member will be kicked out of the group.

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-HeartbeatRequest).

<a name="join_group-1"></a>

### join_group/1 ###

`join_group(GroupID) -> any()`

Equivalent to [`join_group(GroupID, #{})`](#join_group-2).

<a name="join_group-2"></a>

### join_group/2 ###

<pre><code>
join_group(GroupID::binary(), Options::<a href="#type-join_group_options">join_group_options()</a>) -&gt; {error, term()} | {ok, <a href="#type-group_join">group_join()</a>}
</code></pre>
<br />


Join Group

Options:

* `session_timeout :: integer()` : The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms. (default: 10000)

* `member_id :: binary()` : The assigned consumer id or an empty string for a new consumer. When a member first joins the group, the memberID must be
empty (i.e. <<>>, default), but a rejoining member should use the same memberID from the previous generation.

* `protocol_type :: binary()` : Unique name for class of protocols implemented by group (default <<"consumer">>).

* `protocols :: [protocol()]` : List of protocols.


For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-JoinGroupRequest).

<a name="leave_group-2"></a>

### leave_group/2 ###

<pre><code>
leave_group(GroupID::binary(), MemberID::binary()) -&gt; {error, term()} | {ok, <a href="#type-response_code">response_code()</a>}
</code></pre>
<br />


To explicitly leave a group, the client can send a leave group request. This is preferred over letting the session timeout expire since it allows the group to 
rebalance faster, which for the consumer means that less time will elapse before partitions can be reassigned to an active member.

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-LeaveGroupRequest).

<a name="list_groups-0"></a>

### list_groups/0 ###

<pre><code>
list_groups() -&gt; {ok, <a href="#type-groups_list">groups_list()</a>} | {error, term()}
</code></pre>
<br />

Find groups managed by all brokers.

<a name="list_groups-1"></a>

### list_groups/1 ###

<pre><code>
list_groups(Broker::<a href="#type-broker_id">broker_id()</a>) -&gt; {ok, <a href="#type-groups">groups()</a>} | {error, term()}
</code></pre>
<br />


Find groups managed by a broker.

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ListGroupsRequest)

<a name="metadata-0"></a>

### metadata/0 ###

`metadata() -> any()`

Equivalent to [`metadata([])`](#metadata-1).

<a name="metadata-1"></a>

### metadata/1 ###

<pre><code>
metadata(Topics::[binary() | string() | atom()]) -&gt; {ok, <a href="#type-metadata">metadata()</a>} | {error, term()}
</code></pre>
<br />


Return metadata for the given topics

Example:

```

 Metadata = kafe:metadata([<<"topic1">>, <<"topic2">>]).
```

This example return all metadata for `topic1` and `topic2`

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TopicMetadataRequest).

<a name="offset-0"></a>

### offset/0 ###

`offset() -> any()`

Equivalent to [`offset(-1, [])`](#offset-2).

<a name="offset-1"></a>

### offset/1 ###

`offset(Topics) -> any()`

Equivalent to [`offset(-1, Topics)`](#offset-2).

<a name="offset-2"></a>

### offset/2 ###

<pre><code>
offset(ReplicatID::integer(), Topics::<a href="#type-topics">topics()</a>) -&gt; {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]} | {error, term()}
</code></pre>
<br />


Get offet for the given topics and replicat

Example:

```

 Offset = kafe:offet(-1, [<<"topic1">>, {<<"topic2">>, [{0, -1, 1}, {2, -1, 1}]}]).
```

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest).

<a name="offset_commit-2"></a>

### offset_commit/2 ###

<pre><code>
offset_commit(ConsumerGroup::binary(), Topics::<a href="#type-offset_commit_option">offset_commit_option()</a>) -&gt; {ok, [<a href="#type-offset_commit_set">offset_commit_set()</a>]} | {error, term()}
</code></pre>
<br />


Offset commit v0

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).

<a name="offset_commit-4"></a>

### offset_commit/4 ###

<pre><code>
offset_commit(ConsumerGroup::binary(), ConsumerGroupGenerationID::integer(), ConsumerID::binary(), Topics::<a href="#type-offset_commit_option_v1">offset_commit_option_v1()</a>) -&gt; {ok, [<a href="#type-offset_commit_set">offset_commit_set()</a>]} | {error, term()}
</code></pre>
<br />


Offset commit v1

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).

<a name="offset_commit-5"></a>

### offset_commit/5 ###

<pre><code>
offset_commit(ConsumerGroup::binary(), ConsumerGroupGenerationID::integer(), ConsumerID::binary(), RetentionTime::integer(), Topics::<a href="#type-offset_commit_option">offset_commit_option()</a>) -&gt; {ok, [<a href="#type-offset_commit_set">offset_commit_set()</a>]} | {error, term()}
</code></pre>
<br />


Offset commit v2

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).

<a name="offset_fetch-1"></a>

### offset_fetch/1 ###

<pre><code>
offset_fetch(ConsumerGroup::binary()) -&gt; {ok, [<a href="#type-offset_fetch_set">offset_fetch_set()</a>]}
</code></pre>
<br />

Equivalent to [`offset_fetch(ConsumerGroup, [])`](#offset_fetch-2).

<a name="offset_fetch-2"></a>

### offset_fetch/2 ###

<pre><code>
offset_fetch(ConsumerGroup::binary(), Options::<a href="#type-offset_fetch_options">offset_fetch_options()</a>) -&gt; {ok, [<a href="#type-offset_fetch_set">offset_fetch_set()</a>]} | {error, term()}
</code></pre>
<br />


Offset fetch

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest).

<a name="offsets-2"></a>

### offsets/2 ###

<pre><code>
offsets(TopicName::binary(), ConsumerGroup::binary()) -&gt; [{integer(), integer()}] | error
</code></pre>
<br />

Return the list of all unread offsets for a given topic and consumer group

<a name="offsets-3"></a>

### offsets/3 ###

<pre><code>
offsets(TopicName::binary() | {binary(), [integer()]}, ConsumerGroup::binary(), Nth::integer()) -&gt; [{integer(), integer()}] | error
</code></pre>
<br />

Return the list of the next Nth unread offsets for a given topic and consumer group

<a name="produce-2"></a>

### produce/2 ###

`produce(Topic, Message) -> any()`

Equivalent to [`produce(Topic, Message, #{})`](#produce-3).

<a name="produce-3"></a>

### produce/3 ###

<pre><code>
produce(Topic::binary(), Message::<a href="#type-message">message()</a>, Options::<a href="#type-produce_options">produce_options()</a>) -&gt; ok | {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]} | {error, term()}
</code></pre>
<br />


Send a message

Options:

* `timeout :: integer()` : This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in
RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include network latency, (2) the timer begins at the
beginning of the processing of this request so if many requests are queued due to server overload that wait time will not be included, (3) we will not
terminate a local write so if the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client should use the
socket timeout. (default: 5000)

* `required_acks :: integer()` : This field indicates how many acknowledgements the servers should receive before responding to the request. If it is
0 the server will not send any response (this is the only case where the server will not reply to a request) and this function will return ok.
If it is 1, the server will wait the data is written to the local log before sending a response. If it is -1 the server will block until the message is committed
by all in sync replicas before sending a response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server
will never wait for more acknowledgements than there are in-sync replicas). (default: -1)

* `partition :: integer()` : The partition that data is being published to.

* `key_to_partition :: fun((binary(), term()) -> integer())` : Hash function to do partition assignment from the message key. (default:
kafe:default_key_to_partition/2)


If the partition is specified (option `partition`) and there is a message' key, the message will be produce on the specified partition. If no partition
is specified, and there is a message key, the partition will be calculated using the `key_to_partition` function (or an internal function if this 
option is not specified). If there is no key and no partition specified, the partition will be choosen using a round robin algorithm.

Example:

```

 Response = kafe:product(<<"topic">>, <<"a simple message">>, #{timeout => 1000, partition => 0}).
 Response1 = kafe:product(<<"topic">>, {<<"key">>, <<"Another simple message">>}).
```

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceAPI).

<a name="start-0"></a>

### start/0 ###

`start() -> any()`

Start kafe application

<a name="start_consumer-3"></a>

### start_consumer/3 ###

<pre><code>
start_consumer(GroupID::binary(), Callback::fun((GroupID::binary(), Topic::binary(), PartitionID::integer(), Offset::integer(), Key::binary(), Value::binary()) -&gt; ok | {error, term()}) | fun((Message::<a href="kafe_consumer_subscriber.md#type-message">kafe_consumer_subscriber:message()</a>) -&gt; ok | {error, term()}) | atom() | {atom(), [term()]}, Options::<a href="#type-consumer_options">consumer_options()</a>) -&gt; {ok, GroupPID::pid()} | {error, term()}
</code></pre>
<br />


Start a new consumer.

Options:

* `session_timeout :: integer()` : The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms. (default: 10000)

* `member_id :: binary()` : The assigned consumer id or an empty string for a new consumer. When a member first joins the group, the memberID must be
empty (i.e. <<>>, default), but a rejoining member should use the same memberID from the previous generation.

* `topics :: [binary() | {binary(), [integer()]}]` : List or topics (and partitions).

* `fetch_interval :: integer()` : Fetch interval in ms (default : 10)

* `max_bytes :: integer()` : The maximum bytes to include in the message set for this partition. This helps bound the size of the response (default :
1024*1024)

* `min_bytes :: integer()` : This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0
the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is
set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in
combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting
MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding) (default :
1).

* `max_wait_time :: integer()` : The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available
at the time the request is issued (default : 100).

* `commit :: commit()` : Commit configuration (default: [after_processing, {interval, 1000}]).

* `on_start_fetching :: fun((GroupID :: binary()) -> any()) | {atom(), atom()}` : Function called when the fetcher start/restart fetching. (default: undefined).

* `on_stop_fetching :: fun((GroupID :: binary()) -> any()) | {atom(), atom()}` : Function called when the fetcher stop fetching. (default: undefined).

* `can_fetch :: fun(() -> true | false) | {atom(), atom()}` : Messages are fetched, only if this function returns `true` or is undefined.
(default: undefined).

* `on_assignment_change :: fun((GroupID :: binary(), [{binary(), integer()}], [{binary(), integer()}]) -> any()) | {atom(), atom()}` : Function called when the
partitions' assignments change. The first parameter is the consumer group ID, the second is the list of {topic, partition} that were unassigned, the third
parameter is the list of {topic, partition} that were reassigned. (default: undefined).

* `from_beginning :: true | false` : Start consuming method. If it's set to `true`, the consumer will start to consume from the offset next to the
last committed one. If it's set to `false`, the consumer will start to consume next to the last offset. (default: true).

* `errors_actions :: map()` :


<a name="stop_consumer-1"></a>

### stop_consumer/1 ###

<pre><code>
stop_consumer(GroupID::binary()) -&gt; ok | {error, not_found | simple_one_for_one | detached}
</code></pre>
<br />

Stop the given consumer

<a name="sync_group-4"></a>

### sync_group/4 ###

<pre><code>
sync_group(GroupID::binary(), GenerationID::integer(), MemberID::binary(), Assignments::[<a href="#type-group_assignment">group_assignment()</a>]) -&gt; {error, term()} | {ok, <a href="#type-sync_group">sync_group()</a>}
</code></pre>
<br />


The sync group request is used by the group leader to assign state (e.g. partition assignments) to all members of the current generation. All members send 
SyncGroup immediately after joining the group, but only the leader provides the group's assignment.

Example:

```

 kafe:sync_group(<<"my_group">>, 1, <<"kafka-6dbb08f4-a0dc-4f4c-a0b9-dccb4d03ff2c">>,
                 [#{member_id => <<"kafka-6dbb08f4-a0dc-4f4c-a0b9-dccb4d03ff2c">>,
                    member_assignment => #{version => 0,
                                           user_data => <<"my user data">>,
                                           partition_assignment => [#{topic => <<"topic0">>,
                                                                      partitions => [0, 1, 2]},
                                                                    #{topic => <<"topic1">>,
                                                                      partitions => [0, 1, 2]}]}},
                  #{member_id => <<"kafka-0b7e179d-3ff9-46d2-b652-e0d041e4264a">>,
                    member_assignment => #{version => 0,
                                           user_data => <<"my user data">>,
                                           partition_assignment => [#{topic => <<"topic0">>,
                                                                      partitions => [0, 1, 2]},
                                                                    #{topic => <<"topic1">>,
                                                                      partitions => [0, 1, 2]}]}}]).
```

For more informations, see the
[Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-SyncGroupRequest).

