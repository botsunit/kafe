% @hidden
-module(kafe_protocol_fetch).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").

-export([
         run/3,
         request/4,
         response/2
        ]).

run(ReplicaID, TopicName, Options) ->
  {Partition, Offset} = case {maps:get(partition, Options, undefined),
                              maps:get(offset, Options, undefined)} of
                          {undefined, undefined} ->
                            clean_offset(kafe:max_offset(TopicName));
                          {Partition1, undefined} ->
                            clean_offset(kafe:max_offset(TopicName, Partition1));
                          {undefined, Offset1} ->
                            kafe:partition_for_offset(TopicName, Offset1);
                          R -> R
                        end,
  Options1 = Options#{partition => Partition,
                      offset => Offset},
  lager:debug("Fetch ~p (partition #~p, offset ~p)", [TopicName, Partition, Offset]),
  kafe_protocol:run({topic_and_partition, TopicName, Partition},
                    {call,
                     fun ?MODULE:request/4, [ReplicaID, bucs:to_binary(TopicName), Options1],
                     fun ?MODULE:response/2}).

% FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
%   ReplicaId => int32
%   MaxWaitTime => int32
%   MinBytes => int32
%   TopicName => string
%   Partition => int32
%   FetchOffset => int64
%   MaxBytes => int32
request(ReplicaID, TopicName, Options, #{api_version := ApiVersion} = State) ->
  Partition = maps:get(partition, Options, ?DEFAULT_FETCH_PARTITION),
  Offset = maps:get(offset, Options),
  MaxBytes = maps:get(max_bytes, Options, ?DEFAULT_FETCH_MAX_BYTES),
  MinBytes = maps:get(min_bytes, Options, ?DEFAULT_FETCH_MIN_BYTES),
  MaxWaitTime = maps:get(max_wait_time, Options, ?DEFAULT_FETCH_MAX_WAIT_TIME),
  kafe_protocol:request(
    ?FETCH_REQUEST,
    <<ReplicaID:32/signed,
      MaxWaitTime:32/signed,
      MinBytes:32/signed,
      1:32/signed,
      (kafe_protocol:encode_string(TopicName))/binary,
      1:32/signed,
      Partition:32/signed,
      Offset:64/signed,
      MaxBytes:32/signed>>,
    State,
    ApiVersion).

% v0
% FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
%   TopicName => string
%   Partition => int32
%   ErrorCode => int16
%   HighwaterMarkOffset => int64
%   MessageSetSize => int32
%
% v1 (supported in 0.9.0 or later) and v2 (supported in 0.10.0 or later)
% FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]] ThrottleTime
%   TopicName => string
%   Partition => int32
%   ErrorCode => int16
%   HighwaterMarkOffset => int64
%   MessageSetSize => int32
%   ThrottleTime => int32
response(<<NumberOfTopics:32/signed, Remainder/binary>>, ApiVersion) when NumberOfTopics > 0, ApiVersion == ?V0 ->
  response(NumberOfTopics, Remainder, []);
response(<<ThrottleTime:32/signed, NumberOfTopics:32/signed, Remainder/binary>>, ApiVersion) when ApiVersion == ?V1;
                                                                                                  ApiVersion == ?V2 ->
  case response(NumberOfTopics, Remainder, []) of
    {ok, Response} ->
      {ok, #{topics => Response,
             throttle_time => ThrottleTime}};
    Error ->
      Error
  end;
response(_, _) ->
  {error, incomplete_data}.

% Private

clean_offset({Partition, Offset}) ->
  {Partition, if
                Offset > 0 -> Offset - 1;
                true -> Offset
              end}.

response(0, _, Result) ->
  {ok, Result};
response(
  N,
  <<TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionRemainder/binary>>,
  Acc) ->
  case partitions(NumberOfPartitions, PartitionRemainder, []) of
    {ok, Partitions, Remainder} ->
      response(N - 1, Remainder, [#{name => TopicName,
                                    partitions => Partitions}|Acc]);
    Error ->
      Error
  end;
response(_, _, _) ->
  {error, incomplete_data}.

partitions(0, Remainder, Acc) ->
  {ok, Acc, Remainder};
partitions(
  N,
  <<Partition:32/signed,
    ErrorCode:16/signed,
    HighwaterMarkOffset:64/signed,
    MessageSetSize:32/signed,
    MessageSet:MessageSetSize/binary,
    Remainder/binary>>,
  Acc) ->
  partitions(N - 1, Remainder,
             [#{partition => Partition,
                error_code => kafe_error:code(ErrorCode),
                high_watermark_offset => HighwaterMarkOffset,
                messages => message(MessageSet)} | Acc]);
partitions(_, _, _) ->
  {error, incomplete_data}.

message(Data) ->
  message(Data, []).

message(<<Offset:64/signed,
          MessageSize:32/signed,
          Message:MessageSize/binary,
          Remainder/binary>>, Acc) ->
  case Message of
    <<Crc:32/signed,
      0:8/signed,
      Attibutes:8/signed,
      MessageRemainder/binary>> ->
      {Key, MessageRemainder1} = get_kv(MessageRemainder),
      {Value, <<>>} = get_kv(MessageRemainder1),
      message(Remainder, [#{offset => Offset,
                            crc => Crc,
                            magic_byte => 0,
                            attributes => Attibutes,
                            key => Key,
                            value => Value}|Acc]);
    <<Crc:32/signed,
      1:8/signed,
      Attibutes:8/signed,
      Timestamp:64/signed,
      MessageRemainder/binary>> ->
      {Key, MessageRemainder1} = get_kv(MessageRemainder),
      {Value, <<>>} = get_kv(MessageRemainder1),
      message(Remainder, [#{offset => Offset,
                            crc => Crc,
                            magic_byte => 1,
                            attributes => Attibutes,
                            timestamp => Timestamp,
                            key => Key,
                            value => Value}|Acc])
  end;
message(_, Acc) ->
  lists:reverse(Acc).

get_kv(<<KVSize:32/signed, Remainder/binary>>) when KVSize =:= -1 ->
  {<<>>, Remainder};
get_kv(<<KVSize:32/signed, KV:KVSize/binary, Remainder/binary>>) ->
  {KV, Remainder}.

