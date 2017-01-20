% @hidden
-module(kafe_protocol_offset).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/2
        ]).

run(ReplicaID, []) ->
  case maps:keys(kafe:topics()) of
    Topics when is_list(Topics), length(Topics) > 0 ->
      run(ReplicaID, maps:keys(kafe:topics()));
    _ ->
      {error, cant_retrieve_topics}
  end;
run(ReplicaID, Topics) ->
  case lists:flatten(
         maps:fold(
           fun
             (undefined, _, Acc) ->
               Acc;
             (BrokerID, TopicsForBroker, Acc) ->
               case kafe_protocol:run(BrokerID,
                                      {call,
                                       fun ?MODULE:request/3, [ReplicaID, TopicsForBroker],
                                       fun ?MODULE:response/2}) of
                 {ok, Result} ->
                   [Result|Acc];
                 _ ->
                   Acc
               end
           end, [], dispatch(Topics, kafe:topics()))) of
    [] ->
      {error, no_broker_found};
    OffsetsData ->
      {ok, maps:fold(
             fun(K, V, Acc) ->
                 [#{name => K, partitions => V}|Acc]
             end,
             [],
             lists:foldl(
               fun(#{name := Name, partitions := Partitions}, Acc) ->
                   case maps:get(Name, Acc, undefined) of
                     undefined ->
                       maps:put(Name, Partitions, Acc);
                     Data ->
                       maps:put(Name, Data ++ Partitions, Acc)
                   end
               end, #{}, OffsetsData))}
  end.

% Offsets Request (Version: 0) => replica_id [topics]
%   replica_id => INT32
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition timestamp max_num_offsets
%       partition => INT32
%       timestamp => INT64
%       max_num_offsets => INT32
request(ReplicaId, Topics, State) ->
  kafe_protocol:request(
    ?OFFSET_REQUEST,
    <<ReplicaId:32/signed, (topics(Topics))/binary>>,
    State,
    ?V0).

% Offsets Response (Version: 0) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code [offsets]
%       partition => INT32
%       error_code => INT16
response(<<NumberOfTopics:32/signed, Remainder/binary>>, ApiVersion) ->
  {ok, response(NumberOfTopics, Remainder, ApiVersion)}.

% Private

dispatch(Topics, TopicsInfos) ->
  dispatch(Topics, TopicsInfos, #{}).

dispatch([], _, Result) ->
  Result;
dispatch([{Topic, Partitions}|Rest], TopicsInfos, Result) ->
  dispatch(Rest,
           TopicsInfos,
           lists:foldl(fun({ID, _, _} = Partition, Acc) ->
                           Broker = kafe_brokers:broker_id_by_topic_and_partition(Topic, ID),
                           Topics = maps:get(Broker, Acc, []),
                           maps:put(Broker, [{Topic, [Partition]}|Topics], Acc)
                       end, Result, Partitions));
dispatch([Topic|Rest], TopicsInfos, Result) when is_list(Topic);
                                                 is_atom(Topic) ->
  dispatch([bucs:to_binary(Topic)|Rest], TopicsInfos, Result);
dispatch([Topic|Rest], TopicsInfos, Result) when is_binary(Topic) ->
  Partitions = lists:foldl(fun(Partition, Acc) ->
                               [{Partition, ?DEFAULT_OFFSET_TIMESTAMP, ?DEFAULT_OFFSET_MAX_NUM_OFFSETS}|Acc]
                           end, [], maps:keys(maps:get(Topic, TopicsInfos, #{}))),
  dispatch([{Topic, Partitions}|Rest], TopicsInfos, Result).

topics(Topics) ->
  topics(Topics, <<(length(Topics)):32/signed>>).

topics([], Acc) -> Acc;
topics([{TopicName, Partitions} | T], Acc) ->
  topics(T,
         <<
           Acc/binary,
           (kafe_protocol:encode_string(bucs:to_binary(TopicName)))/binary,
           (kafe_protocol:encode_array(
              [<<Partition:32/signed, Timestamp:64/signed, MaxNumOffsets:32/signed>> ||
               {Partition, Timestamp, MaxNumOffsets} <- Partitions]))/binary
         >>);
topics([TopicName | T], Acc) ->
  topics([{TopicName, [{?DEFAULT_OFFSET_PARTITION,
                        ?DEFAULT_OFFSET_TIMESTAMP,
                        ?DEFAULT_OFFSET_MAX_NUM_OFFSETS}]} | T], Acc).

response(0, <<>>, _ApiVersion) ->
    [];
response(
  N,
  <<
    TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionsRemainder/binary
  >>,
  ApiVersion) ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionsRemainder, []),
  [#{name => TopicName, partitions => Partitions} | response(N - 1, Remainder, ApiVersion)].

partitions(0, Remainder, Acc) ->
    {Acc, Remainder};
partitions(
  N,
  <<
    Partition:32/signed,
    ErrorCode:16/signed,
    NumberOfOffsets:32/signed,
    Offsets:NumberOfOffsets/binary-unit:64,
    Remainder/binary
  >>,
  Acc) ->
  partitions(N - 1,
             Remainder,
             [#{id => Partition,
                error_code => kafe_error:code(ErrorCode),
                offsets => [Offset || <<Offset:64/signed>> <= Offsets]} | Acc]).

