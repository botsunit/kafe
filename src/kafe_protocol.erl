% @hidden
-module(kafe_protocol).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").

-export([
         run/1,
         run/2,
         request/3,
         request/4,
         response/2,
         encode_string/1,
         encode_bytes/1,
         encode_array/1
        ]).

run(Request) ->
  case kafe_brokers:first_broker(false) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end.

run(BrokerPID, Request) when is_pid(BrokerPID) ->
  case erlang:is_process_alive(BrokerPID) of
    true ->
      try
        Response = gen_server:call(BrokerPID, Request, ?TIMEOUT),
        _ = kafe_brokers:release_broker(BrokerPID),
        Response
      catch
        Type:Error ->
          _ = kafe_brokers:release_broker(BrokerPID),
          lager:error("Request error: ~p:~p", [Type, Error]),
          {error, Error}
      end;
    false ->
      _ = kafe_brokers:release_broker(BrokerPID),
      {error, broker_not_available}
  end;
run(BrokerName, Request) when is_list(BrokerName) ->
  case kafe_brokers:broker_by_name(BrokerName) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end;
run(BrokerID, Request) when is_atom(BrokerID) ->
  case kafe_brokers:broker_by_id(BrokerID) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end;
run({host_and_port, Host, Port}, Request) ->
  case kafe_brokers:broker_by_host_and_port(Host, Port) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end;
run({topic_and_partition, Topic, Partition}, Request) ->
  case kafe_brokers:broker_by_topic_and_partition(Topic, Partition) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end.

request(ApiKey, RequestMessage,
        #{api_version := ApiVersion} = State) ->
  request(ApiKey, RequestMessage, State, ApiVersion).
request(ApiKey, RequestMessage,
        #{correlation_id := CorrelationId,
          client_id := ClientId} = State,
       ApiVersion) ->
  #{packet => <<
                 ApiKey:16/signed,
                 ApiVersion:16/signed,
                 CorrelationId:32/signed,
                 (encode_string(ClientId))/binary,
                 RequestMessage/binary
               >>,
    state => maps:update(correlation_id, CorrelationId + 1, State),
    api_version => ApiVersion}.

encode_string(undefined) ->
  <<-1:16/signed>>;
encode_string(Data) when is_binary(Data) ->
  <<(byte_size(Data)):16/signed, Data/binary>>.

encode_bytes(undefined) ->
  <<-1:32/signed>>;
encode_bytes(Data) ->
  <<(byte_size(Data)):32/signed, Data/binary>>.

encode_array(List) ->
  Len = length(List),
  Payload = << <<B/binary>> || B <- List>>,
  <<Len:32/signed, Payload/binary>>.

response(<<CorrelationId:32/signed, Remainder/bytes>>, #{requests := Requests} = State) ->
  case orddict:find(CorrelationId, Requests) of
    {ok, #{from := From, handler := {ResponseHandler, ResponseHandlerParams}, api_version := ApiVersion}} ->
      _ = gen_server:reply(From, erlang:apply(ResponseHandler, [Remainder, ApiVersion|ResponseHandlerParams])),
      {ok, maps:update(requests, orddict:erase(CorrelationId, Requests), State)};
    error ->
      {error, request_not_found} %;
  end.

