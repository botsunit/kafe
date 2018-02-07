-module(kafe_protocol_produce_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_produce_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request_v0()),
    ?_test(t_request_v1()),
    ?_test(t_request_v2_explicit_timestamp()),
    ?_test(t_request_v2_automatic_timestamp()),
    ?_test(t_response())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_request_v0() ->
  ?assertEqual(
     #{packet => <<0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 0, 0, 19, 136, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 86, 91, 193, 236,
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>}]}]}],
                                   #{},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 0,
                                     correlation_id => 0,
                                     client_id => <<"test">>})).

t_request_v1() ->
  ?assertEqual(
     #{packet => <<0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 0, 0, 19, 136, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 86, 91, 193, 236,
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>}]}]}],
                                   #{},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 1,
                                     correlation_id => 0,
                                     client_id => <<"test">>})),

  ?assertEqual(
     #{packet => <<0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 0, 0, 19, 136, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 86, 91, 193, 236,
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>}]}]}],
                                   #{timestamp => not_used},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 1,
                                     correlation_id => 0,
                                     client_id => <<"test">>})),

  ?assertEqual(
     #{packet => <<0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 0, 0, 19, 136, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 115, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 86, 91, 193,
                   236, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 0, 0, 0,
                   0, 0, 0, 0, 0, 0, 0, 0, 24, 162, 47, 233, 121, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 104, 111, 108, 97, 32,
                   109, 117, 110, 100, 111, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 30, 227, 127, 239, 189, 0, 0, 0, 0, 0, 0, 0,
                   0, 0, 16, 98, 111, 110, 106, 111, 117, 114, 32, 108, 101, 32, 109, 111, 110, 100, 101>>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>},
                                                        {<<>>, <<"hola mundo">>},
                                                        {<<>>, <<"bonjour le monde">>}]}]}],
                                   #{timestamp => not_used},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 1,
                                     correlation_id => 0,
                                     client_id => <<"test">>})).

t_request_v2_explicit_timestamp() ->
  ?assertEqual(
     #{packet => << ?PRODUCE_REQUEST:16 % API key
                  , 2:16                % API version
                  , 0:32                % correlation id
                  , 4:16, "test"        % client id
                    , -1:16               % required acks
                    , 5000:32             % timeout
                    , 1:32                % message count
                      , 5:16, "topic"       % topic name
                      , 1:32                % partition count
                        , 0:32                    % partition index
                        , (8+4+4+1+1+8+4+4+11):32 % message set size
                          % offset
                          , 0:64
                          % message size
                          , (4+1+1+8+4+4+11):32
                          % crc,                magic, attributes, timestamp,        key,   value
                          , 240, 221, 239, 209, 1,     0,          1000000000000:64, 0:32,  11:32, "hello world"
                 >>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 2,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>}]}]}],
                                   #{timestamp => 1000000000000},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 2,
                                     correlation_id => 0,
                                     client_id => <<"test">>})).

t_request_v2_automatic_timestamp() ->
  Now = erlang:system_time(millisecond),

  #{ packet :=
     << ?PRODUCE_REQUEST:16 % API key
     , 2:16                % API version
     , 0:32                % correlation id
     , 4:16, "test"        % client id
       , -1:16/signed        % required acks
       , _Timeout:32         % timeout
       , 1:32                % message count
         , 5:16, "topic"       % topic name
         , 1:32                % partition count
           , 0:32                    % partition index
           , MessageSet/binary >> } =
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>}]}]}],
                                   #{},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 2,
                                     correlation_id => 0,
                                     client_id => <<"test">>}),

  << _MsgSetSize:32 % message set size
       , 0:64                      % offset
       , _MsgSize:32       % message size
       % crc,     magic, attributes, timestamp,        key+value
       , _Crc:32, 1,     0,          Timestamp:64,     _KeyAndValue/binary >> =
    MessageSet,

  ?assertMatch(T when T >= Now, Timestamp),
  ?assertMatch(T when T < Now + 10000, Timestamp).

t_response() ->
  ?assertEqual({ok, [#{name => <<"topic">>,
                       partitions => [#{error_code => none, offset => 5, partition => 0}]}]},
               kafe_protocol_produce:response(
                 <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                   0, 0, 5>>,
                 #{api_version => 0})).

