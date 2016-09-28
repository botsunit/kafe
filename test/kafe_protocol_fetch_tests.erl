-module(kafe_protocol_fetch_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").

kafe_protocol_fetch_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request()),
    ?_test(t_response()),
    ?_test(t_incomplete_response()),
    ?_test(t_response_incomplete_final_message())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_request() ->
  ?assertEqual(#{api_version => 0,
                 packet => <<0, 0, 0, 57, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 255, 255, 0, 0, 0,
                             1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                             0, 0, 0, 0, 16, 0, 0>>,
                 state => ?REQ_STATE2(1, 0)},
     kafe_protocol_fetch:request(-1, <<"topic">>, #{offset => 0}, ?REQ_STATE2(0, 0))),
  ?assertEqual(#{api_version => 1,
                 packet => <<0, 0, 0, 57, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 255, 255, 0, 0, 0,
                             1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                             0, 0, 0, 0, 16, 0, 0>>,
                 state => ?REQ_STATE2(1, 1)},
     kafe_protocol_fetch:request(-1, <<"topic">>, #{offset => 0}, ?REQ_STATE2(0, 1))),
  ?assertEqual(#{api_version => 2,
                 packet => <<0, 0, 0, 57, 0, 1, 0, 2, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 255, 255, 0, 0, 0,
                             1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                             0, 0, 0, 0, 16, 0, 0>>,
                 state => ?REQ_STATE2(1, 2)},
     kafe_protocol_fetch:request(-1, <<"topic">>, #{offset => 0}, ?REQ_STATE2(0, 2))).

t_response() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
             partitions => [#{error_code => none,
                              high_watermark_offset => 5,
                              messages => [#{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 0,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 1,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 2,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 3,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 4,
                                             value => <<"hello world">>}],
                              partition => 0}]}]},
     kafe_protocol_fetch:response(
       <<1:32                       % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , ((8+4+25)*5):32      % message set size
                % offset, size, crc, magic, attributes, key, value
                , 0:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
                , 1:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
                , 2:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
                , 3:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
                , 4:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
         >>, 0)).

t_incomplete_response() ->
  ?assertEqual({error, incomplete_data},
               kafe_protocol_fetch:response(<<>>, 0)),
  ?assertEqual({error, incomplete_data},
               kafe_protocol_fetch:response(<<0>>, 0)),
  ?assertEqual({error, incomplete_data},
               kafe_protocol_fetch:response(
                 <<1:32                       % topic count
                     , 5:16, "topic"          % topic name
                     , 1:32                   % partition count
                       , 0:32                 % partition index
                       , 0:16                 % error code
                       , 5:64                 % highwater mark
                       , ((8+4+25)*5):32      % message set size
                          , 0 % message set truncated, does not match size
                 >>, 0)).

t_response_incomplete_final_message_is_ignored() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
             partitions => [#{error_code => none,
                              high_watermark_offset => 5,
                              messages => [#{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 0,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 1,
                                             value => <<"hello world">>}
                                          ],
                              partition => 0}]}]},
     kafe_protocol_fetch:response(
       <<1:32                       % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , ((8+4+25)*2 + 1):32  % message set size
                % offset, size, crc, magic, attributes, key, value
                , 0:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
                , 1:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
                , 0 % message truncated after 1 byte
         >>, 0)).
