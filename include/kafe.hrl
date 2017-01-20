-define(DEFAULT_IP, "127.0.0.1").
-define(DEFAULT_PORT, 9092).
-define(DEFAULT_CLIENT_ID, <<"kafe">>).
-define(DEFAULT_CORRELATION_ID, 0).
-define(DEFAULT_API_VERSION, 1).
-define(DEFAULT_OFFSET, 0).
-define(DEFAULT_BROKER_UPDATE, 60000).
-define(DEFAULT_SOCKET_SNDBUF, 4194304).
-define(DEFAULT_SOCKET_RECBUF, 4194304).
-define(DEFAULT_POOL_SIZE, 5).
-define(DEFAULT_CHUNK_POOL_SIZE, 10).
-define(DEFAULT_PROTOCOL_TIMEOUT, 60000).

-define(TIMEOUT, doteki:get_env([kafe, protocol_timeout], ?DEFAULT_PROTOCOL_TIMEOUT)).

-define(V0, 0).
-define(V1, 1).
-define(V2, 2).

-define(PRODUCE_REQUEST, 0).
-define(FETCH_REQUEST, 1).
-define(OFFSET_REQUEST, 2).
-define(METADATA_REQUEST, 3).
-define(LEADER_AND_ISR_REQUEST, 4).
-define(STOP_REPLICA_REQUEST, 5).
-define(UPDATE_METADATA_REQUEST, 6).
-define(CONTROLLED_SHUTDOWN_REQUEST, 7).
-define(OFFSET_COMMIT_REQUEST, 8).
-define(OFFSET_FETCH_REQUEST, 9).
-define(GROUP_COORDINATOR_REQUEST, 10).
-define(JOIN_GROUP_REQUEST, 11).
-define(HEARTBEAT_REQUEST, 12).
-define(LEAVE_GROUP_REQUEST, 13).
-define(SYNC_GROUP_REQUEST, 14).
-define(DESCRIBE_GROUPS_REQUEST, 15).
-define(LIST_GROUPS_REQUEST, 16).

-define(DEFAULT_OFFSET_PARTITION, 0).
-define(DEFAULT_OFFSET_TIMESTAMP, -1).
-define(DEFAULT_OFFSET_MAX_NUM_OFFSETS, 65535).

-define(DEFAULT_PRODUCE_REQUIRED_ACKS, -1).
-define(DEFAULT_PRODUCE_SYNC_TIMEOUT, 5000).

-define(DEFAULT_FETCH_PARTITION, 0).
-define(DEFAULT_FETCH_MAX_BYTES, 1024*1024).
-define(DEFAULT_FETCH_MIN_BYTES, 1).
-define(DEFAULT_FETCH_MAX_WAIT_TIME, 100).

-define(DEFAULT_GROUP_PROTOCOL_VERSION, 0).
-define(DEFAULT_GROUP_PROTOCOL_NAME, <<"default_protocol">>).
-define(DEFAULT_GROUP_USER_DATA, <<>>).

-define(DEFAULT_GROUP_PROTOCOL_TOPICS, lists:delete(<<"__consumer_offsets">>, maps:keys(kafe:topics()))).
-define(DEFAULT_GROUP_PARTITION_ASSIGNMENT, maps:fold(fun
                                                        (<<"__consumer_offsets">>, _, Acc@DGPA) ->
                                                          Acc@DGPA;
                                                        (Topic, Partitions, Acc@DGPA) ->
                                                          [#{topic => Topic,
                                                             partitions => maps:keys(Partitions)}|Acc@DGPA]
                                                      end, [], kafe:topics())).

-define(DEFAULT_JOIN_GROUP_SESSION_TIMEOUT, 30000).
-define(DEFAULT_JOIN_GROUP_MEMBER_ID, <<>>).
-define(DEFAULT_JOIN_GROUP_PROTOCOL_TYPE, <<"consumer">>).
-define(DEFAULT_JOIN_GROUP_PROTOCOLS, [kafe:default_protocol(
                                         ?DEFAULT_GROUP_PROTOCOL_NAME,
                                         ?DEFAULT_GROUP_PROTOCOL_VERSION,
                                         ?DEFAULT_GROUP_PROTOCOL_TOPICS,
                                         ?DEFAULT_GROUP_USER_DATA)]).

-define(DEFAULT_CONSUMER_FETCH_INTERVAL, 10).
-define(DEFAULT_CONSUMER_COMMIT_INTERVAL, 1000).
-define(DEFAULT_CONSUMER_COMMIT, [after_processing, {interval, ?DEFAULT_CONSUMER_COMMIT_INTERVAL}]).
-define(DEFAULT_CONSUMER_ALLOW_UNORDERED_COMMIT, false).
-define(DEFAULT_CONSUMER_COMMIT_RETRY, 0).
-define(DEFAULT_CONSUMER_COMMIT_DELAY, 100).
-define(DEFAULT_CONSUMER_COMMIT_ATTEMPTS, 10).
-define(DEFAULT_CONSUMER_ON_START_FETCHING, undefined).
-define(DEFAULT_CONSUMER_ON_STOP_FETCHING, undefined).
-define(DEFAULT_CONSUMER_ON_ASSIGNMENT_CHANGE, undefined).
-define(DEFAULT_CONSUMER_CAN_FETCH, undefined).
-define(DEFAULT_CONSUMER_START_FROM_BEGINNING, true).

-define(DEFAULT_CONSUMER_FETCH_ERROR_ACTIONS, #{'*' => error}).
-define(DEFAULT_CONSUMER_ERRORS_ACTIONS, #{fetch => #{'*' => error}}).

-define(TRY(M, F, A, D),
        try
          erlang:apply(M, F, A)
        catch
          C:E ->
            error_logger:error_msg("Error while calling ~p:~p(~p): ~p:~p", [M, F, A, C, E]),
            D
        end).

