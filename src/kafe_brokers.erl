% @hidden
-module(kafe_brokers).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_statem).
-include("../include/kafe.hrl").
-include_lib("kernel/include/inet.hrl").

-export([
         start_link/0,
         first_broker/0,
         first_broker/1,
         broker_id_by_topic_and_partition/2,
         broker_by_name/1,
         broker_by_host_and_port/2,
         broker_by_id/1,
         release_broker/1,
         topics/0,
         partitions/1,
         update/0,
         list/0,
         size/0,
         api_version/0,
         api_version/1
        ]).

-export([
         init/1,
         callback_mode/0,
         retrieve/3,
         terminate/3,
         code_change/4
        ]).

-define(SERVER, ?MODULE).
-define(ETS_TABLE, kafe_brokers).
-record(state, {
          default_brokers = [],
          brokers_update_frequency = ?DEFAULT_BROKER_UPDATE,
          pool_size = ?DEFAULT_POOL_SIZE,
          chunk_pool_size = ?DEFAULT_CHUNK_POOL_SIZE
         }).

% @hidden
start_link() ->
  gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

% @equiv first_broker(true)
-spec first_broker() -> pid() | undefined.
first_broker() ->
  first_broker(true).

% @doc
% Return the PID for the first available broker
%
% If <tt>Retrieve</tt> is set to true and there is not broker available, we will ask to reload the broker list and retry get a broker.
% @end
-spec first_broker(Retrieve :: true | false) -> pid() | undefined.
first_broker(Retrieve) when is_boolean(Retrieve) ->
  case get_first_broker() of
    {ok, Broker} ->
      Broker;
    {error, Reason} ->
      if
        Retrieve ->
          gen_statem:call(?SERVER, retrieve),
          first_broker(false);
        true ->
          lager:error("Get broker failed: ~p", [Reason]),
          undefined
      end
  end.

% @doc
% Return a broker ID for the given <tt>Topic</tt> and <tt>Partition</tt>.
% @end
-spec broker_id_by_topic_and_partition(Topic :: binary(), Partition :: integer()) -> atom() | undefined.
broker_id_by_topic_and_partition(Topic, Partition) ->
  case {ets_get(?ETS_TABLE, topics, undefined),
        ets_get(?ETS_TABLE, brokers, undefined)} of
    {#{}, #{}} = {Topics, Brokers} ->
      case maps:get(Topic, Topics, undefined) of
        undefined ->
          undefined;
        Partitions ->
          case maps:get(Partition, Partitions, undefined) of
            undefined ->
              undefined;
            BrokerName ->
              maps:get(BrokerName, Brokers, undefined)
          end
      end;
    _ ->
      undefined
  end.

% @doc
% Return a broker PID for the given <tt>BrokerName</tt>.
% @end
-spec broker_by_name(BrokerName :: string()) -> pid() | undefined.
broker_by_name(BrokerName) ->
  case ets_get(?ETS_TABLE, brokers, undefined) of
    undefined ->
      undefined;
    Brokers ->
      case maps:get(BrokerName, Brokers, undefined) of
        undefined ->
          undefined;
        BrokerID ->
          checkout_broker(BrokerID)
      end
  end.

% @doc
% Return a broker PID for the given <tt>Host</tt> and <tt>Port</tt>.
% @end
-spec broker_by_host_and_port(Host :: string() | binary(),
                              Port ::integer() | port()) -> pid() | undefined.
broker_by_host_and_port(Host, Port) ->
  broker_by_name(kafe_utils:broker_name(Host, Port)).

% @doc
% Return a broker PID for the given broker ID.
% @end
-spec broker_by_id(BrokerID :: atom()) -> pid() | undefined.
broker_by_id(BrokerID) ->
  checkout_broker(BrokerID).

% @doc
% Release the given broker (given by PID).
% @end
-spec release_broker(BrokerPID :: pid()) -> ok.
release_broker(BrokerPID) ->
  case poolgirl:checkin(BrokerPID) of
    ok -> ok;
    {error, Error} ->
      lager:error("Checkin broker ~p failed: ~p", [BrokerPID, Error]),
      ok
  end.

% @doc
% Return the list of availables topics
% @end
-spec topics() -> map().
topics() ->
  ets_get(?ETS_TABLE, topics, #{}).

% @doc
% Return the list of availables partitions for the given <tt>Topic</tt>.
% @end
-spec partitions(Topic :: binary()) -> [integer()].
partitions(Topic) ->
  case maps:get(Topic, topics(), undefined) of
    undefined -> error(unknown_topic, [Topic]);
    Partitions -> maps:keys(Partitions)
  end.

% @doc
% Update brokers
% @end
-spec update() -> ok.
update() ->
  gen_statem:call(?SERVER, retrieve).

% @doc
% Return the list of availables brokers
% @end
-spec list() -> [string()].
list() ->
  ets_get(?ETS_TABLE, brokers_list, []).

% @doc
% Return the number of availables brokers
% @end
-spec size() -> integer().
size() ->
  length(list()).

% @doc
% Return the configured API version
% @end
api_version() ->
  ets_get(?ETS_TABLE, api_version, -1).

% @doc
% Return the API Version for the given API Key
% @end
api_version(ApiKey) ->
  api_version(ApiKey, api_version()).

api_version(ApiKey, auto) ->
  buclists:keyfind(
    ApiKey, 1,
    case ets_get(?ETS_TABLE, api_versions, undefined) of
      undefined ->
        case kafe:api_versions() of
          {ok, #{api_versions := Versions,
                 error_code := none}} ->
            ApiVersionsList = [{Key, Version}
                               || #{api_key := Key,
                                     max_version := Version} <- Versions],
            ets:insert(?ETS_TABLE, [{api_versions, ApiVersionsList}]),
            ApiVersionsList;
          _ ->
            []
        end;
      List ->
        List
    end,
    -1);
api_version(_, Version) when is_integer(Version) ->
  Version;
api_version(ApiKey, Versions) when is_list(Versions) ->
  buclists:keyfind(ApiKey, 1, Versions, -1).

% @hidden
init(_) ->
  lager:debug("Start ~p", [?MODULE]),
  process_flag(trap_exit, true),
  ets:new(?ETS_TABLE, [public, named_table]),
  ets:insert(?ETS_TABLE, [{api_version, doteki:get_env([kafe, api_version], ?DEFAULT_API_VERSION)}]),
  Timeout = doteki:get_env(
              [kafe, brokers_update_frequency],
              ?DEFAULT_BROKER_UPDATE),
  State = #state{
             default_brokers = doteki:get_env(
                                 [kafe, brokers],
                                 [{doteki:get_env([kafe, host], ?DEFAULT_IP),
                                   doteki:get_env([kafe, port], ?DEFAULT_PORT)}]),
             brokers_update_frequency = Timeout,
             pool_size = doteki:get_env(
                           [kafe, pool_size],
                           ?DEFAULT_POOL_SIZE),
             chunk_pool_size = doteki:get_env(
                                 [kafe, chunk_pool_size],
                                 ?DEFAULT_CHUNK_POOL_SIZE)
            },
  retrieve_brokers(State),
  {ok, retrieve, State, Timeout}.

% @hidden
callback_mode() ->
    state_functions.

% @hidden
retrieve(timeout, _, #state{brokers_update_frequency = Timeout} = State) ->
  lager:debug("Try to retrieve brokers after timeout"),
  retrieve_brokers(State),
  {next_state, retrieve, State, Timeout};
retrieve({call, From}, retrieve, #state{brokers_update_frequency = Timeout} = State) ->
  lager:debug("~p ask to retrieve brokers...", [From]),
  retrieve_brokers(State),
  {next_state, retrieve, State, [{reply, From, ok}, Timeout]};
retrieve({call, From}, Event, #state{brokers_update_frequency = Timeout} = State) ->
  lager:debug("~p ask for ~p: invalid event", [From, Event]),
  {next_state, retrieve, State, [{reply, From, {error, invalid_event}}, Timeout]}.

% @hidden
terminate(_Reason, _StateName, _State) ->
  remove_all_pools(),
  ets:delete(?ETS_TABLE),
  ok.

% @hidden
code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

% -- Private --

% Rescan brokers and update the ets table (?ETS_TABLE)
-spec retrieve_brokers(State :: #state{}) -> ok | {error, term()}.
retrieve_brokers(#state{default_brokers = Brokers,
                        pool_size = PoolSize,
                        chunk_pool_size = ChunkPoolSize}) ->
  lager:debug("Start retrieve brokers..."),
  remove_dead_brokers(),
  case ets_get(?ETS_TABLE, brokers_list, []) of
    [] ->
      get_connections(Brokers, PoolSize, ChunkPoolSize);
    _ ->
      ok
  end,
  update_state_with_metadata(PoolSize, ChunkPoolSize).

% Start brokers connections from configuration
get_connections([], _, _) ->
  ok;
get_connections([{Host, Port}|Rest], PoolSize, ChunkPoolSize) ->
  Brokers = ets_get(?ETS_TABLE, brokers, #{}),
  BrokersList = ets_get(?ETS_TABLE, brokers_list, []),
  lager:debug("Get connection for ~s:~p", [Host, Port]),
  try
    case inet:gethostbyname(bucs:to_string(Host)) of
      {ok, #hostent{h_name = Hostname,
                    h_addrtype = AddrType,
                    h_addr_list = AddrsList}} ->
        lager:debug("[get_connections] Got resolved ~p to ~p", [Host, Hostname]),
        lager:debug("[get_connections] AddrsList ~p", [AddrsList]),
        case get_host(AddrsList, Host, AddrType) of
          undefined ->
            lager:warning("Can't retrieve host for ~s:~p", [Host, Port]),
            get_connections(Rest, PoolSize, ChunkPoolSize);
          {BrokerAddr, BrokerHostList} ->
            lager:debug("[get_connections] will now connect to ~p for Hostname: ~p", [BrokerAddr, Host]),
            lager:debug("[get_connections] after get_host BrokerHostList is ~p",  [BrokerHostList]),
            case lists:foldl(fun(E, Acc) ->
                                 BrokerFullName = kafe_utils:broker_name(E, Port),
                                 case lists:member(BrokerFullName, BrokersList) of
                                   true -> Acc;
                                   _ -> [BrokerFullName|Acc]
                                 end
                             end, [], BrokerHostList) of
              [] ->
                lager:debug("All hosts already registered for ~s:~p", [bucinet:ip_to_string(BrokerAddr), Port]),
                get_connections(Rest, PoolSize, ChunkPoolSize);
              BrokerHostList1 ->
                IP = bucinet:ip_to_string(BrokerAddr),
                BrokerID = kafe_utils:broker_id(IP, Port),
                case poolgirl:size(BrokerID) of
                  {ok, N, A} when N > 0 ->
                    lager:debug("Pool ~s size ~p/~p", [BrokerID, N, A]),
                    Brokers1 = lists:foldl(fun(BrokerHost, Acc) ->
                                               maps:put(BrokerHost, BrokerID, Acc)
                                           end, Brokers, BrokerHostList1),
                    ets:insert(?ETS_TABLE, [{brokers, Brokers1},
                                            {brokers_list, lists:usort(BrokerHostList1 ++ BrokersList)}]),
                    get_connections(Rest, PoolSize, ChunkPoolSize);
                  _ ->
                    case poolgirl:add_pool(BrokerID,
                                           {kafe_conn, start_link, [BrokerAddr, Port]},
                                           #{size => PoolSize,
                                             chunk_size => ChunkPoolSize,
                                             allow_empty_pool => false}) of
                      {ok, PoolSize1} ->
                        lager:debug("Broker pool ~s (size ~p) reference ~p", [BrokerID, PoolSize1, BrokerHostList1]),
                        Brokers1 = lists:foldl(fun(BrokerHost, Acc) ->
                                                   maps:put(BrokerHost, BrokerID, Acc)
                                               end, Brokers, BrokerHostList1),
                        ets:insert(?ETS_TABLE, [{brokers, Brokers1},
                                                {brokers_list, lists:usort(BrokerHostList1 ++ BrokersList)}]),
                        get_connections(Rest, PoolSize, ChunkPoolSize);
                      {error, Reason} ->
                        lager:warning("Connection failed to ~p:~p : ~p", [bucinet:ip_to_string(BrokerAddr), Port, Reason]),
                        get_connections(Rest, PoolSize, ChunkPoolSize)
                    end
                end
            end
        end;
      {error, Reason} ->
        lager:warning("Can't retrieve host by name for ~s:~p : ~p", [Host, Port, Reason]),
        get_connections(Rest, PoolSize, ChunkPoolSize)
    end
  catch
    Type:Reason1 ->
      lager:warning("Error while getting connection for ~s:~p : ~p:~p", [Host, Port, Type, Reason1]),
      get_connections(Rest, PoolSize, ChunkPoolSize)
  end.

get_host([], _, _) -> undefined;
get_host([Addr|Rest], Hostname, AddrType) ->
  case inet:getaddr(Hostname, AddrType) of
    {ok, Addr} ->
     % case inet:gethostbyaddr(Addr) of
     %   {ok, #hostent{h_name = _Hostname1, h_aliases = _HostAlias}} ->
     %     %{Addr, lists:usort([Hostname|[Hostname1|HostAlias]])};
     %     {Addr, [Hostname]};
     %   _ ->
     %     {Addr, [Hostname]}
     % end;
      {Addr, [Hostname]};
    _ -> get_host(Rest, Hostname, AddrType)
  end.

remove_all_pools() ->
  Brokers = ets_get(?ETS_TABLE, brokers, #{}),
  lager:debug("Removing all pools: ~p", [Brokers]),
  lists:foreach(fun poolgirl:remove_pool/1, maps:values(Brokers)).


% Remove dead brokers
remove_dead_brokers() ->
  BrokersList = ets_get(?ETS_TABLE, brokers_list, []),
  Brokers = ets_get(?ETS_TABLE, brokers, #{}),
  {BrokersList0,
   Brokers0} = lists:foldl(fun(Broker, {BrokersListAcc, BrokersAcc}) ->
                               case maps:get(Broker, BrokersAcc, undefined) of
                                 undefined ->
                                   {lists:delete(Broker, BrokersListAcc), BrokersAcc};
                                 BrokerID ->
                                   case poolgirl:checkout(BrokerID) of
                                     {ok, BrokerPID} ->
                                       case is_broker_alive(BrokerPID) of
                                         true ->
                                           poolgirl:checkin(BrokerPID),
                                           {BrokersListAcc, BrokersAcc};
                                         false ->
                                           poolgirl:checkin(BrokerPID),
                                           poolgirl:remove_pool(BrokerID),
                                           {lists:delete(Broker, BrokersListAcc),
                                            maps:remove(Broker, BrokersAcc)}
                                       end;
                                     _ ->
                                       {lists:delete(Broker, BrokersListAcc),
                                        BrokersAcc}
                                   end
                               end
                           end, {BrokersList, Brokers}, BrokersList),
  ets:insert(?ETS_TABLE, [{brokers, Brokers0},
                          {brokers_list, BrokersList0}]).

update_state_with_metadata(PoolSize, ChunkPoolSize) ->
  case kafe:metadata() of
    {ok, #{brokers := Brokers,
           topics := Topics}} ->
      lager:info("[update_state_with_metadata] Brokers ~p", [Brokers]),
      BrokersByID = maps:from_list(
                   [ begin
                       get_connections([{bucs:to_string(Host), Port}], PoolSize, ChunkPoolSize),
                       {ID, kafe_utils:broker_name(Host, Port)}
                     end ||
                     #{id := ID, host := Host, port := Port} <- Brokers ]),
      lager:info("BrokersByID ~p", [BrokersByID]),
      remove_unlisted_brokers(maps:values(BrokersByID)),
      TopicsWithLeaders = leaders_for_topics(Topics, BrokersByID),
      ets:insert(?ETS_TABLE, [{topics, TopicsWithLeaders}]);
    {error, Reason} ->
      lager:warning("Get kafka metadata failed: ~p", [Reason])
  end.

remove_unlisted_brokers(NewBrokersList) ->
  Brokers = ets_get(?ETS_TABLE, brokers, #{}),
  lager:debug("Brokers = ~p", [Brokers]),
  lager:debug("NewBrokersList = ~p", [NewBrokersList]),
  NewBrokers = lists:foldr(fun(BrokerName, Acc) ->
                              lager:debug("Getting broker by name ~p", [BrokerName]),
                               case maps:get(BrokerName, Brokers, undefined) of
                                 undefined -> Acc;
                                 Broker -> maps:put(BrokerName, Broker, Acc)
                               end
                           end, #{}, NewBrokersList),
  lager:debug("old brokers = ~p, new brokers = ~p", [Brokers, NewBrokers]),
  [begin
     case lists:member(BrokerID, maps:values(NewBrokers)) of
       false ->
         lager:debug("remove broker ~p", [BrokerID]),
         poolgirl:remove_pool(BrokerID);
       _ ->
         ok
     end
   end || BrokerID <- lists:usort(maps:values(Brokers))],
  ets:insert(?ETS_TABLE, [{brokers, NewBrokers},
                          {brokers_list, NewBrokersList}]).

leaders_for_topics(Topics, BrokersByID) ->
  maps:from_list(
    [ {Topic, leaders_for_partitions(Topic, Partitions, BrokersByID)} ||
      #{name := Topic, partitions := Partitions} <- Topics ]).

leaders_for_partitions(Topic, Partitions, BrokersByID) ->
  maps:from_list(
    [ {PartitionID,
       case Leader of
         -1 ->
           lager:warning("Leader not available for topic ~s, partition ~p", [Topic, PartitionID]),
           leader_not_available;
         _ ->
           maps:get(Leader, BrokersByID)
       end}
       || #{id := PartitionID, leader := Leader} <- Partitions ]).

% Return the first available (alive) broker or undefined
-spec get_first_broker() -> {ok, pid()} | {error, term()}.
get_first_broker() ->
  case ets:lookup(?ETS_TABLE, brokers) of
    [{brokers, Brokers}] ->
      get_first_broker(maps:values(Brokers));
    _ ->
      {error, undefined}
  end.
get_first_broker([]) ->
  {error, no_available_broker};
get_first_broker([BrokerID|Rest]) ->
  case checkout_broker(BrokerID) of
    undefined ->
      get_first_broker(Rest);
    Broker ->
      {ok, Broker}
  end.

checkout_broker(BrokerID) ->
  case poolgirl:checkout(BrokerID) of
    {ok, Broker} ->
      case is_broker_alive(Broker) of
        true ->
          Broker;
        false ->
          _ = poolgirl:checkin(Broker),
          lager:warning("Broker ~p (~s) is not alive", [Broker, BrokerID]),
          undefined
      end;
    {error, Reason} ->
      lager:error("Can't checkout broker from pool ~s: ~p", [BrokerID, Reason]),
      undefined
  end.

% Check if the given broker (PID) is alive
-spec is_broker_alive(pid()) -> true | false.
is_broker_alive(BrokerPid) ->
  try
    ok =:= gen_server:call(BrokerPid, alive, ?TIMEOUT)
  catch
    Type:Error ->
      lager:error("Check if broker is alive failed: ~p:~p", [Type, Error]),
      false
  end.

ets_get(Table, Key, Default) ->
  case ets:lookup(Table, Key) of
    [{Key, Value}] ->
      Value;
    _ ->
      Default
  end.
