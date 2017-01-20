% @hidden
-module(kafe_brokers).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_fsm).
-include("../include/kafe.hrl").
-include_lib("kernel/include/inet.hrl").

-export([
         start_link/0,
         first_broker/0,
         first_broker/1,
         broker_by_topic_and_partition/2,
         broker_id_by_topic_and_partition/2,
         broker_by_name/1,
         broker_by_host_and_port/2,
         broker_by_id/1,
         release_broker/1,
         topics/0,
         partitions/1,
         update/0,
         list/0,
         size/0
        ]).

-export([
         init/1,
         retrieve/2,
         retrieve/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
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
  gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

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
          gen_fsm:sync_send_event(?SERVER, retrieve),
          first_broker(false);
        true ->
          lager:error("Get broker failed: ~p", [Reason]),
          undefined
      end
  end.

% @doc
% Return a broker PID for the given <tt>Topic</tt> and <tt>Partition</tt>.
% @end
-spec broker_by_topic_and_partition(Topic :: binary(), Partition :: integer()) -> pid() | undefined.
broker_by_topic_and_partition(Topic, Partition) ->
  case broker_id_by_topic_and_partition(Topic, Partition) of
    undefined -> undefined;
    BrokerID -> checkout_broker(BrokerID)
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
-spec broker_by_host_and_port(Host :: string(), Port ::integer()) -> pid() | undefined.
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
  maps:keys(maps:get(Topic, ets_get(?ETS_TABLE, topics, #{}), #{})).

% @doc
% Update brokers
% @end
-spec update() -> ok.
update() ->
  gen_fsm:sync_send_event(?SERVER, retrieve).

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

% @hidden
init(_) ->
  ets:new(?ETS_TABLE, [public, named_table]),
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
retrieve(timeout, #state{brokers_update_frequency = Timeout} = State) ->
  retrieve_brokers(State),
  {next_state, retrieve, State, Timeout}.

% @hidden
retrieve(retrieve, _From, #state{brokers_update_frequency = Timeout} = State) ->
  retrieve_brokers(State),
  {reply, ok, retrieve, State, Timeout};
retrieve(_, _From, #state{brokers_update_frequency = Timeout} = State) ->
  {reply, {error, invalid_demand}, retrieve, State, Timeout}.

% @hidden
handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

% @hidden
handle_sync_event(_Event, _From, StateName, State) ->
  {reply, ok, StateName, State}.

% @hidden
handle_info(_Info, StateName, State) ->
  {next_state, StateName, State}.

% @hidden
terminate(_Reason, _StateName, _State) ->
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
        case get_host(AddrsList, Hostname, AddrType) of
          undefined ->
            lager:warning("Can't retrieve host for ~s:~p", [Host, Port]),
            get_connections(Rest, PoolSize, ChunkPoolSize);
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
                get_connections(Rest, PoolSize, ChunkPoolSize);
              BrokerHostList1 ->
                IP = bucinet:ip_to_string(BrokerAddr),
                BrokerID = kafe_utils:broker_id(IP, Port),
                case poolgirl:size(BrokerID) of
                  {ok, N, A} when N > 0 ->
                    lager:debug("Pool ~s size ~p/~p", [BrokerID, N, A]),
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
                                                {brokers_list, BrokerHostList1 ++ BrokersList}]),
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
      case inet:gethostbyaddr(Addr) of
        {ok, #hostent{h_name = Hostname1, h_aliases = HostAlias}} ->
          {Addr, lists:usort([Hostname|[Hostname1|HostAlias]])};
        _ ->
          {Addr, [Hostname]}
      end;
    _ -> get_host(Rest, Hostname, AddrType)
  end.

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
      Brokers1 = lists:foldl(fun(#{host := Host, id := ID, port := Port}, Acc) ->
                                 get_connections([{bucs:to_string(Host), Port}], PoolSize, ChunkPoolSize),
                                 maps:put(ID, kafe_utils:broker_name(Host, Port), Acc)
                             end, #{}, Brokers),
      remove_unlisted_brokers(maps:values(Brokers1)),
      case update_topics(Topics, Brokers1) of
        leader_election ->
          timer:sleep(1000),
          update_state_with_metadata(PoolSize, ChunkPoolSize);
        Topics1 ->
          ets:insert(?ETS_TABLE, [{topics, Topics1}])
      end;
    {error, Reason} ->
      lager:warning("Get kafka metadata failed: ~p", [Reason])
  end.

remove_unlisted_brokers(NewBrokersList) ->
  Brokers = ets_get(?ETS_TABLE, brokers, #{}),
  NewBrokers = [{BrokerName, maps:get(BrokerName, Brokers)} || BrokerName <- NewBrokersList],
  [begin
     case lists:keyfind(BrokerID, 2, NewBrokers) of
       false ->
         poolgirl:remove_pool(BrokerID);
       _ ->
         ok
     end
   end || BrokerID <- lists:usort(maps:values(Brokers))],
  ets:insert(?ETS_TABLE, [{brokers, maps:from_list(NewBrokers)},
                          {brokers_list, NewBrokersList}]).

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
  lager:debug("Leader election in progress for topic ~s, partition ~p", [Topic, ID]),
  leader_election;
brokers_for_partitions([#{id := ID, leader := Leader}|Rest], Brokers1, Topic, Acc) ->
  brokers_for_partitions(Rest, Brokers1, Topic,
                         maps:put(ID, maps:get(Leader, Brokers1), Acc)).

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
          lager:warning("Broker ~s is not alive", [Broker]),
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

