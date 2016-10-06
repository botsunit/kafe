% @hidden
-module(kafe_metrics).

-export([
         init_consumer/1
         , delete_consumer/1
         , consumer_messages/2

         , init_consumer_partition/3
         , delete_consumer_partition/3
         , consumer_partition_messages/4
         , consumer_partition_duration/4

         , ensure_metrics_mod_started/0
        ]).

ensure_metrics_mod_started() ->
  ensure_metrics_mod_started(metrics:backend()).
ensure_metrics_mod_started(metrics_folsom) ->
  application:ensure_all_started(folsom);
ensure_metrics_mod_started(metrics_exometer) ->
  application:ensure_all_started(exometer);
ensure_metrics_mod_started(metrics_grapherl) ->
  application:ensure_all_started(grapherl);
ensure_metrics_mod_started(_) ->
  ok.

init_consumer(Consumer) ->
  ensure_metrics_mod_started(),
  metrics:new(gauge, consumer_metric(Consumer, <<"messages.fetch">>)),
  metrics:new(counter, consumer_metric(Consumer, <<"messages">>)).

delete_consumer(Consumer) ->
  metrics:delete(consumer_metric(Consumer, <<"messages.fetch">>)),
  metrics:delete(consumer_metric(Consumer, <<"messages">>)).

consumer_messages(Consumer, NbMessages) ->
  metrics:update(consumer_metric(Consumer, <<"messages.fetch">>), NbMessages),
  metrics:update(consumer_metric(Consumer, <<"messages">>), {c, NbMessages}).

consumer_metric(Consumer, Ext) ->
  Prefix = case doteki:get_as_binary([metrics, metrics_prefix], <<>>) of
    <<>> -> <<>>;
    Other -> <<Other/binary, ".">>
  end,
  bucs:to_string(<<Prefix/binary,
                   "kafe_consumer.",
                   (bucs:to_binary(Consumer))/binary,
                   ".", Ext/binary>>).

init_consumer_partition(Consumer, Topic, Partition) ->
  ensure_metrics_mod_started(),
  metrics:new(gauge, consumer_partition_metric(Consumer, Topic, Partition, <<"messages.fetch">>)),
  metrics:new(gauge, consumer_partition_metric(Consumer, Topic, Partition, <<"duration.fetch">>)),
  metrics:new(counter, consumer_partition_metric(Consumer, Topic, Partition, <<"messages">>)).

delete_consumer_partition(Consumer, Topic, Partition) ->
  metrics:delete(consumer_partition_metric(Consumer, Topic, Partition, <<"messages.fetch">>)),
  metrics:delete(consumer_partition_metric(Consumer, Topic, Partition, <<"duration.fetch">>)),
  metrics:delete(consumer_partition_metric(Consumer, Topic, Partition, <<"messages">>)).

consumer_partition_messages(Consumer, Topic, Partition, NbMessages) ->
  metrics:update(consumer_partition_metric(Consumer, Topic, Partition, <<"messages.fetch">>),
                 NbMessages),
  metrics:update(consumer_partition_metric(Consumer, Topic, Partition, <<"messages">>),
                 {c, NbMessages}).

consumer_partition_duration(Consumer, Topic, Partition, Duration) ->
  metrics:update(consumer_partition_metric(Consumer, Topic, Partition, <<"duration.fetch">>),
                 Duration).

consumer_partition_metric(Consumer, Topic, Partition, Ext) ->
  Prefix = case doteki:get_as_binary([metrics, metrics_prefix], <<>>) of
    <<>> -> <<>>;
    Other -> <<Other/binary, ".">>
  end,
  bucs:to_string(<<Prefix/binary,
                   "kafe_consumer.",
                   (bucs:to_binary(Consumer))/binary,
                   ".", (bucs:to_binary(Topic))/binary,
                   ".", (bucs:to_binary(Partition))/binary,
                   ".", Ext/binary>>).
