-module(kafe_consumer_tests).

-include_lib("eunit/include/eunit.hrl").

kafe_consumer_topics_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, topics, [{<<"topic1">>, [0, 1, 2]},
                                                          {<<"topic2">>, [0, 1]}])
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertEqual([{<<"topic1">>, 0},
                      {<<"topic1">>, 1},
                      {<<"topic1">>, 2},
                      {<<"topic2">>, 0},
                      {<<"topic2">>, 1}],
                     kafe_consumer:topics(<<"test_cg">>)),
        ?assertEqual([], kafe_consumer:topics(<<"invalid_cg">>))
    end
   ]}.

kafe_consumer_generation_id_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, generation_id, 1)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertEqual(1, kafe_consumer:generation_id(<<"test_cg">>))
    end
   ]}.

kafe_consumer_member_id_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, member_id, <<"member_id">>)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertEqual(<<"member_id">>, kafe_consumer:member_id(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_fetch_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assert(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_not_fetch_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, false)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertNot(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_not_fetch_undef_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:new(<<"test_cg">>)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertNot(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_encode_decode_group_commit_identifier_test_() ->
  {setup,
   fun() ->
       ok
   end,
   fun(_) ->
       ok
   end,
   [
    fun() ->
        GCI = kafe_consumer:encode_group_commit_identifier(self(),
                                                           <<"topic">>,
                                                           0,
                                                           9999,
                                                           <<"group_id">>,
                                                           1,
                                                           <<"member_id">>),
        {Pid,
         Topic,
         Partition,
         Offset,
         GroupID,
         GenerationID,
         MemberID} = kafe_consumer:decode_group_commit_identifier(GCI),
        ?assertEqual(self(), Pid),
        ?assertEqual(<<"topic">>, Topic),
        ?assertEqual(0, Partition),
        ?assertEqual(9999, Offset),
        ?assertEqual(<<"group_id">>, GroupID),
        ?assertEqual(1, GenerationID),
        ?assertEqual(<<"member_id">>, MemberID)
    end
   ]}.

