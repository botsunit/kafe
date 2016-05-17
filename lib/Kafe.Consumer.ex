# File: lib/Kafe.Consumer.ex
# This file was generated from src/kafe_consumer.erl
# Using mix.mk (https://github.com/botsunit/mix.mk)
# MODIFY IT AT YOUR OWN RISK AND ONLY IF YOU KNOW WHAT YOU ARE DOING!
defmodule Kafe.Consumer do
	def unquote(:"start")(arg1, arg2, arg3) do
		:erlang.apply(:"kafe_consumer", :"start", [arg1, arg2, arg3])
	end
	def unquote(:"stop")(arg1) do
		:erlang.apply(:"kafe_consumer", :"stop", [arg1])
	end
	def unquote(:"describe")(arg1) do
		:erlang.apply(:"kafe_consumer", :"describe", [arg1])
	end
	def unquote(:"commit")(arg1) do
		:erlang.apply(:"kafe_consumer", :"commit", [arg1])
	end
	def unquote(:"member_id")(arg1, arg2) do
		:erlang.apply(:"kafe_consumer", :"member_id", [arg1, arg2])
	end
	def unquote(:"member_id")(arg1) do
		:erlang.apply(:"kafe_consumer", :"member_id", [arg1])
	end
	def unquote(:"generation_id")(arg1, arg2) do
		:erlang.apply(:"kafe_consumer", :"generation_id", [arg1, arg2])
	end
	def unquote(:"generation_id")(arg1) do
		:erlang.apply(:"kafe_consumer", :"generation_id", [arg1])
	end
	def unquote(:"topics")(arg1, arg2) do
		:erlang.apply(:"kafe_consumer", :"topics", [arg1, arg2])
	end
	def unquote(:"topics")(arg1) do
		:erlang.apply(:"kafe_consumer", :"topics", [arg1])
	end
	def unquote(:"start_link")(arg1, arg2) do
		:erlang.apply(:"kafe_consumer", :"start_link", [arg1, arg2])
	end
	def unquote(:"init")(arg1) do
		:erlang.apply(:"kafe_consumer", :"init", [arg1])
	end
	def unquote(:"encode_group_commit_identifier")(arg1, arg2, arg3, arg4) do
		:erlang.apply(:"kafe_consumer", :"encode_group_commit_identifier", [arg1, arg2, arg3, arg4])
	end
	def unquote(:"decode_group_commit_identifier")(arg1) do
		:erlang.apply(:"kafe_consumer", :"decode_group_commit_identifier", [arg1])
	end
end
