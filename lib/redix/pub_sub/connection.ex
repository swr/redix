defmodule Redix.PubSub.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.Utils

  require Logger

  defstruct [
    # The options passed when initializing this GenServer
    opts: nil,

    # The TCP socket connected to the Redis server
    socket: nil,

    # The parsing continuation returned by Redix.Protocol if a response is incomplete
    continuation: nil,

    # The current backoff interval
    backoff_current: nil,

    # A dictionary of `channel => recipient_pids` where `channel` is either
    # `{:channel, "foo"}` or `{:pattern, "foo*"}` and `recipient_pids` is an
    # HashDict of pids of recipients to their monitor ref for that
    # channel/pattern.
    subscriptions: HashDict.new,
  ]

  @backoff_exponent 1.5

  ## Callbacks

  def init(opts) do
    state = %__MODULE__{opts: opts}

    if opts[:sync_connect] do
      sync_connect(state)
    else
      {:connect, :init, state}
    end
  end

  def connect(info, state) do
    case Utils.connect(state) do
      {:ok, state} = result ->
        if info == :backoff do
          Enum.each(state.subscriptions, fn({{kind, target}, subscribers}) ->
            msg_kind =
              case kind do
                :channel -> :subscribed
                :pattern -> :psubscribed
              end
            subscribers
            |> HashDict.keys()
            |> Enum.each(fn(pid) -> send(pid, {:redix_pubsub, self(), msg_kind, %{to: target}}) end)

            {channels, patterns} = Enum.partition(state.subscriptions, &match?({{:channel, _}, _}, &1))

            channels = Enum.map(channels, fn({{:channel, channel}, _}) -> channel end)
            patterns = Enum.map(patterns, fn({{:pattern, pattern}, _}) -> pattern end)

            redis_command = []
            redis_command =
              if channels != [] do
                redis_command ++ [["SUBSCRIBE" | channels]]
              else
                redis_command
              end

            redis_command =
              if patterns != [] do
                redis_command ++ [["PSUBSCRIBE" | patterns]]
              else
                redis_command
              end

            case :gen_tcp.send(state.socket, Enum.map(redis_command, &Protocol.pack/1)) do
              :ok ->
                result
              {:error, _reason} = error ->
                {:disconnect, error, state}
            end
          end)
        end
        result
      {:error, reason} ->
        Logger.error [
          "Failed to connect to Redis (", Utils.format_host(state), "): ",
          Utils.format_error(reason),
        ]

        next_backoff = calc_next_backoff(state.backoff_current || state.opts[:backoff_initial], state.opts[:backoff_max])
        {:backoff, next_backoff, %{state | backoff_current: next_backoff}}
      other ->
        other
    end
  end

  def disconnect(:stop, state) do
    # TODO: do stuff

    {:stop, :normal, state}
  end

  def disconnect({:error, reason}, state) do
    Logger.error [
      "Disconnected from Redis (", Utils.format_host(state), "): ", Utils.format_error(reason),
    ]

    :ok = :gen_tcp.close(state.socket)

    for {_target, subscribers} <- state.subscriptions, {subscriber, _monitor} <- subscribers do
      send(subscriber, {:redix_pubsub, self(), :disconnected, %{reason: reason}})
    end

    state = %{state | socket: nil, continuation: nil, backoff_current: state.opts[:backoff_initial]}
    {:backoff, state.opts[:backoff_initial], state}
  end

  def handle_cast(operation, state)

  def handle_cast({:subscribe, channels, subscriber}, state) do
    register_subscription(state, :subscribe, channels, subscriber)
  end

  def handle_cast({:psubscribe, patterns, subscriber}, state) do
    register_subscription(state, :psubscribe, patterns, subscriber)
  end

  def handle_cast({:unsubscribe, channels, subscriber}, state) do
    register_unsubscription(state, :unsubscribe, channels, subscriber)
  end

  def handle_cast({:punsubscribe, patterns, subscriber}, state) do
    register_unsubscription(state, :punsubscribe, patterns, subscriber)
  end

  def handle_cast(:stop, state) do
    {:disconnect, :stop, state}
  end

  def handle_info(msg, state)

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: :once)
    state = new_data(state, data)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, :tcp_closed}, state}
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    {targets_to_unsubscribe_from, state} = Enum.flat_map_reduce(state.subscriptions, state, fn({key, subscribers}, acc) ->
      subscribers =
        case HashDict.pop(subscribers, pid) do
          {^ref, new_subscribers} -> new_subscribers
          {_, subscribers} -> subscribers
        end
      state = %{acc | subscriptions: HashDict.put(acc.subscriptions, key, subscribers)}
      if HashDict.size(subscribers) == 0 do
        {[key], %{acc | subscriptions: HashDict.delete(acc.subscriptions, key)}}
      else
        {[], state}
      end
    end)

    if targets_to_unsubscribe_from == [] do
      {:noreply, state}
    else
      {channels, patterns} = Enum.partition(targets_to_unsubscribe_from, &match?({:channel, _}, &1))
      commands = [
        Protocol.pack(["UNSUBSCRIBE" | Enum.map(channels, fn({:channel, channel}) -> channel end)]),
        Protocol.pack(["PUNSUBSCRIBE" | Enum.map(patterns, fn({:pattern, pattern}) -> pattern end)]),
      ]
      case :gen_tcp.send(state.socket, commands) do
        :ok -> {:noreply, state}
        {:error, _reason} = error -> {:disconnect, error, state}
      end
    end
  end

  ## Helper functions

  defp sync_connect(state) do
    case Utils.connect(state) do
      {:ok, _state} = result ->
        result
      {:error, reason} ->
        {:stop, reason}
      {:stop, reason, _state} ->
        {:stop, reason}
    end
  end

  defp new_data(state, <<>>) do
    state
  end

  defp new_data(state, data) do
    case (state.continuation || &Protocol.parse/1).(data) do
      {:ok, resp, rest} ->
        state = handle_pubsub_msg(state, resp)
        new_data(%{state | continuation: nil}, rest)
      {:continuation, continuation} ->
        %{state | continuation: continuation}
    end
  end

  defp register_subscription(state, kind, targets, subscriber) do
    msg_kind =
      case kind do
        :subscribe -> :subscribed
        :psubscribe -> :psubscribed
      end

    {targets_to_subscribe_to, state} = Enum.flat_map_reduce(targets, state, fn(target, acc) ->
      key = key_for_target(kind, target)
      {targets_to_subscribe_to, for_target} =
        if for_target = HashDict.get(acc.subscriptions, key) do
          {[], for_target}
        else
          {[target], HashDict.new()}
        end
      for_target = put_new_lazy(for_target, subscriber, fn -> Process.monitor(subscriber) end)
      state = %{acc | subscriptions: HashDict.put(acc.subscriptions, key, for_target)}
      send(subscriber, {:redix_pubsub, self(), msg_kind, %{to: target}})
      {targets_to_subscribe_to, state}
    end)

    if targets_to_subscribe_to == [] do
      {:noreply, state}
    else
      redis_command =
        case kind do
          :subscribe -> "SUBSCRIBE"
          :psubscribe -> "PSUBSCRIBE"
        end

      command = Protocol.pack([redis_command | targets_to_subscribe_to])
      case :gen_tcp.send(state.socket, command) do
        :ok -> {:noreply, state}
        {:error, _reason} = error -> {:disconnect, error, state}
      end
    end
  end

  defp register_unsubscription(state, kind, targets, subscriber) do
    msg_kind =
      case kind do
        :unsubscribe -> :unsubscribed
        :punsubscribe -> :punsubscribed
      end

    {targets_to_unsubscribe_from, state} = Enum.flat_map_reduce(targets, state, fn(target, acc) ->
      send(subscriber, {:redix_pubsub, self(), msg_kind, %{from: target}})
      if for_target = HashDict.get(acc.subscriptions, key_for_target(kind, target)) do
        case HashDict.pop(for_target, subscriber) do
          {ref, new_for_target} when is_reference(ref) ->
            Process.demonitor(ref)
            # TODO flush messages?
            targets_to_unsubscribe_from =
              if HashDict.size(new_for_target) == 0 do
                [target]
              else
                []
              end
            state = %{acc | subscriptions: HashDict.put(state.subscriptions, key_for_target(kind, target), new_for_target)}
            {targets_to_unsubscribe_from, state}
          {nil, _} ->
            {[], state}
        end
      else
        {[], state}
      end
    end)

    if targets_to_unsubscribe_from == [] do
      {:noreply, state}
    else
      redis_command =
        case kind do
          :unsubscribe -> "UNSUBSCRIBE"
          :punsubscribe -> "PUNSUBSCRIBE"
        end

      command = Protocol.pack([redis_command | targets_to_unsubscribe_from])
      case :gen_tcp.send(state.socket, command) do
        :ok -> {:noreply, state}
        {:error, _reason} = error -> {:disconnect, error, state}
      end
    end
  end

  defp handle_pubsub_msg(state, [operation, _target, _count])
      when operation in ~w(subscribe psubscribe unsubscribe punsubscribe) do
    state
  end

  defp handle_pubsub_msg(state, ["message", channel, payload]) do
    subscribers = HashDict.fetch!(state.subscriptions, {:channel, channel})
    Enum.each(subscribers, fn({subscriber, _monitor}) ->
      meta = %{channel: channel, payload: payload}
      send(subscriber, {:redix_pubsub, self(), :message, meta})
    end)
    state
  end

  defp handle_pubsub_msg(state, ["pmessage", pattern, channel, payload]) do
    subscribers = HashDict.fetch!(state.subscriptions, {:pattern, pattern})
    Enum.each(subscribers, fn({subscriber, _monitor}) ->
      meta = %{channel: channel, pattern: pattern, payload: payload}
      send(subscriber, {:redix_pubsub, self(), :pmessage, meta})
    end)
    state
  end

  defp calc_next_backoff(backoff_current, backoff_max) do
    next_exponential_backoff = round(backoff_current * @backoff_exponent)

    if backoff_max == :infinity do
      next_exponential_backoff
    else
      min(next_exponential_backoff, backoff_max)
    end
  end

  defp put_new_lazy(hash_dict, key, fun) when is_function(fun, 0) do
    if HashDict.has_key?(hash_dict, key) do
      hash_dict
    else
      HashDict.put(hash_dict, key, fun.())
    end
  end

  defp key_for_target(kind, target) when kind in [:subscribe, :unsubscribe],
    do: {:channel, target}
  defp key_for_target(kind, target) when kind in [:psubscribe, :punsubscribe],
    do: {:pattern, target}
end
