defmodule MaelstromKv do
  @moduledoc """
  VSR-backed key-value store for Maelstrom integration.

  Implements VsrServer callbacks for distributed consensus and delegates
  all Maelstrom protocol I/O to MaelstromNexus via `Maelstrom.Handler`.

  In test mode (no nexus), works with standard GenServer.call/reply.
  In Maelstrom mode, uses MaelstromNexus.reply/send_msg/error_reply for I/O.
  """

  use VsrServer
  require Logger

  alias Maelstrom.VsrCodec

  defstruct [
    :from_table,
    nexus: Maelstrom.Handler,
    data: %{}
  ]

  @type t :: %__MODULE__{
          from_table: :ets.tid() | nil,
          nexus: GenServer.server(),
          data: %{term() => term()}
        }

  # Client API

  def start_link(opts \\ []) do
    VsrServer.start_link(__MODULE__, opts)
  end

  # VsrServer callbacks

  def init(opts) do
    from_table_ref = :ets.new(__MODULE__, [:set, :private])
    nexus = Keyword.get(opts, :nexus, Maelstrom.Handler)
    inner_state = %__MODULE__{from_table: from_table_ref, nexus: nexus}
    {:ok, inner_state}
  end

  # VsrServer callback for sending VSR messages over Maelstrom network
  def send_vsr(dest_node_id, vsr_message, inner_state) do
    body = VsrCodec.encode(vsr_message)
    MaelstromNexus.send_msg(inner_state.nexus, dest_node_id, body)
  end

  # Client reply callback - handles both test mode (GenServer.from tuple) and Maelstrom mode
  def send_reply(from, reply, _vsr_state) when is_tuple(from) do
    GenServer.reply(from, reply)
  end

  def send_reply(%{"node" => node_id, "from" => from_hash}, reply, inner_state) do
    if node_id == VsrServer.node_id() do
      local_reply(inner_state, from_hash, reply)
    else
      encoded_reply = Base.encode64(:erlang.term_to_binary(reply))

      MaelstromNexus.send_msg(inner_state.nexus, node_id, %{
        "type" => "forwarded_reply",
        "from_hash" => from_hash,
        "reply" => encoded_reply
      })
    end
  end

  def get_state(state), do: state.data
  def set_state(state, data), do: %{state | data: data}

  ## Log callback implementations for DETS storage

  def log_append(log, entry) do
    :ok = :dets.insert(log, {entry.op_number, entry})
    :ok = :dets.sync(log)
    log
  end

  def log_fetch(log, op_number) do
    case :dets.lookup(log, op_number) do
      [{^op_number, entry}] -> {:ok, entry}
      [] -> {:error, :not_found}
    end
  end

  def log_get_all(log) do
    :dets.select(log, [{{:"$1", :"$2"}, [], [:"$2"]}])
  end

  def log_get_from(log, op_number) do
    :dets.select(log, [
      {{:"$1", :"$2"}, [{:>=, {:map_get, :op_number, :"$2"}, op_number}], [:"$2"]}
    ])
  end

  def log_length(log), do: :dets.info(log, :size)

  def log_replace(log, entries) do
    Logger.debug("Replacing log entries: #{inspect(entries)}")

    Enum.each(entries, fn entry ->
      :ok = :dets.insert(log, {entry.op_number, entry})
    end)

    :ok = :dets.sync(log)
    log
  end

  def log_clear(log) do
    :ok = :dets.delete_all_objects(log)
    :ok = :dets.sync(log)
    log
  end

  ## VSR commit handler

  def handle_commit(["read", key], state), do: read_impl(key, state)
  def handle_commit(["write", key, value], state), do: write_impl(key, value, state)

  def handle_commit(["cas", key, from_value, to_value], state),
    do: cas_impl(key, from_value, to_value, state)

  ## Client API for tests (standard GenServer.call path)

  @doc "Reads a value from the key-value store."
  def read(server, key) do
    GenServer.call(server, {:client_request, ["read", key]})
  end

  @doc "Writes a value to the key-value store."
  def write(server, key, value) do
    GenServer.call(server, {:client_request, ["write", key, value]})
  end

  @doc "Compare-and-swap operation on the key-value store."
  def cas(server, key, from_value, to_value) do
    GenServer.call(server, {:client_request, ["cas", key, from_value, to_value]})
  end

  # GenServer.call handler for test mode
  def handle_call({:client_request, operation}, from, state) do
    {:noreply, state, {:client_request, from, operation}}
  end

  ## Maelstrom request handlers (cast from Handler)

  def handle_cast({:maelstrom_request, type, operation, original_msg}, state) do
    from_hash = :erlang.phash2(make_ref())
    :ets.insert(state.from_table, {from_hash, {type, original_msg}})

    node_id = VsrServer.node_id(self())
    encoded_from = %{"node" => node_id, "from" => from_hash}

    {:noreply, state, {:client_request, encoded_from, operation}}
  end

  def handle_cast({:forwarded_reply, from_hash, encoded_reply}, state) do
    reply = :erlang.binary_to_term(Base.decode64!(encoded_reply))
    local_reply(state, from_hash, reply)
    {:noreply, state}
  end

  ## Private helpers

  defp read_impl(key, state) do
    value = Map.get(state.data, key)
    result = if value, do: {:ok, value}, else: {:error, :not_found}
    {state, result}
  end

  defp write_impl(key, value, state) do
    new_data = Map.put(state.data, key, value)
    {%{state | data: new_data}, :ok}
  end

  defp cas_impl(key, from_value, to_value, state) do
    current_value = Map.get(state.data, key)

    if current_value == from_value do
      new_data = Map.put(state.data, key, to_value)
      {%{state | data: new_data}, :ok}
    else
      {state, {:error, :precondition_failed}}
    end
  end

  defp local_reply(%{from_table: from_table, nexus: nexus}, from_hash, reply) do
    case :ets.lookup(from_table, from_hash) do
      [{^from_hash, {type, original_msg}}] ->
        :ets.delete(from_table, from_hash)

        case build_reply(type, reply) do
          {:ok, body} ->
            MaelstromNexus.reply(nexus, original_msg, body)

          {:error, code, text} ->
            MaelstromNexus.error_reply(nexus, original_msg, code, text)
        end

      [] ->
        Logger.error("reply failed, could not find hash #{from_hash}")
    end
  end

  defp build_reply("read", {:ok, value}), do: {:ok, %{"type" => "read_ok", "value" => value}}
  defp build_reply("read", {:error, :not_found}), do: {:error, 20, "key not found"}
  defp build_reply("read", {:error, :not_primary}), do: {:error, 11, "not primary"}
  defp build_reply("write", :ok), do: {:ok, %{"type" => "write_ok"}}
  defp build_reply("write", {:error, reason}), do: {:error, 13, "write failed: #{reason}"}
  defp build_reply("cas", :ok), do: {:ok, %{"type" => "cas_ok"}}
  defp build_reply("cas", {:error, :precondition_failed}), do: {:error, 22, "precondition failed"}
  defp build_reply("cas", {:error, reason}), do: {:error, 13, "cas failed: #{reason}"}
end
