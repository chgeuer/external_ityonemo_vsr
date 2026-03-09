defmodule Maelstrom.Handler do
  @moduledoc """
  MaelstromNexus handler that bridges Maelstrom's JSON protocol with VSR consensus.

  Routes incoming Maelstrom messages to the appropriate handler:
  - Echo messages are replied to directly
  - VSR protocol messages are decoded and forwarded to VsrServer
  - Client operations (read/write/cas) are forwarded to MaelstromKv for consensus
  - Forwarded replies are routed back to MaelstromKv for local delivery
  """

  use MaelstromNexus
  require Logger

  alias Maelstrom.VsrCodec

  @vsr_types VsrCodec.vsr_types()

  @impl true
  def init(args) do
    kv = Keyword.get(args, :kv, MaelstromKv)
    dets_root = Keyword.get(args, :dets_root, ".")
    {:ok, %{kv: kv, dets_root: dets_root}}
  end

  @impl true
  def handle_init(node_id, node_ids, state) do
    Logger.info("Initializing Maelstrom node #{node_id} with cluster: #{inspect(node_ids)}")

    other_replicas = Enum.reject(node_ids, &(&1 == node_id))
    cluster_size = length(node_ids)
    VsrServer.set_cluster(state.kv, node_id, other_replicas, cluster_size)

    table_name = String.to_atom("maelstrom_log_#{node_id}")
    dets_file = Path.join(state.dets_root, "#{node_id}.dets")
    {:ok, log} = :dets.open_file(table_name, file: String.to_charlist(dets_file))
    VsrServer.set_log(state.kv, log)

    {:ok, state}
  end

  @impl true
  def handle_message("echo", body, _msg, state) do
    {:reply, %{"type" => "echo_ok", "echo" => body["echo"]}, state}
  end

  # VSR protocol messages - decode to struct and forward to VsrServer
  def handle_message(type, body, _msg, state) when type in @vsr_types do
    vsr_struct = VsrCodec.decode(type, body)
    VsrServer.vsr_send(state.kv, vsr_struct)
    {:noreply, state}
  end

  # Client operations - forward to MaelstromKv for VSR consensus
  def handle_message("read", body, msg, state) do
    GenServer.cast(state.kv, {:maelstrom_request, "read", ["read", body["key"]], msg})
    {:noreply, state}
  end

  def handle_message("write", body, msg, state) do
    GenServer.cast(
      state.kv,
      {:maelstrom_request, "write", ["write", body["key"], body["value"]], msg}
    )

    {:noreply, state}
  end

  def handle_message("cas", body, msg, state) do
    GenServer.cast(
      state.kv,
      {:maelstrom_request, "cas", ["cas", body["key"], body["from"], body["to"]], msg}
    )

    {:noreply, state}
  end

  # Forwarded reply from another node
  def handle_message("forwarded_reply", body, _msg, state) do
    GenServer.cast(state.kv, {:forwarded_reply, body["from_hash"], body["reply"]})
    {:noreply, state}
  end
end
