defmodule Maelstrom.KvServerTest do
  @moduledoc """
  Tests for MaelstromKv key-value operations through VSR consensus.
  """

  use ExUnit.Case, async: true

  setup t do
    temp_dir = System.tmp_dir!()
    unique_id = System.unique_integer([:positive])
    node_id = "n1"
    dets_file = Path.join(temp_dir, "kv_test_#{unique_id}_log.dets")

    pid =
      start_supervised!({MaelstromKv, name: :"#{t.test}-node"})

    VsrServer.set_cluster(pid, node_id, [], 1)

    table_name = String.to_atom("kv_test_log_#{unique_id}")
    {:ok, log} = :dets.open_file(table_name, file: String.to_charlist(dets_file))
    VsrServer.set_log(pid, log)

    on_exit(fn ->
      File.rm(dets_file)
    end)

    {:ok, pid: pid}
  end

  @tag :maelstrom
  test "kv server handles read/write operations", %{pid: pid} do
    assert {:error, :not_found} = MaelstromKv.read(pid, "test_key")
    assert :ok = MaelstromKv.write(pid, "test_key", "test_value")
    assert {:ok, "test_value"} = MaelstromKv.read(pid, "test_key")
  end

  @tag :maelstrom
  test "kv server handles CAS operations", %{pid: pid} do
    assert {:error, :precondition_failed} =
             MaelstromKv.cas(pid, "cas_key", "old_value", "new_value")

    assert :ok = MaelstromKv.cas(pid, "cas_key2", nil, "initial_value")
    assert {:ok, "initial_value"} = MaelstromKv.read(pid, "cas_key2")
  end
end
