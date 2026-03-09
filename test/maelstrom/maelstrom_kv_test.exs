defmodule MaelstromKvTest do
  @moduledoc """
  Unit tests for MaelstromKv implementation.
  """

  use ExUnit.Case, async: true

  setup do
    temp_dir = System.tmp_dir!()
    unique_id = System.unique_integer([:positive])
    node_id = "n0_#{unique_id}"
    dets_file = Path.join(temp_dir, "test_#{unique_id}_log.dets")

    {:ok, pid} = MaelstromKv.start_link([])

    VsrServer.set_cluster(pid, node_id, [], 1)

    table_name = String.to_atom("test_log_#{unique_id}")
    {:ok, log} = :dets.open_file(table_name, file: String.to_charlist(dets_file))
    VsrServer.set_log(pid, log)

    on_exit(fn ->
      File.rm(dets_file)
    end)

    {:ok, server: pid}
  end

  describe "initialization" do
    test "starts with empty data store", %{server: server} do
      state = VsrServer.dump(server)
      kv_state = state.inner

      assert kv_state.data == %{}
    end
  end

  describe "read operations" do
    test "reads return error for non-existent keys", %{server: server} do
      result = MaelstromKv.read(server, "nonexistent")
      assert result == {:error, :not_found}
    end

    test "reads return values for existing keys", %{server: server} do
      write_result = MaelstromKv.write(server, "key1", "value1")
      assert write_result == :ok

      :timer.sleep(50)

      result = MaelstromKv.read(server, "key1")
      assert result == {:ok, "value1"}
    end
  end

  describe "write operations" do
    test "write operations complete after consensus", %{server: _server} do
      operation = ["write", "key1", "value1"]

      kv_state = %MaelstromKv{}
      {new_kv_state, result} = MaelstromKv.handle_commit(operation, kv_state)

      assert result == :ok
      assert new_kv_state.data["key1"] == "value1"
    end
  end

  describe "compare-and-swap (CAS) operations" do
    test "CAS succeeds when expected value matches", %{server: _server} do
      kv_state = %MaelstromKv{data: %{"key1" => "old_value"}}
      operation = ["cas", "key1", "old_value", "new_value"]

      {new_kv_state, result} = MaelstromKv.handle_commit(operation, kv_state)

      assert result == :ok
      assert new_kv_state.data["key1"] == "new_value"
    end

    test "CAS fails when expected value doesn't match", %{server: _server} do
      kv_state = %MaelstromKv{data: %{"key1" => "actual_value"}}
      operation = ["cas", "key1", "expected_value", "new_value"]

      {new_kv_state, result} = MaelstromKv.handle_commit(operation, kv_state)

      assert result == {:error, :precondition_failed}
      # Unchanged
      assert new_kv_state.data["key1"] == "actual_value"
    end

    test "CAS works with nil values", %{server: _server} do
      kv_state = %MaelstromKv{}
      operation = ["cas", "new_key", nil, "value1"]

      {new_kv_state, result} = MaelstromKv.handle_commit(operation, kv_state)

      assert result == :ok
      assert new_kv_state.data["new_key"] == "value1"
    end
  end
end
