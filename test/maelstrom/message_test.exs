defmodule Maelstrom.VsrCodecTest do
  @moduledoc """
  Tests for VSR struct ↔ plain map conversion via Maelstrom.VsrCodec.
  """

  use ExUnit.Case, async: true

  @moduletag :maelstrom

  alias Maelstrom.VsrCodec
  alias Vsr.LogEntry

  describe "decode/2" do
    test "decodes prepare message" do
      body = %{
        "type" => "prepare",
        "view" => 1,
        "op_number" => 5,
        "operation" => ["write", "key", "value"],
        "commit_number" => 4,
        "from" => "client_ref",
        "leader_id" => "n0"
      }

      result = VsrCodec.decode("prepare", body)

      assert %Vsr.Message.Prepare{
               view: 1,
               op_number: 5,
               operation: ["write", "key", "value"],
               commit_number: 4,
               from: "client_ref",
               leader_id: "n0"
             } = result
    end

    test "decodes prepare_ok message" do
      body = %{
        "type" => "prepare_ok",
        "view" => 1,
        "op_number" => 5,
        "replica" => "n1"
      }

      result = VsrCodec.decode("prepare_ok", body)

      assert %Vsr.Message.PrepareOk{
               view: 1,
               op_number: 5,
               replica: "n1"
             } = result
    end

    test "decodes commit message" do
      body = %{"type" => "commit", "view" => 1, "commit_number" => 5}
      result = VsrCodec.decode("commit", body)
      assert %Vsr.Message.Commit{view: 1, commit_number: 5} = result
    end

    test "decodes heartbeat message" do
      body = %{"type" => "heartbeat", "view" => 1, "leader_id" => "n0"}
      result = VsrCodec.decode("heartbeat", body)
      assert %Vsr.Message.Heartbeat{view: 1, leader_id: "n0"} = result
    end

    test "decodes client_request message" do
      body = %{
        "type" => "client_request",
        "operation" => ["read", "key"],
        "from" => "client_ref",
        "read_only" => true,
        "client_key" => "unique_key",
        "client_id" => "test_client",
        "request_id" => 42
      }

      result = VsrCodec.decode("client_request", body)

      assert %Vsr.Message.ClientRequest{
               operation: ["read", "key"],
               from: "client_ref",
               read_only: true,
               client_key: "unique_key",
               client_id: "test_client",
               request_id: 42
             } = result
    end

    test "decodes new_state message with log entries" do
      body = %{
        "type" => "new_state",
        "view" => 2,
        "log" => [
          %{
            "view" => 1,
            "op_number" => 1,
            "operation" => ["write", "k", "v"],
            "sender_id" => "c0"
          },
          %{"view" => 1, "op_number" => 2, "operation" => ["read", "k"], "sender_id" => "c1"}
        ],
        "op_number" => 2,
        "commit_number" => 2,
        "state_machine_state" => %{"k" => "v"},
        "leader_id" => "n0"
      }

      result = VsrCodec.decode("new_state", body)

      assert %Vsr.Message.NewState{view: 2, op_number: 2, commit_number: 2} = result
      assert [%LogEntry{op_number: 1}, %LogEntry{op_number: 2}] = result.log
    end

    test "raises on unknown message type" do
      assert_raise KeyError, fn ->
        VsrCodec.decode("unknown_type", %{"type" => "unknown_type"})
      end
    end

    test "raises on missing required field" do
      assert_raise KeyError, fn ->
        VsrCodec.decode("prepare", %{"type" => "prepare", "view" => 1})
      end
    end
  end

  describe "encode/1" do
    test "encodes prepare message" do
      prepare = %Vsr.Message.Prepare{
        view: 1,
        op_number: 5,
        operation: ["write", "key", "value"],
        commit_number: 4,
        from: "client_ref",
        leader_id: "n0"
      }

      result = VsrCodec.encode(prepare)

      assert result["type"] == "prepare"
      assert result["view"] == 1
      assert result["op_number"] == 5
      assert result["operation"] == ["write", "key", "value"]
      assert result["commit_number"] == 4
      assert result["from"] == "client_ref"
      assert result["leader_id"] == "n0"
    end

    test "encodes heartbeat message" do
      heartbeat = %Vsr.Message.Heartbeat{view: 1, leader_id: "n0"}
      result = VsrCodec.encode(heartbeat)

      assert result["type"] == "heartbeat"
      assert result["view"] == 1
      assert result["leader_id"] == "n0"
    end

    test "encodes new_state with log entries" do
      new_state = %Vsr.Message.NewState{
        view: 2,
        log: [
          %LogEntry{view: 1, op_number: 1, operation: ["write", "k", "v"], sender_id: "c0"}
        ],
        op_number: 1,
        commit_number: 1,
        state_machine_state: %{"k" => "v"},
        leader_id: "n0"
      }

      result = VsrCodec.encode(new_state)

      assert result["type"] == "new_state"

      assert [%{"view" => 1, "op_number" => 1, "operation" => ["write", "k", "v"]}] =
               result["log"]
    end

    test "all string keys in encoded output" do
      commit = %Vsr.Message.Commit{view: 1, commit_number: 5}
      result = VsrCodec.encode(commit)

      assert Enum.all?(Map.keys(result), &is_binary/1)
    end
  end

  describe "round-trip" do
    test "prepare round-trip" do
      original = %Vsr.Message.Prepare{
        view: 1,
        op_number: 5,
        operation: ["write", "key", "value"],
        commit_number: 4,
        from: %{"node" => "n0", "from" => 12345},
        leader_id: "n0"
      }

      encoded = VsrCodec.encode(original)
      decoded = VsrCodec.decode("prepare", encoded)

      assert decoded.view == original.view
      assert decoded.op_number == original.op_number
      assert decoded.operation == original.operation
      assert decoded.commit_number == original.commit_number
      assert decoded.from == original.from
      assert decoded.leader_id == original.leader_id
    end

    test "new_state round-trip preserves log entries" do
      original = %Vsr.Message.NewState{
        view: 2,
        log: [
          %LogEntry{view: 1, op_number: 1, operation: ["write", "k", "v"], sender_id: "c0"},
          %LogEntry{view: 1, op_number: 2, operation: ["read", "k"], sender_id: "c1"}
        ],
        op_number: 2,
        commit_number: 2,
        state_machine_state: %{"k" => "v"},
        leader_id: "n0"
      }

      encoded = VsrCodec.encode(original)
      decoded = VsrCodec.decode("new_state", encoded)

      assert length(decoded.log) == 2
      assert Enum.at(decoded.log, 0).op_number == 1
      assert Enum.at(decoded.log, 1).op_number == 2
    end

    test "encoded output is Jason-serializable" do
      prepare = %Vsr.Message.Prepare{
        view: 1,
        op_number: 5,
        operation: ["write", "key", "value"],
        commit_number: 4,
        from: %{"node" => "n0", "from" => 12345},
        leader_id: "n0"
      }

      encoded = VsrCodec.encode(prepare)
      json = Jason.encode!(encoded)
      decoded_json = Jason.decode!(json)

      assert decoded_json == encoded
    end
  end
end
