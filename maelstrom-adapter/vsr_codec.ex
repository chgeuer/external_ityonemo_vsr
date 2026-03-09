defmodule Maelstrom.VsrCodec do
  @moduledoc """
  Converts VSR protocol message structs to/from plain maps for MaelstromNexus JSON transport.

  MaelstromNexus uses plain maps (encoded via Jason) for all messages.
  VSR uses typed structs internally. This module bridges the two representations.
  """

  alias Vsr.LogEntry

  @vsr_type_to_module %{
    "prepare" => Vsr.Message.Prepare,
    "prepare_ok" => Vsr.Message.PrepareOk,
    "commit" => Vsr.Message.Commit,
    "start_view_change" => Vsr.Message.StartViewChange,
    "start_view_change_ack" => Vsr.Message.StartViewChangeAck,
    "do_view_change" => Vsr.Message.DoViewChange,
    "start_view" => Vsr.Message.StartView,
    "view_change_ok" => Vsr.Message.ViewChangeOk,
    "get_state" => Vsr.Message.GetState,
    "new_state" => Vsr.Message.NewState,
    "client_request" => Vsr.Message.ClientRequest,
    "heartbeat" => Vsr.Message.Heartbeat
  }

  @vsr_module_to_type Map.new(@vsr_type_to_module, fn {k, v} -> {v, k} end)

  @vsr_types Map.keys(@vsr_type_to_module)

  @log_bearing_modules [Vsr.Message.DoViewChange, Vsr.Message.StartView, Vsr.Message.NewState]

  @doc """
  Returns the list of VSR message type strings.
  """
  def vsr_types, do: @vsr_types

  @doc """
  Decodes a plain map (from JSON) into a VSR message struct.
  """
  @spec decode(String.t(), map()) :: struct()
  def decode(type, body) do
    module = Map.fetch!(@vsr_type_to_module, type)
    base_struct = module.__struct__()

    for field <- Map.keys(base_struct), field != :__struct__, reduce: base_struct do
      struct -> Map.replace!(struct, field, Map.fetch!(body, to_string(field)))
    end
    |> maybe_reify_log(module)
  end

  @doc """
  Encodes a VSR message struct into a plain map suitable for JSON encoding.
  """
  @spec encode(struct()) :: map()
  def encode(struct) do
    type_string = Map.fetch!(@vsr_module_to_type, struct.__struct__)

    struct
    |> Map.from_struct()
    |> Map.new(fn {k, v} -> {to_string(k), v} end)
    |> Map.put("type", type_string)
    |> maybe_serialize_log(struct.__struct__)
  end

  # Incoming: convert log entries from maps to LogEntry structs
  defp maybe_reify_log(struct, module) when module in @log_bearing_modules do
    Map.update!(struct, :log, fn log ->
      Enum.map(log, fn entry ->
        %LogEntry{
          view: Map.fetch!(entry, "view"),
          op_number: Map.fetch!(entry, "op_number"),
          operation: Map.fetch!(entry, "operation"),
          sender_id: Map.fetch!(entry, "sender_id")
        }
      end)
    end)
  end

  defp maybe_reify_log(struct, _module), do: struct

  # Outgoing: convert LogEntry structs to plain maps
  defp maybe_serialize_log(map, module) when module in @log_bearing_modules do
    Map.update!(map, "log", fn log ->
      Enum.map(log, fn entry ->
        entry
        |> Map.from_struct()
        |> Map.new(fn {k, v} -> {to_string(k), v} end)
      end)
    end)
  end

  defp maybe_serialize_log(map, _module), do: map
end
