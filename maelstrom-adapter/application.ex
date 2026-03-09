defmodule Maelstrom.Application do
  @moduledoc """
  Main application supervisor for Maelstrom integration.

  Starts MaelstromKv (VsrServer) and the MaelstromNexus handler
  which manages stdin/stdout JSON protocol communication.
  """

  use Application

  require Logger

  def start(_type, _args) do
    {:ok, handler_config} = :logger.get_handler_config(:default)
    stderr_config = put_in(handler_config, [:config, :type], :standard_error)

    :ok = :logger.remove_handler(:default)
    :ok = :logger.add_handler(:default, :logger_std_h, stderr_config)

    Logger.info("Starting...")

    children = [
      {MaelstromKv, name: MaelstromKv},
      {MaelstromNexus,
       {Maelstrom.Handler, name: Maelstrom.Handler, handler_args: [kv: MaelstromKv]}}
    ]

    opts = [strategy: :one_for_one, name: Maelstrom.Application]
    Supervisor.start_link(children, opts)
  end
end
