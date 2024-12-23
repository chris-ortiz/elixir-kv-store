defmodule KV.Registry do
  use GenServer

  @doc """
  Starts the registry
  """
  def start_link(opts) do
    GenServer.start_link(KV.Registry, :ok, opts)
  end

  @doc """
  Lookup the bucket
  """
  def lookup(server, name) do
    GenServer.call(server, {:lookup, name})
  end

  def create(server, name) do
    GenServer.cast(server, {:create, name})
  end

  @impl true
  def init(:ok) do
    {:ok, {%{}, %{}}}
  end

  @impl true
  def handle_call({:lookup, name}, _from, {names, refs}) do
    {:reply, Map.fetch(names, name), {names, refs}}
  end

  @impl true
  def handle_cast({:create, name}, {names, refs}) do
    if Map.has_key?(names, name) do
      {:noreply, {names, refs}}
    else
      {:ok, bucket} = DynamicSupervisor.start_child(KV.BucketSupervisor, KV.Bucket)
      ref = Process.monitor(bucket)
      {:noreply, {Map.put(names, name, bucket), Map.put(refs, ref, name)}}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, {names, refs}) do
    {name, refs} = Map.pop(refs, ref)
    names = Map.delete(names, name)
    {:noreply, {names, refs}}
  end

  @impl true
  def handle_info(msg, state) do
    require Logger
    Logger.debug("Unexpected message in KV.Registry: #{inspect(msg)}")
    {:noreply, state}
  end
end
