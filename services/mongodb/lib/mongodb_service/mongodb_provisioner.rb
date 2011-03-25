require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"

require "datamapper"
require "eventmachine"
require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'

require "mongodb_service/barrier"

module VCAP; module Services; module MongoDB; end; end; end

class VCAP::Services::MongoDB::Provisioner

  def initialize(opts)
    @logger    = opts[:logger]
    @local_ip  = VCAP.local_ip(opts[:local_ip])
    @nodes     = {}
    @svc_mbus  = opts[:service_mbus]
    @mdb_mbus  = opts[:mongodb_mbus]
    @mdb_ver   = opts[:mongodb_version]
    @prov_svcs = {}
    @opts      = opts
  end

  def start
    @logger.info("Starting MongoDB-Service..")

    @service_nats = NATS.connect(:uri => @svc_mbus) {on_service_connect}
    @mongodb_nats = NATS.connect(:uri => @mdb_mbus) {on_node_connect}

    VCAP::Component.register(
      :nats   => @svc_nats,
      :type   => 'MongoDB-Service',
      :host   => @local_ip,
      :config => @opts)

    EM.add_periodic_timer(60) {process_nodes}

    self
  end

  def shutdown
    @logger.info("Shutting down..")
    @service_nats.close
    @mongodb_nats.close
  end

  def process_nodes
    @nodes.delete_if {|_, timestamp| Time.now.to_i - timestamp > 300}
  end

  def on_service_connect
    @logger.debug("Connected to service mbus..")
  end

  def on_node_connect
    @logger.debug("Connected to node mbus..")
    @service_nats.subscribe("MongoDBaaS.announce") {|msg| on_node_announce(msg)}
    @service_nats.publish("MongoDBaaS.discover")
  end

  def on_node_announce(msg)
    @logger.debug("[MongoDB] Received MongoDB Node announcement: #{msg}")
    announce_message = Yajl::Parser.parse(msg)
    @nodes[announce_message["id"]] = Time.now.to_i
  end

  # Updates our internal state to match that supplied by handles
  # +handles+  An array of config handles
  def update_handles(handles)
    current   = Set.new(@prov_svcs.keys)
    supplied  = Set.new(handles.map {|h| h['service_id']})
    intersect = current & supplied

    handles_keyed = {}
    handles.each {|v| handles_keyed[v['service_id']] = v}

    to_add = supplied - intersect
    to_add.each do |h_id|
      @logger.debug("Adding handle #{h_id}")
      h = handles_keyed[h_id]
      @prov_svcs[h_id] = {
        :data        => h['configuration'],
        :credentials => h['credentials'],
        :service_d   => h_id
      }
    end

    # TODO: Handle removing existing handles if we decide to periodically sync with the CC
  end

  def unprovision_service(instance_id, &blk)
    begin
      success = true
      @logger.debug("Unprovisioning mongodb instance #{instance_id}")
      request = {:name => instance_id}
      @mongodb_nats.publish("MongoDBaaS.unprovision", Yajl::Encoder.encode(request))
      @prov_svcs.delete(instance_id)
    rescue => e
      @logger.warn(e)
      success = nil
    end
    blk.call(success)
  end

  def provision_service(version, plan, &blk)
    @logger.debug("Attempting to provision MongoDB instance (version=#{version}, plan=#{plan})")
    subscription = nil
    barrier = VCAP::Services::MongoDB::Barrier.new(:timeout => 2, :callbacks => @nodes.length) do |responses|
      @logger.debug("[MongoDB] Found the following MongoDB Nodes: #{responses.pretty_inspect}")
      @mongodb_nats.unsubscribe(subscription)
      provision_node(version, plan, responses, blk) unless responses.empty?
    end
    subscription = @mongodb_nats.request("MongoDBaaS.discover", &barrier.callback)
  rescue => e
    @logger.warn(e)
  end

  def provision_node(version, plan, mongodb_nodes, blk)
    @logger.debug("Provisioning mongo db node (version=#{version}, plan=#{plan}, nnodes=#{mongodb_nodes.length}")
    node_with_most_memory = nil
    most_memory = 0

    mongodb_nodes.each do |mongodb_node_msg|
      mongodb_node_msg = mongodb_node_msg.first
      node = Yajl::Parser.parse(mongodb_node_msg)
      if node["available_memory"] > most_memory
        node_with_most_memory = node["id"]
        most_memory = node["available_memory"]
      end
    end

    if node_with_most_memory
      @logger.debug("Provisioning on #{node_with_most_memory}")
      request = {"plan" => plan}
      subscription = nil

      timer = EM.add_timer(2) {@mongodb_nats.unsubscribe(subscription)}
      subscription = @mongodb_nats.request("MongoDBaaS.provision.#{node_with_most_memory}",
                                        Yajl::Encoder.encode(request)) do |msg|
        EM.cancel_timer(timer)
        @mongodb_nats.unsubscribe(subscription)
        opts = Yajl::Parser.parse(msg)
        svc = {:data => opts, :service_id => opts['name'], :credentials => opts}
        @logger.debug("Provisioned #{svc.pretty_inspect}")
        @prov_svcs[svc[:service_id]] = svc
        blk.call(svc)
      end
    else
      @logger.warn("Could not find a MongoDB node to provision: #{provision_request} #{reply}")
    end

  end

  def bind_instance(instance_id, binding_token, app_id, options, &blk)
    @logger.debug("Attempting to bind to service #{instance_id}")
    svc = @prov_svcs[instance_id]
    handle = nil
    if svc
      @logger.debug("Config: #{svc.inspect}")
      handle = {
        'service_id'    => UUIDTools::UUID.random_create.to_s,
        'configuration' => svc,
        'credentials'   => svc[:data],
      }
      @logger.debug("Binding mongo instance #{instance_id} to handle #{handle['service_id']}")
    end
    blk.call(handle)
  end

  def unbind_instance(binding_token, app_id, &blk)
    blk.call(true)
  end

end
