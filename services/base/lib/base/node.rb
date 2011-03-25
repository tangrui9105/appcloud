#puts "LOOKING FOR EVMA_HTTPSERVER in #{$LOAD_PATH}"
#require 'evma_httpserver'
require 'nats/client'
require 'vcap/component'

$:.unshift(File.dirname(__FILE__))
require 'base'

class VCAP::Services::Base::Node < VCAP::Services::Base::Base

  def initialize(options)
    super(options)
    @node_id = options[:node_id]
  end

  def flavor
    return "Node"
  end

  def on_connect_node
    @logger.debug("#{service_description}: Connected to node mbus")
    @node_nats.subscribe("#{service_name}.provision.#{@node_id}") { |msg, reply|
      on_provision(msg, reply)
    }
    @node_nats.subscribe("#{service_name}.unprovision.#{@node_id}") { |msg, reply|
      on_unprovision(msg, reply)
    }
    @node_nats.subscribe("#{service_name}.discover") { |_, reply|
      on_discover(reply)
    }
    send_node_announcement
    EM.add_periodic_timer(30) {
      send_node_announcement
    }
  end

  def on_provision(msg, reply)
    @logger.debug("#{service_description}: Provision request: #{msg} from #{reply}")
    provision_message = Yajl::Parser.parse(msg)
    plan = provision_message["plan"]
    response = provision(plan)
    response["node_id"] = @node_id
    @node_nats.publish(reply, Yajl::Encoder.encode(response))
  rescue => e
    @logger.warn(e)
  end
  
  def on_unprovision(msg, reply)
    @logger.debug("#{service_description}: Unprovision request: #{msg}.")
    unprovision_message = Yajl::Parser.parse(msg)
    name = unprovision_message["name"]
    unprovision(name)
  rescue => e
    @logger.warn(e)
  end

  def on_discover(reply)
    send_node_announcement(reply)
  end

  def send_node_announcement(reply = nil)
    @logger.debug("#{service_description}: Sending announcement for #{reply || "everyone"}")
    a = announcement
    a[:id] = @node_id
    @node_nats.publish(reply || "#{service_name}.announce", Yajl::Encoder.encode(a))
  rescue
    @logger.warn(e)
  end

  # subclasses must implement the following methods

  # provision(plan) --> {name, host, port, user, password}
  abstract :provision

  # unprovision(name) --> void
  abstract :unprovision

  # announcement() --> { any service-specific announcement details }
  abstract :announcement

end
