require "erb"
require "fileutils"
require "logger"
require "pp"
require "set"

require "datamapper"
require "nats/client"
require "uuidtools"

require 'vcap/common'
require 'vcap/component'

module VCAP; module Services; module MongoDB; end; end; end

class VCAP::Services::MongoDB::Node

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,   :key => true
    property :port,       Integer,  :unique => true
    property :password,   String,   :required => true
    property :plan,       Enum[:free], :required => true
    property :pid,        Integer
    property :memory,     Integer


    def running?
      VCAP.process_running? pid
    end

    def kill(sig=9)
      Process.kill(sig, pid) if running?
    end
  end

  def initialize(options)
    @logger = options[:logger]
    @logger.info("Starting MongoDB-Node..")
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @mongod_path = options[:mongod_path]

    @local_ip = VCAP.local_ip(options[:ip_route])
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]
    @node_id = options[:node_id]

    @config_template = ERB.new(File.read(options[:config_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}

    ProvisionedService.all.each do |provisioned_service|
      @free_ports.delete(provisioned_service.port)
      if provisioned_service.running?
        @logger.info("Service #{provisioned_service.name} already running with pid #{provisioned_service.pid}")
        @available_memory -= (provisioned_service.memory || @max_memory)
        next
      end
      begin
        pid = start_instance(provisioned_service)
        provisioned_service.pid = pid
        unless provisioned_service.save
          provisioned_service.kill
          raise "Couldn't save pid (#{pid})"
        end
      rescue => e
        @logger.warn("Error starting service #{provisioned_service.name}: #{e}")
      end
    end

    @nats = NATS.connect(:uri => options[:mbus]) {on_connect}

    VCAP::Component.register(:nats => @nats,
                             :type => 'MongoDB-Service-Node',
                             :host => @local_ip,
                             :config => options)
  end

  def shutdown
    @logger.info("Shutting down..")
    @nats.close
  end

  def on_connect
    @logger.debug("Connected to mbus..")
    @nats.subscribe("MongoDBaaS.provision.#{@node_id}") {|msg, reply| on_provision(msg, reply)}
    @nats.subscribe("MongoDBaaS.unprovision.#{@node_id}") {|msg, reply| on_unprovision(msg, reply)}
    @nats.subscribe("MongoDBaaS.unprovision") {|msg, reply| on_unprovision(msg, reply)}
    @nats.subscribe("MongoDBaaS.discover") {|_, reply| send_node_announcement(reply)}
    send_node_announcement
    EM.add_periodic_timer(30) {send_node_announcement}
  end

  def on_provision(msg, reply)
    @logger.debug("Provision request: #{msg} from #{reply}")
    provision_message = Yajl::Parser.parse(msg)

    port = @free_ports.first
    @free_ports.delete(port)

    provisioned_service          = ProvisionedService.new
    provisioned_service.name     = "mongodb-#{UUIDTools::UUID.random_create.to_s}"
    provisioned_service.port     = port
    provisioned_service.plan     = provision_message["plan"]
    provisioned_service.password = UUIDTools::UUID.random_create.to_s
    provisioned_service.memory   = @max_memory
    provisioned_service.pid      = start_instance(provisioned_service)

    unless provisioned_service.save
      provisioned_service.kill
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = {
      "node_id" => @node_id,
      "hostname" => @local_ip,
      "port" => provisioned_service.port,
      "password" => provisioned_service.password,
      "name" => provisioned_service.name
    }
    @nats.publish(reply, Yajl::Encoder.encode(response))
  rescue => e
    @logger.warn(e)
  end

  def on_unprovision(msg, reply)
    @logger.debug("Unprovision request: #{msg}.")
    unprovision_message = Yajl::Parser.parse(msg)

    provisioned_service = ProvisionedService.get(unprovision_message["name"])
    raise "Could not find service: #{unprovision_message["name"]}" if provisioned_service.nil?

    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")

    Process.kill(9, provisioned_service.pid) if provisioned_service.running?

    dir = File.join(@base_dir, provisioned_service.name)

    pid = fork
    if pid
      Process.detach(pid)
    else
      $0 = "Unprovisioning mongodb service: #{provisioned_service.name}"
      close_fds
      FileUtils.rm_rf(dir)
      exit
    end

    @available_memory += provisioned_service.memory

    raise "Could not delete service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
    @logger.debug("Successfully fulfilled unprovision request: #{msg}.")
  rescue => e
    @logger.warn(e)
  end

  def send_node_announcement(reply = nil)
    @logger.debug("Sending announcement for #{reply || "everyone"}")
    response = {
      :id => @node_id,
      :available_memory => @available_memory
    }
    @nats.publish(reply || "MongoDBaaS.announce", Yajl::Encoder.encode(response))
  end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect}")

    memory = @max_memory

    pid = fork
    if pid
      @logger.debug("Service #{provisioned_service.name} started with pid #{pid}")
      @available_memory -= memory
      # In parent, detach the child.
      Process.detach(pid)
      pid
    else
      $0 = "Starting MongoDB service: #{provisioned_service.name}"
      close_fds

      port = provisioned_service.port
      password = provisioned_service.password
      dir = File.join(@base_dir, provisioned_service.name)
      data_dir = File.join(dir, "data")
      log_file = File.join(dir, "log")

      config = @config_template.result(binding)
      config_path = File.join(dir, "mongodb.conf")

      FileUtils.mkdir_p(dir)
      FileUtils.mkdir_p(data_dir)
      FileUtils.rm_f(log_file)
      FileUtils.rm_f(config_path)
      File.open(config_path, "w") {|f| f.write(config)}

      exec("#{@mongod_path} -f #{config_path}")
    end
  end

  def memory_for_service(provisioned_service)
    case provisioned_service.plan
      when :free then 256
      else
        raise "Invalid plan: #{provisioned_service.plan}"
    end
  end

  def close_fds
    3.upto(get_max_open_fd) do |fd|
      begin
        IO.for_fd(fd, "r").close
      rescue
      end
    end
  end

  def get_max_open_fd
    max = 0

    dir = nil
    if File.directory?("/proc/self/fd/") # Linux
      dir = "/proc/self/fd/"
    elsif File.directory?("/dev/fd/") # Mac
      dir = "/dev/fd/"
    end

    if dir
      Dir.foreach(dir) do |entry|
        begin
          pid = Integer(entry)
          max = pid if pid > max
        rescue
        end
      end
    else
      max = 65535
    end

    max
  end

end
