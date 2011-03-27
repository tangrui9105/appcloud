require "erb"
require "fileutils"
require "logger"
require "pp"

require "datamapper"
require "uuidtools"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Redis
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require 'redis_service/common'

class VCAP::Services::Redis::Node

  include VCAP::Services::Redis::Common

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
  end

  def initialize(options)
    super(options)

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @redis_server_path = options[:redis_server_path]
    @redis_client_path = options[:redis_client_path]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]
    @max_swap = options[:max_swap]
    @config_template = ERB.new(File.read(options[:config_template]))
    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
		@local_db = options[:local_db]

		setup_db
		start_provisioned_services
  end

	def setup_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
	end

	def start_provisioned_services
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
				save_provisioned_service(provisioned_service)
      rescue => e
        @logger.warn("Error starting service #{provisioned_service.name}: #{e}")
      end
    end
	end

  def announcement
    a = {
      :available_memory => @available_memory
    }
  end

  def provision(plan)
    port = @free_ports.first
    @free_ports.delete(port)

    provisioned_service          = ProvisionedService.new
    provisioned_service.name     = "redis-#{UUIDTools::UUID.random_create.to_s}"
    provisioned_service.port     = port
    provisioned_service.plan     = plan
    provisioned_service.password = UUIDTools::UUID.random_create.to_s
    provisioned_service.memory   = @max_memory
    provisioned_service.pid      = start_instance(provisioned_service)

		save_provisioned_service(provisioned_service)

    response = {
      "hostname" => @local_ip,
      "port" => provisioned_service.port,
      "password" => provisioned_service.password,
      "name" => provisioned_service.name
    }
  rescue => e
    @logger.warn(e)
		nil
  end

  def unprovision(service_id)
    provisioned_service = get_provisioned_service(service_id)

    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")
    stop_instance(provisioned_service) if provisioned_service.running?
    @available_memory += provisioned_service.memory
		destroy_provisioned_service(provisioned_service)
    @free_ports.add(provisioned_service.port)

    @logger.debug("Successfully fulfilled unprovision request: #{service_id}.")
		true
  rescue => e
    @logger.warn(e)
		nil
  end

  def bind(service_id, application_id, binding_options = :all)
	  provisioned_service = get_provisioned_service(service_id)
    response = {
			"hostname" => @local_ip,
			"port" => provisioned_service.port,
			"password" => provisioned_service.password
    }
  rescue => e
    @logger.warn(e)
		nil
	end

  def unbind(service_id, application_id, binding_options = :all)
	  true
	end

	def save_provisioned_service(provisioned_service)
    unless provisioned_service.save
		  stop_instance(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end
	end

	def destroy_provisioned_service(provisioned_service)
    raise "Could not delete service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
	end

	def get_provisioned_service(name)
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?
		provisioned_service
	end

  def start_instance(provisioned_service)
    @logger.debug("Starting: #{provisioned_service.pretty_inspect} on port #{provisioned_service.port}")

		# FIXME: it need call mememory_for_service() to get the memory according to the plan in the further.
    memory = @max_memory

    pid = fork
    if pid
      @logger.debug("Service #{provisioned_service.name} started with pid #{pid}")
      @available_memory -= memory
      # In parent, detch the child.
      Process.detach(pid)
      pid
    else
			$0 = "Starting Redis service: #{provisioned_service.name}"
			close_fds

			port = provisioned_service.port
			password = provisioned_service.password
			dir = File.join(@base_dir, provisioned_service.name)
			data_dir = File.join(dir, "data")
			log_file = File.join(dir, "log")
			swap_file = File.join(dir, "redis.swap")
			vm_max_memory = (memory * 0.7).round
			vm_pages = (@max_swap * 1024 * 1024 / 32).round # swap in bytes / size of page (32 bytes)

			config = @config_template.result(Kernel.binding)
			config_path = File.join(dir, "redis.conf")

			FileUtils.mkdir_p(dir)
			FileUtils.mkdir_p(data_dir)
			FileUtils.rm_f(log_file)
			FileUtils.rm_f(config_path)
			File.open(config_path, "w") {|f| f.write(config)}

			exec("#{@redis_server_path} #{config_path}")
    end
  end

	def stop_instance(provisioned_service)
    dir = File.join(@base_dir, provisioned_service.name)
		FileUtils.rm_rf(dir)
	  %x[#{@redis_client_path} -p #{provisioned_service.port} -a #{provisioned_service.password} shutdown]
	end

  def memory_for_service(provisioned_service)
    case provisioned_service.plan
      when :free then 16
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
