require File.dirname(__FILE__) + '/spec_helper'

require 'redis_service/redis_node'

module VCAP
  module Services
    module Redis
      class Node
         attr_reader :base_dir, :redis_server_path, :redis_client_path, :local_ip, :available_memory, :max_memory, :max_swap, :node_id, :config_template, :free_ports
      end
    end
  end
end

describe VCAP::Services::Redis::Node do

  before :all do
    @logger = Logger.new(STDOUT, "daily")
    @logger.level = Logger::DEBUG
    @options = {
      :logger => @logger,
      :base_dir => "/var/vcap/services/redis/instances",
      :redis_server_path => "redis-server",
      :redis_client_path => "redis-cli",
      :local_ip => "127.0.0.1",
      :available_memory => 4096,
      :max_memory => 16,
      :max_swap => 32,
      :node_id => "redis-node-1",
      :config_template => File.expand_path("../resources/redis.conf.erb", File.dirname(__FILE__)),
      :local_db => "sqlite3:/var/vcap/services/redis/redis_node.db",
      :port_range => Range.new(5000, 25000),
      :mbus => "nats://localhost:4222"
    }
    EM.run do
			@node = VCAP::Services::Redis::Node.new(@options)
      EM.add_timer(1) {EM.stop}
		end
  end

  before :each do
			@provisioned_service          = VCAP::Services::Redis::Node::ProvisionedService.new
			@provisioned_service.name     = "redis-#{UUIDTools::UUID.random_create.to_s}"
			@provisioned_service.port     = 11111
			@provisioned_service.plan     = :free
			@provisioned_service.password = UUIDTools::UUID.random_create.to_s
			@provisioned_service.memory   = @options[:max_memory]
	end

  after :all do
    EM.run do
			@node.shutdown
      EM.add_timer(1) {EM.stop}
		end
	end

  describe 'Node.initialize' do
    it "should set up a base directory" do
      @node.base_dir.should be @options[:base_dir]
    end

    it "should set up a redis server path" do
      @node.redis_server_path.should be @options[:redis_server_path]
    end

    it "should set up a redis client path" do
      @node.redis_client_path.should be @options[:redis_client_path]
    end

    it "should set up a local IP" do
      @node.local_ip.should be
    end

    it "should set up an available memory size" do
      @node.available_memory.should be
    end

    it "should set up a maximum memory size" do
      @node.max_memory.should be @options[:max_memory]
    end

    it "should set up a max swap size " do
      @node.max_swap.should be @options[:max_swap]
    end

    it "should load the redis configuration template" do
      @node.config_template.should be
    end

    it "should setup a free port set" do
      @node.free_ports.should be
    end
  end

  describe 'Node.start_provisioned_services' do
	  it "should check whether provisioned service is running or not" do
      EM.run do
				@provisioned_service.pid = @node.start_instance(@provisioned_service)
				EM.add_timer(1) {
					@provisioned_service.running?.should == true
					@node.stop_instance(@provisioned_service)
				}
				EM.add_timer(2) {
					@provisioned_service.running?.should == false
					EM.stop
				}
			end
		end

		it "should not start a new instance if the service is already started when start all provisioned services" do
      EM.run do
				@provisioned_service.pid = @node.start_instance(@provisioned_service)
				@provisioned_service.save
				EM.add_timer(1) {
					@node.start_provisioned_services
					provisioned_service = VCAP::Services::Redis::Node::ProvisionedService.get(@provisioned_service.name)
					provisioned_service.pid.should == @provisioned_service.pid
					@node.stop_instance(@provisioned_service)
					@provisioned_service.destroy
				}
				EM.add_timer(2) { EM.stop }
			end
		end

		it "should start a new instance if the service is not started when start all provisioned services" do
      EM.run do
				@provisioned_service.pid = @node.start_instance(@provisioned_service)
				@provisioned_service.save
				@node.stop_instance(@provisioned_service)
				EM.add_timer(1) {
					@node.start_provisioned_services
					provisioned_service = VCAP::Services::Redis::Node::ProvisionedService.get(@provisioned_service.name)
					provisioned_service.pid.should_not == @provisioned_service.pid
					@node.stop_instance(@provisioned_service)
					@provisioned_service.destroy
				}
        EM.add_timer(2) { EM.stop }
      end
		end
	end

  describe 'Node.announcement' do
    it "should send node announcement" do
			@node.announcement.should be
    end

		it "should send available_memory in announce message" do
			@node.announcement[:available_memory].should == @node.available_memory
		end
  end

  describe "Node.on_provision" do
    it "should start a redis server instance when doing provision" do
			@provisioned_service.pid      = @node.start_instance(@provisioned_service)
			EM.run do
				EM.add_timer(1) {
					%x[#{@options[:redis_client_path]} -p #{@provisioned_service.port} -a #{@provisioned_service.password} get test].should == "\n"
					@node.stop_instance(@provisioned_service)
				}
        EM.add_timer(2) { EM.stop }
			end
		end

    it "should delete the provisioned service port in free port list when finish a provision" do
		  response = @node.provision(:free)
			@node.free_ports.include?(response["port"]).should == false
			@node.unprovision(response["name"])
		end

    it "should decrease available memory when finish a provision" do
			old_memory = @node.available_memory
		  response = @node.provision(:free)
			(old_memory - @node.available_memory).should == @node.max_memory
			@node.unprovision(response["name"])
    end

		it "should send provision messsage when finish a provision" do
		  response = @node.provision(:free)
			response["hostname"].should be
			response["port"].should be
			response["password"].should be
			response["name"].should be
			@node.unprovision(response["name"])
		end
  end

  describe "Node.on_unprovision" do
    it "should stop the redis server instance when doing unprovision" do
			@provisioned_service.pid      = @node.start_instance(@provisioned_service)
			EM.run do
				EM.add_timer(1) {
					@node.stop_instance(@provisioned_service)
					%x[#{@options[:redis_client_path]} -p #{@provisioned_service.port} -a #{@provisioned_service.password} get test].should_not == "\n"
				}
        EM.add_timer(2) { EM.stop }
			end
		end

    it "should add the provisioned service port in free port list when finish an unprovision" do
		  response = @node.provision(:free)
			@node.unprovision(response["name"])
			@node.free_ports.include?(response["port"]).should == true
		end

    it "should increase available memory when finish an unprovision" do
		  response = @node.provision(:free)
			old_memory = @node.available_memory
			@node.unprovision(response["name"])
			(@node.available_memory - old_memory).should == @node.max_memory
    end

    it "should raise error when unprovision an non-existed name" do
			@node.unprovision("non-existed")
    end
  end

	describe "Node.save_provisioned_service" do
	  it "shuold raise error when save failed" do
			@provisioned_service.pid = 100
			@provisioned_service.persisted_state=DataMapper::Resource::State::Immutable
			begin
				@node.save_provisioned_service(@provisioned_service)
			rescue => e
			  e.should be
			end
		end
	end

	describe "Node.destory_provisioned_service" do
	  it "shuold raise error when destroy failed" do
			begin
				@node.destroy_provisioned_service(@provisioned_service)
			rescue => e
			  e.should be
				@node.destroy_provisioned_service(@provisioned_service)
			end
		end
	end

  describe "Node.memory_for_service" do
    it "should return memory size by the plan" do
			provisioned_service = VCAP::Services::Redis::Node::ProvisionedService.new
			provisioned_service.plan = :free
		  @node.memory_for_service(provisioned_service).should == 16
    end
  end

  describe "Node.memory_for_service" do
    it "should return the maximum open file descriptor number" do
		  @node.get_max_open_fd.should be
		end
	end

  describe "Node.close_fds" do
    it "should close all file open descriptors" do
			pid = fork
			if pid
				Process.detach(pid)
			else 
			  @node.close_fds
			end
		end
	end
end
