require File.dirname(__FILE__) + '/spec_helper'

require 'rabbit_service/rabbit_node'

module VCAP
  module Services
    module Rabbit
      class Node
         attr_reader :rabbit_ctl, :rabbit_server, :local_ip, :available_memory, :max_memory, :local_db, :mbus
				 attr_accessor :logger
      end
    end
  end
end

describe VCAP::Services::Rabbit::Node do
  before :all do
    @logger = Logger.new(STDOUT, "daily")
    @logger.level = Logger::DEBUG
    @options = {
      :logger => @logger,
      :rabbit_ctl => "rabbitmqctl",
      :rabbit_server => "rabbitmq-server",
      :ip_route => "127.0.0.1",
      :available_memory => 4096,
      :max_memory => 16,
      :node_id => "rabbit-node-1",
      :local_db => "sqlite3:/tmp/rabbit_node.db",
      :mbus => "nats://localhost:4222"
    }
		EM.run do
			@node = VCAP::Services::Rabbit::Node.new(@options)
			EM.add_timer(0.1) {
				EM.stop
			}
		end
  end

  before :each do
    @provisioned_service = VCAP::Services::Rabbit::Node::ProvisionedService.new
    @provisioned_service.name = "rabbit-#{UUIDTools::UUID.random_create.to_s}"
    @provisioned_service.plan = :free
    @provisioned_service.plan_option = ""
    @provisioned_service.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    @provisioned_service.admin_username = "au" + @node.generate_credential
    @provisioned_service.admin_password = "ap" + @node.generate_credential
    @provisioned_service.memory = @options[:memory]

		@application = VCAP::Services::Rabbit::Node::BindingApplication.new
		@application.application_id = "application-#{UUIDTools::UUID.random_create.to_s}"
		@application.binding_options = :all
		@application.vhost = "unittest"
		@application.username = "u" + @node.generate_credential
		@application.password = "p" + @node.generate_credential
		@application.permissions = '".*" ".*" ".*"'
	end

  describe "Node.initialize" do
    it "should set up a rabbit controler path" do
      @node.rabbit_ctl.should be @options[:rabbit_ctl]
    end
    it "should set up a rabbit server path" do
      @node.rabbit_server.should be @options[:rabbit_server]
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
    it "should setup a local database path" do
      @node.local_db.should be @options[:local_db]
		end
	end

  describe "Node.start" do
		it "should start db with correct options" do
		  @node.start_db.should be
		end
		
		it "should start rabbit server with correct options" do
			EM.run do
				@node.start_server.should be
				EM.add_timer(1) {
					EM.stop
				}
			end
		end

		it "should start successfully" do
		  @node.start.should be
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

	describe "Node.provision" do
	  it "should create a new vhost with an administrator in rabbit server" do
		  EM.run do
				response = @node.provision(:free)
				EM.add_timer(1) {
					AMQP.start(:host => response["hostname"],
										 :vhost => response["vhost"],
										 :user => response["username"],
										 :pass => response["password"]) do |conn|
						conn.connected?.should == true
					end
					AMQP.stop
				}
				EM.add_timer(2) {
					@node.unprovision(response["name"])
					EM.stop
				}
			end
		end

    it "should decrease available memory when finish a provision" do
			EM.run do
				old_memory = @node.available_memory
				response = @node.provision(:free)
				EM.add_timer(1) {
					(old_memory - @node.available_memory).should == @node.max_memory
					@node.unprovision(response["name"])
					EM.stop
				}
			end
    end

		it "should send provision messsage when finish a provision" do
			EM.run do
				response = @node.provision(:free)
				EM.add_timer(1) {
					response["name"].should be
					response["hostname"].should be
					response["vhost"].should be
					response["username"].should be
					response["password"].should be
					@node.unprovision(response["name"])
					EM.stop
				}
			end
		end
	end

	describe "Node.unprovision" do
	  it "should delete the vhost and the administrator in rabbit server" do
		  EM.run do
				response = @node.provision(:free)
				EM.add_timer(1) {
					@node.unprovision(response["name"])
				}
				EM.add_timer(2) {
					AMQP.start(:host => response["hostname"],
										 :vhost => response["vhost"],
										 :user => response["username"],
										 :pass => response["password"]) do |conn|
						conn.connected?.should == false
					end
					AMQP.stop
					EM.stop
				}
			end
		end

    it "should decrease available memory when finish a provision" do
			EM.run do
				response = @node.provision(:free)
				old_memory = 0
				EM.add_timer(1) {
					old_memory = @node.available_memory
					@node.unprovision(response["name"])
				}
				EM.add_timer(2) {
					(@node.available_memory - old_memory).should == @node.max_memory
					EM.stop
				}
			end
    end

    it "should raise error when unprovision an non-existed name" do
      @node.logger.level = Logger::ERROR
			@node.unprovision("non-existed")
      @node.logger.level = Logger::DEBUG
    end
	end

	describe "Node.bind" do
		before :all do
		  EM.run do
				@response = @node.provision(:free)
				EM.add_timer(1) {
					EM.stop
				}
			end
		end

		after :all do
			@node.unprovision(@response["name"])
		end

	  it "should create a new user with specified permission in rabbit server" do
		  EM.run do
				response = @node.bind(@response["name"], "application1")
				EM.add_timer(1) {
					AMQP.start(:host => response["hostname"],
										 :vhost => response["vhost"],
										 :user => response["username"],
										 :pass => response["password"]) do |conn|
						conn.connected?.should == true
					end
					AMQP.stop
				}
				EM.add_timer(2) {
					@node.unbind(@response["name"], "application1")
					EM.stop
				}
			end
		end

		it "should send binding messsage when finish a binding" do
		  EM.run do
				response = @node.bind(@response["name"], "application1")
				EM.add_timer(1) {
					response["hostname"].should be
					response["vhost"].should be
					response["username"].should be
					response["password"].should be
					@node.unbind(@response["name"], "application1")
					EM.stop
				}
			end
		end
	end

	describe "Node.unbind" do
		before :all do
		  EM.run do
				@response = @node.provision(:free)
				EM.add_timer(1) {
					EM.stop
				}
			end
		end

		after :all do
			@node.unprovision(@response["name"])
		end

	  it "should delete user with specified permission in rabbit server" do
		  EM.run do
				response = @node.bind(@response["name"], "application1")
				EM.add_timer(1) {
					@node.unbind(@response["name"], "application1")
				}
				EM.add_timer(2) {
					AMQP.start(:host => response["hostname"],
										 :vhost => response["vhost"],
										 :user => response["username"],
										 :pass => response["password"]) do |conn|
						conn.connected?.should == false
					end
					AMQP.stop
					EM.stop
				}
			end
		end

		it "should send binding messsage when finish a binding" do
		  EM.run do
				response = @node.bind(@response["name"], "application1")
				EM.add_timer(1) {
					@node.unbind(@response["name"], "application1").should be
					EM.stop
				}
			end
		end
	end

	describe "Node.rabbitmqctl" do
	  it "should add and delete vhost in rabbit server" do
		  @node.add_vhost("test_vhost")
		  @node.delete_vhost("test_vhost")
		end

	  it "should add and delete user in rabbit server" do
		  @node.add_user("test_user", "test_password")
		  @node.delete_user("test_user")
		end

	  it "should set user permissions in rabbit server" do
		  @node.add_vhost("test_vhost")
		  @node.add_user("test_user", "test_password")
			@node.set_permissions('test_vhost', 'test_user', '".*" ".*" ".*"')
		  @node.delete_user("test_user")
		  @node.delete_vhost("test_vhost")
		end
	end
end
