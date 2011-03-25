require "set"
require "datamapper"
require "uuidtools"
require "vcap/common"
require "vcap/component"

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module Rabbit
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

class VCAP::Services::Rabbit::Node
  class ProvisionedService
    include DataMapper::Resource
    property :name,            String,      :key => true
    property :vhost,           String,      :required => true
    property :admin_username,  String,      :required => true
    property :admin_password,  String,      :required => true
    property :plan,            Enum[:free], :required => true
    property :plan_option,     String,      :required => false
    property :memory,          Integer,     :required => true
  end

  class BindingApplication
    include DataMapper::Resource
    # Name, binding_options, vhost can determine a binding,
    # if the three are the same, then share the same user.
    property :name,            String,   :key => true
    property :binding_options, String,   :required => false
    property :vhost,           String,   :required => true
    property :username,        String,   :required => true
    property :password,        String,   :required => true
    property :permissions,     String,   :required => true
  end

  def initialize(options)
	  super(options)
    @rabbit_ctl = options[:rabbit_ctl]
    @rabbit_server = options[:rabbit_server]
    @available_memory = options[:available_memory]
    @max_memory = options[:max_memory]
		@local_db = options[:local_db]
    @binding_options = ["configure", "write", "read"]
		@options = options
	end

	def start
    @logger.info("Starting rabbit service node...")
	  start_db
    start_server
  end

	def start_db
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
	end

	def announcement
		a = { 
			:available_memory => @available_memory
		}   
	end

  def provision(plan)
    provisioned_service = ProvisionedService.new
    provisioned_service.name = "rabbit-#{UUIDTools::UUID.random_create.to_s}"
    provisioned_service.plan = plan
    provisioned_service.plan_option = ""
    provisioned_service.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    provisioned_service.admin_username = "au" + generate_credential
    provisioned_service.admin_password = "ap" + generate_credential
    provisioned_service.memory   = @max_memory
			
		@available_memory -= provisioned_service.memory

		save_provisioned_service(provisioned_service)

    add_vhost(provisioned_service.vhost)
    add_user(provisioned_service.admin_username, provisioned_service.admin_password) 
    set_permissions(provisioned_service.vhost, provisioned_service.admin_username, '".*" ".*" ".*"') 

    response = {
			"name" => provisioned_service.name,
			"hostname" => @local_ip,
			"vhost" => provisioned_service.vhost,
			"username" => provisioned_service.admin_username,
			"password" => provisioned_service.admin_password
    }
  rescue => e
		@available_memory += provisioned_service.memory
    @logger.warn(e)
  end

  def unprovision(name)
    provisioned_service = get_provisioned_service(name)
    delete_user(provisioned_service.admin_username)
    delete_vhost(provisioned_service.vhost)
		destroy_provisioned_service(provisioned_service)
    @available_memory += provisioned_service.memory

  rescue => e
    @logger.warn(e)
  end

  def binding(application_id, service_id, binding_options = :all)
    provisioned_service = get_provisioned_service(service_id)

    application = BindingApplication.first(:name => application_id, :binding_options => binding_options, :vhost => provisioned_service.vhost)
    if application.nil?
      application = BindingApplication.new
      application.name = application_id
      application.binding_options = binding_options
      application.username = "u" + generate_credential
      application.password = "p" + generate_credential
      application.vhost = provisioned_service.vhost
      application.permissions = '".*" ".*" ".*"'
			save_binding_application(application)
      add_user(application.username, application.password)
      set_permissions(application.vhost, application.username, application.permissions)
		end

    response = {
		  "name" => application_id,
			"hostname" => @local_ip,
			"vhost" => application.vhost,
			"username" => application.username,
			"password" => application.password
    }
  rescue => e
    @logger.warn(e)
  end

  def unbinding(application_id)
    application = get_binding_application(application_id)
    delete_user(application.username)
		destroy_binding_application(application)

  rescue => e
    @logger.warn(e)
  end

	def save_provisioned_service(provisioned_service)
		raise "Could not save service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.save
	end

	def destroy_provisioned_service(provisioned_service)
    raise "Could not delete service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
	end

	def get_provisioned_service(name)
    provisioned_service = ProvisionedService.get(name)
		raise "Could not find service: #{name}" if provisioned_service.nil?
		provisioned_service
	end

	def save_binding_application(application)
		raise "Could not save application: #{application.errors.pretty_inspect}" unless application.save
	end

	def destroy_binding_application(application)
    raise "Could not delete application: #{application.errors.pretty_inspect}" unless application.destroy
	end

	def get_binding_application(name)
    application = BindingApplication.get(name)
		raise "Could not find application: #{name}" if application.nil?
		application
	end


  def start_server
    %x[#{@rabbit_server} -detached]
  end

  def add_vhost(vhost)
    %x[#{@rabbit_ctl} add_vhost #{vhost}]
  end

  def delete_vhost(vhost)
    %x[#{@rabbit_ctl} delete_vhost #{vhost}]
  end

  def add_user(username, password)
    %x[#{@rabbit_ctl} add_user #{username} #{password}]
  end

  def delete_user(username)
    %x[#{@rabbit_ctl} delete_user #{username}]
  end

  def set_permissions(vhost, username, permissions)
    %x[#{@rabbit_ctl} set_permissions -p #{vhost} #{username} #{permissions}]
  end

  def generate_credential(length = 12)
    Array.new(length) {VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)]}.join
  end
end
