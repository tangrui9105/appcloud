require "set"
require "datamapper"
require "uuidtools"

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

require "rabbit_service/common"

VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

class VCAP::Services::Rabbit::Node

	include VCAP::Services::Rabbit::Common

  class ProvisionedService
    include DataMapper::Resource
    property :name,            String,      :key => true
    property :vhost,           String,      :required => true
    property :admin_username,  String,      :required => true
    property :admin_password,  String,      :required => true
    property :plan,            Enum[:free], :required => true
    property :plan_option,     String,      :required => false
    property :memory,          Integer,     :required => true
		has n, :binding_applications
  end

  class BindingApplication
    include DataMapper::Resource
    # Name, binding_options, vhost can determine a binding,
    # if the three are the same, then share the same user.
		property :id,              Serial
    property :application_id,  String,   :required => true
    property :binding_options, String,   :required => true
    property :username,        String,   :required => true
    property :password,        String,   :required => true
    property :permissions,     String,   :required => true
		belongs_to :provisioned_service
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
    service = ProvisionedService.new
    service.name = "rabbit-#{UUIDTools::UUID.random_create.to_s}"
    service.plan = plan
    service.plan_option = ""
    service.vhost = "v" + UUIDTools::UUID.random_create.to_s.gsub(/-/, "")
    service.admin_username = "au" + generate_credential
    service.admin_password = "ap" + generate_credential
    service.memory   = @max_memory
			
		@available_memory -= service.memory

		save_provisioned_service(service)

    add_vhost(service.vhost)
    add_user(service.admin_username, service.admin_password) 
    set_permissions(service.vhost, service.admin_username, '".*" ".*" ".*"') 

    response = {
			"name" => service.name,
			"hostname" => @local_ip,
			"vhost" => service.vhost,
			"username" => service.admin_username,
			"password" => service.admin_password
    }
  rescue => e
		@available_memory += service.memory
    @logger.warn(e)
		nil
  end

  def unprovision(service_id)
    service = get_provisioned_service(service_id)
		# Delete all bindings in this service
		service.binding_applications.each do |application|
			delete_user(application.username)
			destroy_binding_application(application)
		end
    delete_user(service.admin_username)
    delete_vhost(service.vhost)
		destroy_provisioned_service(service)
    @available_memory += service.memory
		true
  rescue => e
    @logger.warn(e)
		nil
  end

  def bind(service_id, application_id, binding_options = :all)
    application = get_binding_application(service_id, application_id, binding_options)
    if application.nil?
		  service = get_provisioned_service(service_id)
      application = BindingApplication.new
      application.application_id = application_id
      application.binding_options = binding_options
      application.username = "u" + generate_credential
      application.password = "p" + generate_credential
      application.provisioned_service = service
      application.permissions = '".*" ".*" ".*"'
			save_binding_application(application)
      add_user(application.username, application.password)
      set_permissions(service.vhost, application.username, application.permissions)
		else
			service = application.provisioned_service
		end

    response = {
			"hostname" => @local_ip,
			"vhost" => service.vhost,
			"username" => application.username,
			"password" => application.password
    }
  rescue => e
    @logger.warn(e)
		nil
  end

  def unbind(service_id, application_id, binding_options = :all)
    application = get_binding_application(service_id, application_id, binding_options)
		raise "Could not find application: #{application_id}" if application.nil?
    delete_user(application.username)
		destroy_binding_application(application)
		true
  rescue => e
    @logger.warn(e)
		nil
  end

	def save_provisioned_service(provisioned_service)
		raise "Could not save service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.save
	end

	def destroy_provisioned_service(provisioned_service)
    raise "Could not delete service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy
	end

	def get_provisioned_service(service_id)
    provisioned_service = ProvisionedService.get(service_id)
		raise "Could not find service: #{service_id}" if provisioned_service.nil?
		provisioned_service
	end

	def save_binding_application(application)
		raise "Could not save application: #{application.errors.pretty_inspect}" unless application.save
	end

	def destroy_binding_application(application)
    raise "Could not delete application: #{application.errors.pretty_inspect}" unless application.destroy
	end

	def get_binding_application(service_id, application_id, binding_options)
    service = ProvisionedService.get(service_id)
		raise "Could not find service: #{service_id}" if service.nil?
    application = service.binding_applications.first(:application_id => application_id, :binding_options => binding_options)
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
