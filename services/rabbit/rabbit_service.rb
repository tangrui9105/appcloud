
require 'optparse'
require 'logger'
require 'erb'

require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

require 'vcap/common'
require 'vcap/component'

SERVICE_TYPE = 'messaging'
SERVICE_VENDOR = 'rabbitmq'
VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

config_file = File.join(File.dirname(__FILE__), 'config/rabbit_service.yml')
OptionParser.new do |opts|
  opts.banner = 'Usage: rabbit_service [options]'
  opts.on("-c", "--config [ARG]", "Configuration File") do |opt|
    config_file = opt
  end
  opts.on("-h", "--help", "Help") do
    puts opts
    exit
  end
end.parse!

begin
  config = File.open(config_file) do |f|
    YAML.load(f)
  end
rescue => e
  puts "Could not read configuration file:  #{e}"
  exit
end

@options = {
  :rabbit => {
    :host  => config['rabbit']['host'],
    :port  => config['rabbit']['port'],
    :vhost => config['rabbit']['vhost'],
    :user  => config['rabbit']['user'],
    :pass  => config['rabbit']['pass'],
    :ctl   => config['rabbit']['ctl'] || 'rabbitmqctl',
  }
}

@logger = Logger.new(config['log_file'] ? config['log_file'] : STDOUT)

if Process.uid != 0
  @logger.fatal("RabbitMQ Service requires root privileges."); exit
end

def shutdown(pidfile)
  @connection.close if @connection
  FileUtils.rm_f(pidfile)
  NATS.stop
end

def generate_name(prefix)
  "services.#{prefix}.#{SERVICE_TYPE}.#{SERVICE_VENDOR}-#{UUIDTools::UUID.random_create.to_s}"
end

def error(msg)
  STDERR.puts(msg)
  exit 1
end

def create_vhost_and_user(vhost, user, pass)
  begin
    @logger.info("Creating vhost: #{vhost}")
    #Create vhost
    %x[#{@options[:rabbit][:ctl]} add_vhost #{vhost}]
    @logger.info("Creating credentials: #{user}/#{pass}")

    %x[#{@options[:rabbit][:ctl]} add_user #{user} #{pass}]
    @logger.info("Adding permissions to #{vhost} for #{user}/#{pass}")

    %x[#{@options[:rabbit][:ctl]} set_permissions -p #{ vhost} #{user} \".*\" \".*\" \".*\"]
  end
end

def delete_vhost_and_user(vhost, user)
  begin
    @logger.info("Deleting vhost: #{vhost}")
    `#{@options[:rabbit][:ctl]} delete_vhost #{vhost}`
    @logger.info("Deleting user: #{user}")
    `#{@options[:rabbit][:ctl]} delete_user #{user}`
  end
end

begin
  @logger.info("Connecting to RabbitMQ Server: #{@options[:rabbit][:host]}:#{@options[:rabbit][:port]}")
  out = system("#{@options[:rabbit][:ctl]} status")

  match = /\[\{rabbit,\"RabbitMQ\",\"(\S+)\"\}/.match(out)
  unless match
    @logger.fatal("Could not connect to rabbitMQ Server: is it running?")
    exit
  end

  version = match[1]
  @logger.info("Connected to RabbitMQ Server, running version: #{version}")
rescue => e
  @logger.fatal("Could not connect to rabbitMQ Server: #{e}")
  exit
end

class ProvisionEntry
  include DataMapper::Resource
  property :vhost,    String,  :key => true
  property :user,     String,  :required => true
  property :pass,     String,  :required => true
  property :tier,     String,  :required => true
end

pidfile = config['pid']

begin
  FileUtils.mkdir_p(File.dirname(pidfile))
  FileUtils.mkdir_p(File.dirname(config['localdb']))
rescue => e
  @logger.fatal "Can't create pid or localdb directory, exiting: #{e}"
end
File.open(pidfile, 'w') { |f| f.puts "#{Process.pid}" }

DataMapper::Logger.new(STDOUT, :info)
DataMapper.setup(:default, "sqlite3://#{config['localdb']}")
DataMapper::auto_upgrade!

@discovery_message = {
  :type => SERVICE_TYPE,
  :vendor => SERVICE_VENDOR,
  :version => version,
  :description => 'RabbitMQ service',
  :tiers => {
    :free => {
      :description => 'Free offering',
      :order => 1
    },
    :standard => {
      :description => 'Standard offering',
      :order => 2
    },
  }
}.to_json

# Starup event loop and messaging

EM.error_handler{ |e|
  if e.kind_of? NATS::Error
    @logger.fatal "NATS problem, #{e}"
  else
    @logger.error "#{e.message} #{e.backtrace.join("\n")}"
  end
  exit
}

def send_start_message
  @logger.debug "Sent services.start message: #{@discovery_message}"
  NATS.publish('services.start', @discovery_message)
end

def generate_credential(length=12)
  Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
end

trap('INT') { shutdown(pidfile) }
trap('TERM'){ shutdown(pidfile) }

NATS.start(:uri => config['mbus']) do

  # Register ourselves with the system
  VCAP::Component.register(:type => 'RabbitMQ-Service',
                           :host => VCAP.local_ip(config['local_route']),
                           :config => config)

  NATS.subscribe('services.discovery') { send_start_message }

  provision_exchange_name = generate_name('provision')

  NATS.subscribe(provision_exchange_name) do |payload, reply|
    @logger.debug "Received provision request: #{payload}"

require 'pp'
pp Time.now

    json_payload = JSON.parse(payload)

    vhost = 'v' + UUIDTools::UUID.random_create.to_s.gsub(/-/, '')
    user  = 'u' + generate_credential
    pass  = 'p' + generate_credential

    provision_entry = ProvisionEntry.new
    provision_entry.user  = user
    provision_entry.pass  = pass
    provision_entry.vhost = vhost
    provision_entry.tier  = json_payload['tier']
    provision_entry.save

    create_vhost_and_user(vhost, user, pass)

    provisioned_message = {
      :type => SERVICE_TYPE,
      :vendor => SERVICE_VENDOR,
      :version => version,
      :tier => json_payload['tier'],
      :options => {
        :host  => @options[:rabbit][:host],
        :port  => @options[:rabbit][:port],
        :user  => user,
        :pass  => pass,
        :vhost => vhost
      }
    }.to_json
    @logger.debug "Replying to provision request: #{provisioned_message}"

pp Time.now
pp reply

    NATS.publish(reply, provisioned_message)
  end

  NATS.subscribe("services.unprovision.#{SERVICE_TYPE}.#{SERVICE_VENDOR}") do |payload, reply|
    @logger.debug "Received unprovision service request: #{payload}"
    json_payload = JSON.parse(payload)
    if json_payload['options']
      provision_options = json_payload['options']
      if provision_options['vhost']
        provision_entry = ProvisionEntry.get(provision_options['vhost'])
        if provision_entry
          if @options[:rabbit][:host] == provision_options['host'] and
              @options[:rabbit][:port] == provision_options['port'] and
              provision_entry.user == provision_options['user'] and
              provision_entry.pass == provision_options['pass'] and
              provision_entry.tier == json_payload['tier']
            provision_entry.destroy!
            delete_vhost_and_user(provision_entry.vhost, provision_entry.user)
          end
        end
      end
    end
  end

  NATS.subscribe("services.find.#{SERVICE_TYPE}.#{SERVICE_VENDOR}") do |payload, reply|
    @logger.debug "Received find service request"
    find_response = {:exchange => provision_exchange_name}.to_json
    @logger.debug "Replying to find request: #{find_response}"
    NATS.publish(reply, find_response)
  end

  send_start_message

  # TODO(dlc) Not sure this makes sense? Listen for CC start instead?
  EM.add_periodic_timer(300) do
    send_start_message
  end
end
