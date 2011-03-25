
require 'optparse'
require 'logger'

require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

require 'vcap/common'
require 'vcap/component'

SERVICE_TYPE = 'key-value'
SERVICE_VENDOR = 'memcache'
SERVICE_EXECUTABLE = 'memcached'

config_file = File.join(File.dirname(__FILE__), 'config/memcache_service.yml')
OptionParser.new do |opts|
  opts.banner = 'Usage: memcache_service [options]'
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
  :memcache => {
    :hostname => config['memcache']['host'],
    :port => config['memcache']['port'],
    :default_mem => config['memcache']['default_mem'],
    :max_mem => config['memcache']['max_mem']
  },
  :executable => SERVICE_EXECUTABLE
}

@logger = Logger.new(config['log_file'] ? config['log_file'] : STDOUT)

# Starup event loop and messaging

EM.error_handler{ |e|
  if e.kind_of? NATS::Error
    @logger.fatal "NATS problem, #{e}"
  else
    @logger.error "#{e.message} #{e.backtrace.join("\n")}"
  end
  exit
}

def shutdown(pidfile)
  print "\nInterupted\n\n"
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

begin
  executable = @options[:executable]
  output = system("#{executable} -h")
  error "Can't find the memcached executable : #{executable}" if $? != 0
  # Pull the version from the help output
  match = /memcached\s+(\d{1}\.\d{1}\.\d{1})\s*$/.match(output)
  error "Could not extract version from #{memcached}" unless match
  version = match[1]
end

def run_memcached(entry)
  executable = @options[:executable]
  command = "#{executable} -p #{entry.port} -m #{entry.max_mem}"
  #puts "Command to be executed : #{command}"
  pid = EM.system(command) { puts "Memcached instance exited -- #{entry.name}"}
  entry.pid = pid
  entry.save
end

def kill_memcached(entry)
  return if entry.pid == 0
  puts "Trying to stop memcached entry.."
  `kill #{entry.pid}`
end

def delete_memcached(entry)
  kill_memcached(entry)
  entry.destroy!
end

class ProvisionEntry
  include DataMapper::Resource
  property :name,         String,  :key => true
  property :pid,          Integer, :default => 0
  property :port,         Integer, :required => true
  property :tier,         String,  :required => true
  property :max_mem,      Integer, :required => true
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
  :description => 'memcache key-value cache service',
  :tiers => {
    :free => {
      :description => 'Free offering (64MiB)',
      :order => 1
    },
    :standard => {
      :description => 'Standard offering.',
      :options => {
        :size => {
          :description => 'Cache capacity.',
          :type => 'value',
          :values => ['64MiB', '128MiB', '256MiB', '512MiB']
        }
      },
      :pricing => {
        :type => :flat,
        :period => :month,
        :values => {
          '64MiB' => 5,
          '128MiB' => 10,
          '256MiB' => 20,
          '512MiB' => 40
        }
      },
      :order => 2
    }
  }
}.to_json

def send_start_message
  @logger.debug "Sent services.start message: #{@discovery_message}"
  NATS.publish('services.start', @discovery_message)
end

trap('INT') { shutdown(pidfile) }
trap('TERM'){ shutdown(pidfile) }

NATS.start(:uri => config['mbus']) do

  # Register ourselves with the system
  VCAP::Component.register(:type => 'Memcache-Service',
                           :host => VCAP.local_ip(config['local_route']),
                           :config => config)

  NATS.subscribe('services.discovery') { send_start_message }

  provision_exchange_name = generate_name('provision')
  NATS.subscribe(provision_exchange_name) do |payload, reply|
    @logger.debug "Received provision request: #{payload}"
    json_payload = JSON.parse(payload)

    name = 'd' + UUIDTools::UUID.random_create.to_s.gsub(/-/, '')
    port = @options[:memcache][:port] += 1
    tier = json_payload['tier']

    mem =   mem = @options[:memcache][:default_mem]
    case tier
      when 'tiny' then mem = 64
      when 'small' then mem = 128
      when 'medium' then mem = 256
      when 'large' then mem = 512
    end

    provision_entry = ProvisionEntry.new
    provision_entry.name = name
    provision_entry.tier = tier
    provision_entry.max_mem = mem
    provision_entry.port = port
    provision_entry.save

    # start the instance here
    run_memcached(provision_entry)

    provisioned_message = {
      :type => 'key-value',
      :vendor => 'memcache',
      :version => version,
      :tier => tier,
      :options => {
        :name => name,
        :default_mem => @options[:memcache][:default_mem],
        :hostname => @options[:memcache][:hostname],
        :max_mem => mem,
        :port => port
      }
    }.to_json
    @logger.debug "Replying to provision request: #{provisioned_message}"
    NATS.publish(reply, provisioned_message)
  end

  NATS.subscribe('services.unprovision.key-value.memcache') do |payload, reply|
    @logger.debug "Received unprovision service request: #{payload}"
    json_payload = JSON.parse(payload)
    if json_payload['options']
      provision_options = json_payload['options']
      if provision_options['name']
        provision_entry = ProvisionEntry.get(provision_options['name'])
        delete_memcached(provision_entry) if provision_entry
      end
    end
  end

  NATS.subscribe('services.find.key-value.memcache') do |payload, reply|
    @logger.debug "Received find service request: #{payload}"
    find_response = {:exchange => provision_exchange_name}.to_json
    @logger.debug "Replying to find request: #{find_response}"
    NATS.publish(reply, find_response)
  end

  EM.add_periodic_timer(300) do
    send_start_message
  end

  send_start_message

  # Restart all instances that we know about..
  max_port = 0
  all_existing = ProvisionEntry.all
  all_existing.each do |entry|
    puts "Restarting instance on port:#{entry.port}"
    # Make sure we don't collide with new instances
    max_port = entry.port if entry.port > max_port
    run_memcached(entry)
  end
  puts "Setting to max port of #{max_port}"
  @options[:memcache][:port] = max_port if max_port > @options[:memcache][:port]

end

