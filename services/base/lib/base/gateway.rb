require 'rubygems'
require 'bundler/setup'

require 'optparse'
require 'logger'
require 'net/http'
require 'thin'
require 'yaml'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..')
require 'common/asynchronous_service_gateway'
require 'common/util'
require 'vcap/common'

$LOAD_PATH.unshift File.dirname(__FILE__)
require 'abstract'

module VCAP
  module Services
    module Base
    end
  end
end


class VCAP::Services::Base::Gateway

  abstract :default_config_file
  abstract :provisioner_class

  def start

    config_file = default_config_file

    OptionParser.new do |opts|
      opts.banner = "Usage: $0 [options]"
      opts.on("-c", "--config [ARG]", "Configuration File") do |opt|
        config_file = opt
      end
      opts.on("-h", "--help", "Help") do
        puts opts
        exit
      end
    end.parse!

    begin
      config = YAML.load_file(config_file)
      config = VCAP.symbolize_keys(config)
    rescue => e
      puts "Couldn't read config file: #{e}"
      exit
    end

    logger = Logger.new(config[:log_file] || STDOUT, "daily")
    logger.level = case (config[:log_level] || "INFO")
                   when "DEBUG" then Logger::DEBUG
                   when "INFO" then Logger::INFO
                   when "WARN" then Logger::WARN
                   when "ERROR" then Logger::ERROR
                   when "FATAL" then Logger::FATAL
                   else Logger::UNKNOWN
                   end
    config[:logger] = logger

    if config[:pid]
      pf = VCAP::PidFile.new(config[:pid])
      pf.unlink_at_exit
    end

    config[:host] = VCAP.local_ip(config[:host])
    config[:port] ||= VCAP.grab_ephemeral_port
    config[:service][:label] = "#{config[:service][:name]}-#{config[:service][:version]}"
    config[:service][:url]   = "http://#{config[:host]}:#{config[:port]}"

    # Go!
    EM.run do
      sp = provisioner_class.new(
             :logger   => logger,
             :version  => config[:service][:version],
             :local_ip => config[:host],
             :mbus => config[:mbus]
           )
      sg = VCAP::Services::AsynchronousServiceGateway.new(
             :service => config[:service],
             :token   => config[:token],
             :logger  => logger,
             :provisioner => sp,
             :cloud_controller_uri => config[:cloud_controller_uri] || default_cloud_controller_uri
           )
      Thin::Server.start(config[:host], config[:port], sg)
    end
  end

end
