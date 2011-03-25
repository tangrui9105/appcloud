require 'rubygems'

require 'vcap/common'
require 'yaml'

CC_CONFIG_FILE = File.expand_path("../../../cloudcontroller/config/cloudcontroller.yml", __FILE__)

def default_cloud_controller_uri
  puts CC_CONFIG_FILE
  config = YAML.load_file(CC_CONFIG_FILE)
  config['external_uri']
end

def parse_gateway_config(config_file)
  config = YAML.load_file(config_file)
  config = VCAP.symbolize_keys(config)

  # TODO: Make this declarative

  token = config[:token]
  raise "Token missing" unless token
  raise "Token must be a String or Int, #{token.class} given" unless (token.is_a?(Integer) || token.is_a?(String))
  config[:token] = token.to_s

  config
end
