$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'rspec'

class RedisTests
  def initialize
    @has_tests = false
  end

  def has_tests
    @has_tests
  end
end

