$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')

require 'base/provisioner'
require 'rabbit_service/common'

class VCAP::Services::Rabbit::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Rabbit::Common

  def node_score(node)
    node['available_memory']
  end

end
