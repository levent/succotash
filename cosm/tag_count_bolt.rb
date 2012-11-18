require 'redis'

module Cosm
  class TagCountBolt < RedStorm::SimpleBolt
    on_init do
      @redis = Redis.new(:host => 'localhost', :port => 6379)
    end

    on_receive do |tuple|
      @redis.zincrby 'cosm_tags', 1, tuple.getString(0)
    end
  end
end
