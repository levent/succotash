require 'redis'
require 'geocoder'

module Cosm
  class GeoCountBolt < RedStorm::SimpleBolt
    on_init do
      @redis = Redis.new(:host => 'localhost', :port => 6379)
    end

    on_receive do |tuple|
      coords = tuple.getString(0)
      begin
        @redis.zincrby 'cosm_countries', 1, Geocoder.search(coords).first.country
      rescue
      end
    end
  end
end
