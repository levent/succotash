require 'red_storm'
require 'json'
require 'socket'
require 'thread'
require 'redis'

module RedStorm
  module Cosm
    class CosmFirehoseSpout < RedStorm::SimpleSpout
      output_fields :cosm_feed

      on_send {@q.pop.to_s if @q.size > 0}

      on_init do
        @q = Queue.new

        api_key = ENV['API_KEY']
        resource = 'firehose'
        subscribe = "{\"method\":\"subscribe\", \"resource\":\"#{resource}\", \"headers\":{\"X-ApiKey\":\"#{api_key}\"}}"
        Thread.new do
          Thread.current.abort_on_exception = true

          s = TCPSocket.new 'api.cosm.com', 8081
          s.puts subscribe
          while line = s.gets
            begin
            @q << line
            rescue
            end
          end
          s.close
        end
      end

    end

    class JsonParseTagsBolt < RedStorm::SimpleBolt
      output_fields :tag
      on_receive do |something|
        tags = []
        json = JSON.parse(something.getString(0))['body']
        if json
          tags = [*json['datastreams']].map{|ds| ds['tags']} << json['tags']
        end
        tags = tags.flatten.compact
        if tags.any?
          tags.map{|w| [w]}
        else
          ['N/A']
        end
      end
    end

    class TagDowncaseBolt < RedStorm::SimpleBolt
      output_fields :tag
      on_receive do |tuple|
        [tuple.getString(0).downcase]
      end
    end

    class TagCountBolt < RedStorm::SimpleBolt
      on_init do
        @redis = Redis.new(:host => 'localhost', :port => 6379)
      end

      on_receive do |tuple|
        @redis.zincrby 'cosm_tags', 1, tuple.getString(0)
      end
    end

    class TagCountTopology < RedStorm::SimpleTopology

      spout CosmFirehoseSpout

      bolt JsonParseTagsBolt, :parallelism => 2 do
        source CosmFirehoseSpout, :shuffle => true
      end

      bolt TagDowncaseBolt, :parallelism => 12 do
        source JsonParseTagsBolt, :shuffle => true
      end

      bolt TagCountBolt, :parallelism => 4 do
        source TagDowncaseBolt, :shuffle => true
        debug true
      end

      configure do |env|
        debug false
        set "topology.worker.childopts", "-Djruby.compat.version=RUBY1_9"
        case env
        when :local
          max_task_parallelism 3
        when :cluster
          max_task_parallelism 5
          num_workers 20
          max_spout_pending(1000);
        end
      end

    end
  end
end
