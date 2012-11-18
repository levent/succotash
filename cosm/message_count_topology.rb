require 'red_storm'
require 'json'

module RedStorm
  module Cosm
    class TestJsonSpout < RedStorm::SimpleSpout
      output_fields :test

      on_send {@sentences[rand(@sentences.length)]}

      on_init do
        @sentences = [
          '{"tags":"abc"}'
        ]
      end
    end

    class JsonParseBolt < RedStorm::SimpleBolt
      output_fields :test
      on_receive(:ack => true, :anchor => true) {|json| JSON.parse(json)['tags'] }
    end

    class MessageCountTopology < RedStorm::SimpleTopology

      spout TestJsonSpout, :parallelism => 2 do
        debug false
      end

      bolt JsonParseBolt, :parallelism => 2 do
        source TestJsonSpout, :shuffle => true
        debug true
      end

      configure do |env|
        debug false
        set "topology.worker.childopts", "-Djruby.compat.version=RUBY1_9"
        case env
        when :local
          max_task_parallelism 40
        when :cluster
          num_workers 20
          max_spout_pending(1000);
        end
      end

      on_submit do |env|
        if env == :local
          sleep(5)
          cluster.shutdown
        end
      end

    end
  end
end
