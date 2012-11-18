require 'red_storm'
require 'cosm/firehose_spout'
require 'cosm/downcase_bolt'
require 'cosm/extract_country_bolt'
require 'cosm/extract_tags_bolt'
require 'cosm/tag_count_bolt'

module Cosm

  class FirehoseCountTopology < RedStorm::SimpleTopology

    spout FirehoseSpout

    bolt ExtractCountryBolt, :parallelism => 2 do
      source FirehoseSpout, :shuffle => true
    end

    bolt ExtractTagsBolt, :parallelism => 2 do
      source FirehoseSpout, :shuffle => true
    end

    bolt DowncaseBolt, :parallelism => 12 do
      source ExtractTagsBolt, :shuffle => true
    end

    bolt TagCountBolt, :parallelism => 4 do
      source DowncaseBolt, :shuffle => true
    end

    # bolt GeoCountBolt, :parallelism => 1 do
    #   source ExtractCountryBolt, :shuffle => true
    # end

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
