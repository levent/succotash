require 'thread'
require 'socket'

module Cosm
  class FirehoseSpout < RedStorm::SimpleSpout
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
end
