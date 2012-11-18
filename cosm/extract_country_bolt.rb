require 'json'

module Cosm
  class ExtractCountryBolt < RedStorm::SimpleBolt
    output_fields :geo
    on_receive do |something|
      tags = []
      json = JSON.parse(something.getString(0))['body']
      if json
        location = json['location']
        if location && location['lat'] && location['lon']
          "#{location['lat']},#{location['lon']}"
        end
      end
    end
  end
end
