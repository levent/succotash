module Cosm
  class ExtractTagsBolt < RedStorm::SimpleBolt
    output_fields :tag
    on_receive do |something|
      tags = []
      json = JSON.parse(something.getString(0))['body']
      begin
        location = json['location']
        lat = location['lat']
        lon = location['lon']
        geo = "#{lat},#{lon}"
      rescue
      end
      if json
        tags = [*json['datastreams']].map{|ds| ds['tags']} << json['tags']
      end
      tags = tags.flatten.compact
      tags.any? ? tags.map{|w| [w]} : nil
    end
  end
end
