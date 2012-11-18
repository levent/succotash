module Cosm
  class DowncaseBolt < RedStorm::SimpleBolt
    output_fields :tag
    on_receive do |tuple|
      [tuple.getString(0).downcase]
    end
  end
end
