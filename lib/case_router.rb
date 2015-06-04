require 'json'
require 'bunny'

class CaseRouter

  def process_new_cases
    conn = Bunny.new

    conn.start

    ch = conn.create_channel
    q  = ch.queue('new_cases')

    q.subscribe(block: true) do |delivery_info, properties, body|
      puts " [x] #{delivery_info.routing_key}:#{body}"

      route_case(JSON.parse(body, symbolize_names: true))
    end
  end

  def route_case(new_case)
    new_case[:is_med] ? publish(new_case, 'labs') : publish(new_case, 'third_party')
  end

  def publish(new_case, routing_key)
    conn = Bunny.new

    conn.start

    ch = conn.create_channel
    x  = ch.topic('new_cases')

    x.publish(new_case.to_json, routing_key: routing_key)

    puts "Published #{new_case[:case_id]}"

    puts new_case.inspect

    conn.close
  end
end

case_router = CaseRouter.new
case_router.process_new_cases
