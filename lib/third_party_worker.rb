require 'json'
require 'bunny'

class ThirdPartyWorker

  def process_new_cases
    conn = Bunny.new

    conn.start

    ch = conn.create_channel
    x  = ch.topic('new_cases')
    q  = ch.queue('', exclusive: true)

    q.bind(x, routing_key: 'labs_processed')
    q.bind(x, routing_key: 'third_party')

    q.subscribe(block: true) do |delivery_info, properties, body|
      puts " [x] #{delivery_info.routing_key}:#{body}"

      parsed_body = JSON.parse(body, symbolize_names: true)
      # Read third party node
      # Order requirements
      # publish
    end
  end

  def publish(new_case)
    conn = Bunny.new

    conn.start

    ch = conn.create_channel
    x  = ch.topic('new_cases')

    x.publish(new_case.to_json, routing_key: 'labs_processed')

    puts "Published #{new_case[:case_id]}"

    puts new_case.inspect

    conn.close
  end

end

third_party_worker = ThirdPartyWorker.new
third_party_worker.process_new_cases
