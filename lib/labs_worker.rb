require 'json'
require 'bunny'

class LabsWorker

  def process_new_cases
    conn = Bunny.new

    conn.start

    ch = conn.create_channel
    x  = ch.topic('new_cases')
    q  = ch.queue('', exclusive: true)

    q.bind(x, routing_key: 'labs')

    q.subscribe(block: true) do |delivery_info, properties, body|
      puts " [x] #{delivery_info.routing_key}:#{body}"

      parsed_body = JSON.parse(body, symbolize_names: true)

      parsed_body[:history] += {
          worker_name: 'labs_worker',
          timestamp:   Time.now,
          message:     'Case picked up by labs_worker.'
      }

      processed_body = get_med_info(parsed_body)

      publish(processed_body)
    end
  end

  def get_med_info(body)
    sleep 5
    body[:status]   = 'labs_processed'
    body[:history] += {
        worker_name: 'labs_worker',
        timestamp:   Time.now,
        message:     'Labs ordered.'
    }
    return body
  end

  def publish(new_case)
    conn = Bunny.new

    conn.start

    ch = conn.create_channel
    x  = ch.topic('new_cases')

    new_case[:history] += {
        worker_name: 'labs_worker',
        timestamp:   Time.now,
        message:     'Case sent to third_party_worker.'
    }

    x.publish(new_case.to_json, routing_key: 'labs_processed')

    puts "Published #{new_case[:case_id]}"

    puts new_case.inspect

    conn.close
  end

end

labs_worker = LabsWorker.new
labs_worker.process_new_cases
