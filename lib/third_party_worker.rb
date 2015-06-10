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
      parsed_body[:third_party_send_attempts] += 1
      parsed_body[:history] += {
        worker_name: 'third_party_worker',
        timestamp:   Time.now,
        message:     'Case picked up by third_party_worker.'
      }

      if third_party_present?(parsed_body)
        parsed_body = order_reqs(parsed_body)
        publish(parsed_body)
      elsif parsed_body[:third_party_send_attempts] > 1
        # Send to Error queue
      else
        # Send to Retry labs queue
      end
    end
  end

  def third_party_present?(body)
    if body[:third_party_requirements]
      return true
    else
      return false
    end
  end

  def order_reqs(parsed_body)
    parsed_body[:status]   = 'third_party_reqs_ordered'
    parsed_body[:history] += {
      worker_name: 'third_party_worker',
      timestamp:   Time.now,
      message:     'Third Party requirements ordered.'
    }
    return parsed_body
  end

  def publish(new_case)
    conn = Bunny.new

    conn.start

    ch = conn.create_channel
    x  = ch.topic('new_cases')

    new_case[:history] += {
      worker_name: 'third_party_worker',
      timestamp:   Time.now,
      message:     'Case sent to New Business.'
    }

    x.publish(new_case.to_json, routing_key: 'new_business')

    puts "Published #{new_case[:case_id]}"

    puts new_case.inspect

    conn.close
  end

end

third_party_worker = ThirdPartyWorker.new
third_party_worker.process_new_cases
