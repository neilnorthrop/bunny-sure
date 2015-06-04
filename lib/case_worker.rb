require 'json'
require 'bunny'

class CaseWorker

  def publish_new_case
    conn     = Bunny.new

    conn.start

    ch       = conn.create_channel
    q        = ch.queue('new_cases')
    new_case = generate_case

    ch.default_exchange.publish(new_case.to_json, routing_key: q.name)

    puts "Published #{new_case[:case_id]}"

    conn.close
  end

  def generate_case
    {
      case_id: generate_id,
      is_med:  get_med,
      status:  'new'
    }
  end

  def generate_id
    "#{rand}#{rand}"
  end

  def get_med
    [true, false].sample
  end
end

case_worker = CaseWorker.new
10.times do
  case_worker.publish_new_case
  sleep 10
end
