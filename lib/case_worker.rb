require 'json'
require 'bunny'

class CaseWorker
  attr_accessor :new_case

  def initialize
    @new_case = generate_case
  end

  def publish_new_case
    conn = Bunny.new

    conn.start

    ch   = conn.create_channel
    q    = ch.queue('new_cases')

    ch.default_exchange.publish(new_case.to_json, routing_key: q.name)

    puts "Published #{new_case[:case_id]}\n\n"

    conn.close
  end

  def generate_case
    {
      case_id: generate_id,
      is_med:  get_med,
      status:  'new',
      third_party_requirements: {
        mvr: '',
        mib: ''
      },
      third_party_send_attempts: 0
    }
  end

  def generate_id
    "#{rand}#{rand}"
  end

  def get_med
    [true, false].sample
  end
end

10.times do |counter|
  case_worker = CaseWorker.new
  puts "Counter at: #{counter}."
  case_worker.new_case.delete(:third_party_requirements) if counter == 0
  case_worker.publish_new_case
  sleep 5
end
