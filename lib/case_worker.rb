require 'json'
require 'bunny'

class CaseWorker

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

  def publish(new_case)
    puts "Published #{new_case[:case_id]}"
  end

end

case_worker = CaseWorker.new
10.times do
  new_case = case_worker.generate_case
  case_worker.publish(new_case)
  sleep 10
end
