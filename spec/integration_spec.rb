class DummyActiveJobSubscriber < ActiveJob::Base
  include Wisper::ActiveJob::Listener
  queue_as :fast

  def self.it_happened
    # noop
  end
end

RSpec.describe 'integration tests:' do
  let(:publisher) do
    Class.new do
      include Wisper::Publisher

      def run
        broadcast(:it_happened, 'hello, world', hello: 'world')
      end
    end.new
  end

  let(:subscriber) do
    Class.new do
      def self.it_happened
        # noop
      end
    end
  end

  let(:adapter) { ActiveJob::Base.queue_adapter }

  before do
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
  end

  context 'when the subscriber is a plain old ruby class' do
    it 'enqueues Wrapper ActiveJob with nil, event name and args array' do
      publisher.subscribe(subscriber, async: Wisper::ActiveJobBroadcaster.new)

      publisher.run

      expect(adapter.enqueued_jobs.size).to eq 1
      job = adapter.enqueued_jobs.first
      expect(job['job_class']).to eq 'Wisper::ActiveJobBroadcaster::Wrapper'
      expect(job['arguments']).to eq [nil, 'it_happened', 'hello, world', {"_aj_symbol_keys"=>["hello"], "hello"=>"world"}]
    end
  end

  context 'when the subscriber extends ActiveJob' do
    it 'enqueues subscriber ActiveJob with event name and args array' do
      publisher.subscribe(DummyActiveJobSubscriber, async: Wisper::ActiveJobBroadcaster.new)

      publisher.run

      expect(adapter.enqueued_jobs.size).to eq 1
      job = adapter.enqueued_jobs.first
      expect(job[:queue]).to eq 'fast'
      expect(job['job_class']).to eq 'DummyActiveJobSubscriber'
      expect(job['arguments']).to eq ['it_happened', 'hello, world', {"_aj_symbol_keys"=>["hello"], "hello"=>"world"}]
    end
  end
end
