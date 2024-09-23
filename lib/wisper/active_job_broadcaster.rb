require 'wisper'
require 'wisper/active_job/version'
require 'active_job'

module Wisper
  class ActiveJobBroadcaster
    def broadcast(subscriber, publisher, event, args, options)
      if subscriber < ActiveJob::Listener
        subscriber.perform_later(event, args, options)
      else
        wrapper = subscriber.respond_to?(:queue) ? Wrapper.set(queue: subscriber.queue) : Wrapper
        wrapper.perform_later(subscriber.name, event, args, options)
      end
    end

    class Wrapper < ::ActiveJob::Base
      queue_as :default

      def perform(class_name, event, args, options)
        listener = class_name.constantize
        listener.public_send(event, *args, **options)
      end
    end

    def self.register
      Wisper.configure do |config|
        config.broadcaster :active_job, ActiveJobBroadcaster.new
        config.broadcaster :async,      ActiveJobBroadcaster.new
      end
    end
  end
end

Wisper::ActiveJobBroadcaster.register
