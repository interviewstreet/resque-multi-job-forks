$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

$TESTING = true

require 'rubygems'
require 'bundler'
Bundler.setup
Bundler.require
require 'test/unit'
require 'resque-multi-job-forks'
require 'timeout'

# setup redis & resque.
redis = Redis.new(:db => 1)
Resque.redis = redis

# adds simple STDOUT logging to test workers.
# set `VERBOSE=true` when running the tests to view resques log output.
module Resque
  class Worker
    def log(msg)
      puts "*** #{msg}" unless ENV['VERBOSE'].nil?
    end
    alias_method :log!, :log

    # Processes a given job in the child.
    def perform_without_multi_job_forks(job)
      begin
        # 'will_fork?' returns false in test mode, but since we need to test
        # if the :after_fork hook runs, we ignore 'will_fork?' here.
        run_hook :after_fork, job # if will_fork?
        job.perform
      rescue Object => e
        log "#{job.inspect} failed: #{e.inspect}"
        begin
          job.fail(e)
        rescue Object => e
          log "Received exception when reporting failure: #{e.inspect}"
        end
        failed!
      else
        log "done: #{job.inspect}"
      ensure
        yield job if block_given?
      end
    end
  end
end

# stores a record of the job processing sequence.
# you may wish to reset this in the test `setup` method.

class SequenceStore
  def initialize
    @redis_key = "sequence_store_#{srand}".to_sym
    clear!
  end

  def clear!
    Resque.redis.del(@redis_key)
  end

  def push(entry)
    Resque.redis.rpush(@redis_key, entry)
  end

  def all to_sym = true
    entries = Resque.redis.lrange(@redis_key, 0, -1)
    entries = entries.map(&:to_sym) if to_sym
    entries
  end

  def [](key)
    entry = Resque.redis.lrange(@redis_key, key.to_i, key.to_i).first
    entry.nil? ? nil : entry.to_sym
  end
end

$SEQUENCE_STORE = SequenceStore.new

# test job, tracks sequence.
class SequenceJob
  @queue = :jobs
  def self.perform(i)
    $SEQUENCE_STORE.push "work_#{i}".to_sym
    sleep(2)
  end
end

class QuickSequenceJob
  @queue = :jobs
  def self.perform(i)
    $SEQUENCE_STORE.push "work_#{i}".to_sym
  end
end

# test hooks, tracks sequence.
Resque.after_fork do
  $SEQUENCE_STORE.push :after_fork
end

Resque.before_fork do
  $SEQUENCE_STORE.push :before_fork
end

Resque.before_child_exit do |worker|
  $SEQUENCE_STORE.push "before_child_exit_#{worker.jobs_processed}".to_sym
end
