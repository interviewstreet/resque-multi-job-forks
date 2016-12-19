require 'resque'
require 'resque/worker'
require 'resque/plugins/multi_job_forks/rss_reader'

module Resque
  class Worker
    include Plugins::MultiJobForks::RssReader
    attr_accessor :seconds_per_fork
    attr_accessor :jobs_per_fork
    attr_accessor :memory_threshold
    attr_reader   :jobs_processed
    attr_accessor :job_check_interval

    def self.multi_jobs_per_fork?
      !ENV["ENABLE_MULTI_JOBS_PER_FORK"].nil?
    end

    if multi_jobs_per_fork? && !method_defined?(:shutdown_without_multi_job_forks)
      def perform_with_multi_job_forks(job = nil, &block)
        perform_without_multi_job_forks(job, &block)
        hijack_fork unless fork_hijacked?
        @jobs_processed += 1
      end
      alias_method :perform_without_multi_job_forks, :perform
      alias_method :perform, :perform_with_multi_job_forks

      def shutdown_with_multi_job_forks?
        release_fork if fork_hijacked? && (fork_job_limit_reached? || @shutdown)
        shutdown_without_multi_job_forks?
      end
      alias_method :shutdown_without_multi_job_forks?, :shutdown?
      alias_method :shutdown?, :shutdown_with_multi_job_forks?

      def shutdown_with_multi_job_forks
        shutdown_without_multi_job_forks
        shutdown_child if is_parent_process?
      end
      alias_method :shutdown_without_multi_job_forks, :shutdown
      alias_method :shutdown, :shutdown_with_multi_job_forks

      def pause_processing_with_multi_job_forks
        pause_processing_without_multi_job_forks
        shutdown_child if is_parent_process?
      end
      alias_method :pause_processing_without_multi_job_forks, :pause_processing
      alias_method :pause_processing, :pause_processing_with_multi_job_forks

      def working_on_with_worker_registration(job)
        register_worker
        working_on_without_worker_registration(job)
      end
      alias_method :working_on_without_worker_registration, :working_on
      alias_method :working_on, :working_on_with_worker_registration

      # Reconnect only once
      def reconnect_with_multi_job_forks
        unless @reconnected
          reconnect_without_multi_job_forks
          @reconnected = true
        end
      end
      alias_method :reconnect_without_multi_job_forks, :reconnect
      alias_method :reconnect, :reconnect_with_multi_job_forks

      def work(interval = 5.0, &block)
        interval = Float(interval)
        @job_check_interval = interval
        startup

        work_loop(interval, &block)

        unregister_worker
      rescue Exception => exception
        return if exception.class == SystemExit && !@child && run_at_exit_hooks
        log_with_severity :error, "Failed to start worker : #{exception.inspect}"
        unregister_worker(exception)
      end

      def work_loop interval = 5.0, &block
        loop do
          break if shutdown?

          unless work_one_job(&block)
            break if interval.zero?
            log_with_severity :debug, "Sleeping for #{interval} seconds"
            procline paused? ? "Paused" : "Waiting for #{queues.join(',')}"
            sleep interval
          end
        end
      end

      private

      def perform_with_fork(job, &block)
        run_hook :before_fork, job

        begin
          @child = fork do
            unregister_signal_handlers if term_child
            perform(job, &block)
            work_loop(job_check_interval, &block)
            exit! unless run_at_exit_hooks
          end
        rescue NotImplementedError
          @fork_per_job = false
          perform(job, &block)
          return
        end

        srand # Reseeding
        procline "Forked #{@child} at #{Time.now.to_i}"

        begin
          Process.waitpid(@child)
        rescue SystemCallError
          nil
        end

        job.fail(DirtyExit.new("Child process received unhandled signal #{$?.stopsig}")) if $?.signaled?
        @child = nil
      end

      public
    end

    # Need to tell the child to shutdown since it might be looping performing multiple jobs per fork
    # The TSTP signal is registered only in forked processes and calls this function
    def shutdown_child
      begin
        Process.kill('TSTP', @child)
      rescue Errno::ESRCH
        nil
      end
    end

    def is_parent_process?
      @child
    end

    def fork_hijacked?
      @release_fork_limit
    end

    def hijack_fork
      log 'hijack fork.'
      @suppressed_fork_hooks = [Resque.after_fork, Resque.before_fork]
      Resque.after_fork = Resque.before_fork = nil
      @release_fork_limit = fork_job_limit
      @jobs_processed = 0
      @cant_fork = true
      @fork_per_job = false
      trap('TSTP') { shutdown }
    end

    def release_fork
      log "jobs processed by child: #{jobs_processed}; rss: #{rss}"
      run_hook :before_child_exit, self
      Resque.after_fork, Resque.before_fork = *@suppressed_fork_hooks
      @release_fork_limit = @jobs_processed = @cant_fork = nil
      remove_instance_variable(:@fork_per_job) if defined?(@fork_per_job)
      log 'hijack over, counter terrorists win.'
      @shutdown = true
    end

    def fork_job_limit
      jobs_per_fork.nil? ? Time.now.to_i + seconds_per_fork : jobs_per_fork
    end

    def fork_job_limit_reached?
      fork_job_limit_remaining <= 0 || fork_job_over_memory_threshold?
    end

    def fork_job_limit_remaining
      jobs_per_fork.nil? ? @release_fork_limit - Time.now.to_i : jobs_per_fork - @jobs_processed
    end

    def seconds_per_fork
      @seconds_per_fork ||= minutes_per_fork * 60
    end

    def minutes_per_fork
      ENV['MINUTES_PER_FORK'].nil? ? 1 : ENV['MINUTES_PER_FORK'].to_i
    end

    def jobs_per_fork
      @jobs_per_fork ||= ENV['JOBS_PER_FORK'].nil? ? nil : ENV['JOBS_PER_FORK'].to_i
    end

    def fork_job_over_memory_threshold?
      !!(memory_threshold && rss > memory_threshold)
    end

    def memory_threshold
      @memory_threshold ||= ENV["RESQUE_MEM_THRESHOLD"].to_i
      @memory_threshold > 0 && @memory_threshold
    end
  end

  # the `before_child_exit` hook will run in the child process
  # right before the child process terminates
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def self.before_child_exit(&block)
    if block
      @before_child_exit ||= []
      @before_child_exit << block
    end
    @before_child_exit
  end

  # Set the before_child_exit proc.
  def self.before_child_exit=(before_child_exit)
    @before_child_exit = before_child_exit.respond_to?(:each) ? before_child_exit : [before_child_exit].compact
  end

end
