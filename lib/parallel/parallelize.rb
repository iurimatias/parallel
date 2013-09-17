module Parallel

  class << self

    def in_threads(options={:count => 2})
      count, options = extract_count_from_options(options)

      out = []
      threads = []

      count.times do |i|
        threads[i] = Thread.new do
          out[i] = yield(i)
        end
      end

      kill_on_ctrl_c(threads) { wait_for_threads(threads) }

      out
    end

    def in_processes(options = {}, &block)
      count, options = extract_count_from_options(options)
      count ||= processor_count
      map(0...count, options.merge(:in_processes => count), &block)
    end

    private

    def work_direct(array, options)
      results = []
      array.each_with_index do |e,i|
        results << (options[:with_index] ? yield(e,i) : yield(e))
      end
      results
    end

    def work_in_threads(items, options, &block)
      results = []
      current = -1
      exception = nil

      in_threads(options[:count]) do
        # as long as there are more items, work on one of them
        loop do
          break if exception

          index = Thread.exclusive{ current+=1 }
          break if index >= items.size

          with_instrumentation items[index], index, options do
            begin
              results[index] = call_with_index(items, index, options, &block)
            rescue Exception => e
              exception = e
              break
            end
          end
        end
      end

      handle_exception(exception, results)
    end

    def work_in_processes(items, options, &blk)
      workers = create_workers(items, options, &blk)
      current_index = -1
      results = []
      exception = nil
      kill_on_ctrl_c(workers.map(&:pid)) do
        in_threads(options[:count]) do |i|
          worker = workers[i]

          begin
            loop do
              break if exception
              index = Thread.exclusive{ current_index += 1 }
              break if index >= items.size

              output = with_instrumentation items[index], index, options do
                worker.work(index)
              end

              if ExceptionWrapper === output
                exception = output.exception
              else
                results[index] = output
              end
            end
          ensure
            worker.close_pipes
            worker.wait # if it goes zombie, rather wait here to be able to debug
          end
        end
      end

      handle_exception(exception, results)
    end

    def wait_for_threads(threads)
      threads.compact.each do |t|
        begin
          t.join
        rescue Interrupt
          # thread died, do not stop other threads
        end
      end
    end

    # options is either a Integer or a Hash with :count
    def extract_count_from_options(options)
      if options.is_a?(Hash)
        count = options[:count]
      else
        count = options
        options = {}
      end
      [count, options]
    end

    # kill all these pids or threads if user presses Ctrl+c
    def kill_on_ctrl_c(things)
      if @to_be_killed
        @to_be_killed << things
      else
        @to_be_killed = [things]
        Signal.trap :SIGINT do
          if @to_be_killed.any?
            $stderr.puts 'Parallel execution interrupted, exiting ...'
            @to_be_killed.flatten.compact.each { |thing| kill_that_thing!(thing) }
          end
          exit 1 # Quit with 'failed' signal
        end
      end
      yield
    ensure
      @to_be_killed.pop # free threads for GC and do not kill pids that could be used for new processes
    end

    def kill_that_thing!(thing)
      if thing.is_a?(Thread)
        thing.kill
      else
        begin
          Process.kill(:KILL, thing)
        rescue Errno::ESRCH
          # some linux systems already automatically killed the children at this point
          # so we just ignore them not being there
        end
      end
    end

    def call_with_index(array, index, options, &block)
      args = [array[index]]
      args << index if options[:with_index]
      if options[:preserve_results] == false
        block.call(*args)
        nil # avoid GC overhead of passing large results around
      else
        block.call(*args)
      end
    end

    def with_instrumentation(item, index, options)
      on_start = options[:start]
      on_finish = options[:finish]
      on_start.call(item, index) if on_start
      yield
    ensure
      on_finish.call(item, index) if on_finish
    end
  end

end
