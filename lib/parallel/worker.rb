module Parallel

  class Worker
    attr_reader :pid, :read, :write
    def initialize(read, write, pid)
      @read, @write, @pid = read, write, pid
    end

    def close_pipes
      read.close
      write.close
    end

    def wait
      Process.wait(pid)
    rescue Interrupt
      # process died
    end

    def work(index)
      begin
        Marshal.dump(index, write)
      rescue Errno::EPIPE
        raise DeadWorker
      end

      begin
        Marshal.load(read)
      rescue EOFError
        raise Parallel::DeadWorker
      end
    end
  end

  class << self

    private

    def create_workers(items, options, &block)
      workers = []
      Array.new(options[:count]).each do
        workers << worker(items, options.merge(:started_workers => workers), &block)
      end
      workers
    end

    def worker(items, options, &block)
      # use less memory on REE
      GC.copy_on_write_friendly = true if GC.respond_to?(:copy_on_write_friendly=)

      child_read, parent_write = IO.pipe
      parent_read, child_write = IO.pipe

      pid = Process.fork do
        begin
          options.delete(:started_workers).each(&:close_pipes)

          parent_write.close
          parent_read.close

          process_incoming_jobs(child_read, child_write, items, options, &block)
        ensure
          child_read.close
          child_write.close
        end
      end

      child_read.close
      child_write.close

      Worker.new(parent_read, parent_write, pid)
    end

    def process_incoming_jobs(read, write, items, options, &block)
      while !read.eof?
        index = Marshal.load(read)
        result = begin
          call_with_index(items, index, options, &block)
        rescue Exception => e
          ExceptionWrapper.new(e)
        end
        Marshal.dump(result, write)
      end
    end

  end

end
