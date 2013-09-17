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

end
