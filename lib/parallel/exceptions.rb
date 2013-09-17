module Parallel

  class DeadWorker < Exception
  end

  class Break < Exception
  end

  class ExceptionWrapper
    attr_reader :exception
    def initialize(exception)
      dumpable = Marshal.dump(exception) rescue nil
      unless dumpable
        exception = RuntimeError.new("Undumpable Exception -- #{exception.inspect}")
      end

      @exception = exception
    end
  end

  class << self

    def handle_exception(exception, results)
      return nil if exception.class == Parallel::Break
      raise exception if exception
      results
    end

  end

end
