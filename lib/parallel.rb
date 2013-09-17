require 'thread' # to get Thread.exclusive
require 'rbconfig'
require 'parallel/version'
require 'parallel/exceptions'
require 'parallel/worker'
require 'parallel/processor_info'
require 'parallel/parallelize'

module Parallel

  class << self

    def each(array, options={}, &block)
      map(array, options.merge(:preserve_results => false), &block)
      array
    end

    def each_with_index(array, options={}, &block)
      each(array, options.merge(:with_index => true), &block)
    end

    def map(array, options = {}, &block)
      array = array.to_a # turn Range and other Enumerable-s into an Array

      if options[:in_threads]
        method = :in_threads
        size = options[method]
      else
        method = :in_processes
        size = options[method] || processor_count
      end
      size = [array.size, size].min

      return work_direct(array, options, &block) if size == 0

      if method == :in_threads
        work_in_threads(array, options.merge(:count => size), &block)
      else
        work_in_processes(array, options.merge(:count => size), &block)
      end
    end

    def map_with_index(array, options={}, &block)
      map(array, options.merge(:with_index => true), &block)
    end

  end

end
