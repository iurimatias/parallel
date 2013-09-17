module Parallel

  class << self
    def processor_count
      @processor_count ||= case RbConfig::CONFIG['host_os']
      when /darwin9/
        `hwprefs cpu_count`.to_i
      when /darwin/
        (hwprefs_available? ? `hwprefs thread_count` : `sysctl -n hw.ncpu`).to_i
      when /linux|cygwin/
        `grep -c ^processor /proc/cpuinfo`.to_i
      when /(net|open|free)bsd/
        `sysctl -n hw.ncpu`.to_i
      when /mswin|mingw/
        require 'win32ole'
        wmi = WIN32OLE.connect("winmgmts://")
        cpu = wmi.ExecQuery("select NumberOfLogicalProcessors from Win32_Processor")
        cpu.to_enum.first.NumberOfLogicalProcessors
      when /solaris2/
        `psrinfo -p`.to_i # this is physical cpus afaik
      else
        $stderr.puts "Unknown architecture ( #{RbConfig::CONFIG["host_os"]} ) assuming one processor."
        1
      end
      @processor_count
    end

    def physical_processor_count
      @physical_processor_count ||= begin
        ppc = case RbConfig::CONFIG['host_os']
        when /darwin1/, /freebsd/
          `sysctl -n hw.physicalcpu`.to_i
        when /linux/
          cores_per_physical = `grep cores /proc/cpuinfo`[/\d+/].to_i
          physicals = `grep 'physical id' /proc/cpuinfo |sort|uniq|wc -l`.to_i
          physicals * cores_per_physical
        when /mswin|mingw/
          require 'win32ole'
          wmi = WIN32OLE.connect("winmgmts://")
          cpu = wmi.ExecQuery("select NumberOfProcessors from Win32_Processor")
          cpu.to_enum.first.NumberOfProcessors
        else
          processor_count
        end
        # fall back to logical count if physical info is invalid
        ppc > 0 ? ppc : processor_count
      end
    end

    def hwprefs_available?
      `which hwprefs` != ''
    end

  end

end
