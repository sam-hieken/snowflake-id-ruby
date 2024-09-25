require 'monitor'

class IdWorker

      attr_reader :custom_epoch, :node_id, :logger, :last_timestamp, :sequence

      # WORKER_ID_BITS = 5
      # DATACENTER_ID_BITS = 5
      EPOCH_BITS = 41
      NODE_ID_BITS = 10
      SEQUENCE_BITS = 12
      
      MAX_NODE_ID = (1 << NODE_ID_BITS) - 1;
      MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1;

      # note: this is a class-level (global) lock.
      # May want to change to an instance-level lock if this is reworked to some kind of singleton or worker daemon.
      MUTEX_LOCK = Monitor.new

      def initialize(node_id = 0, custom_epoch = 1420070400000)
        # raise "Worker ID set to #{worker_id} which is invalid" if worker_id > MAX_WORKER_ID || worker_id < 0
        # raise "Datacenter ID set to #{datacenter_id} which is invalid" if datacenter_id > MAX_DATACENTER_ID || datacenter_id < 0
        raise "Node ID set to #{node_id} which is invalid" if node_id > MAX_NODE_ID || node_id < 0
        @node_id = node_id
        @custom_epoch = custom_epoch
        @sequence = 0
        # @reporter = reporter || lambda{ |r| puts r }
        @logger = logger || lambda{ |r| puts r }
        @last_timestamp = -1
        # @logger.call("IdWorker starting. timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d" % [TIMESTAMP_LEFT_SHIFT, DATACENTER_ID_BITS, WORKER_ID_BITS, SEQUENCE_BITS, worker_id])
      end

      def get_id(*)
        # log stuff here, theoretically
        next_id
      end
      alias call get_id

      protected

      def next_id
        MUTEX_LOCK.synchronize do
          timestamp = current_time_millis
          if timestamp < @last_timestamp
            @logger.call("clock is moving backwards.  Rejecting requests until %d." % @last_timestamp)
          end
          if timestamp == @last_timestamp
            @sequence = (@sequence + 1) & MAX_SEQUENCE
            if @sequence == 0
              timestamp = till_next_millis(@last_timestamp)
            end
          else
            @sequence = 0
          end
          @last_timestamp = timestamp
          timestamp << (NODE_ID_BITS + SEQUENCE_BITS) |
            (node_id << SEQUENCE_BITS) |
            sequence
        end
      end

      private

      def current_time_millis
        (Time.now.to_f * 1000).to_i - custom_epoch
      end

      def till_next_millis(last_timestamp = @last_timestamp)
        timestamp = nil
        # the scala version didn't have the sleep. Not sure if sleeping releases the mutex lock, more research required
        while (timestamp = current_time_millis) < last_timestamp; sleep 0.0001; end
        timestamp
      end

end
