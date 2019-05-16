# frozen_string_literal: true

require_relative 'type_const'

module PgHero
  module Methods
    module QueryBlockers

      def supports_query_blocker_monitoring?
        supports_pg_blocking_pids?
      end

      def capture_query_blockers?
        config['capture_query_blockers'] != false
      end

      def capture_query_blockers(save_empty_samples: true)
        return unless capture_query_blockers?

        sample_set = sample_query_blockers
        if !sample_set.sessions.empty? || save_empty_samples
          repository.insert_query_blockers(sample_set)
        end
        sample_set
      end

      def sample_query_blockers
        unless supports_pg_blocking_pids?
          raise NotEnabled, "Query blockers requires Postgres 9.6+ support for pg_blocking_pids. Actual version: #{server_version_num}"
        end

        SampleSet.new(self)
      end

      class SampleSet
        # Do transforms (both normalization and de-normalization)
        # to make the data easier for later analysis here rather
        # than in SQL to minimize the cost on the monitored DB
        # and the complexity of the (already complicated) query
        # used for atomic data collection

        BLOCKER_QUERY_COLUMNS =
          TypeConst.const_column_list(
            pid:                TypeConst::INTEGER,
            user:               TypeConst::TEXT,
            source:             TypeConst::TEXT,
            client_addr:        TypeConst::INET,
            client_hostname:    TypeConst::TEXT,
            client_port:        TypeConst::INTEGER,
            backend_start:      TypeConst::DATETIME,
            xact_start:         TypeConst::DATETIME,
            query_start:        TypeConst::DATETIME,
            state_change:       TypeConst::TEXT,
            wait_event_type:    TypeConst::TEXT,
            wait_event:         TypeConst::TEXT,
            state:              TypeConst::TEXT,
            backend_xid:        TypeConst::XID,
            backend_xmin:       TypeConst::XID,
            query:              TypeConst::TEXT,
            backend_type:       TypeConst::TEXT,
            blocked_by:         TypeConst::INTEGER_ARRAY
          )

        BLOCKER_QUERY_COLUMN_NAMES =
          BLOCKER_QUERY_COLUMNS.map(&:name).freeze

        private_constant :BLOCKER_QUERY_COLUMNS, :BLOCKER_QUERY_COLUMN_NAMES

        BLOCKER_ATTRIBUTE_COLUMNS =
            (BLOCKER_QUERY_COLUMNS + TypeConst.const_column_list(blocking: TypeConst::INTEGER_ARRAY)).freeze


        attr_reader :captured_at, :database, :txid_xmin, :txid_xmax, :txid_xip, :sessions
        attr_accessor :id

        def initialize(database)
          self.id = nil # No real id if not stored in the database
          records = database.select_all(SampleSet.blocker_sample_set_sql(database.server_version_num))
          first_record = records.first # Encodes whether the set has any real blockers

          @captured_at = first_record[:sample_captured_at]
          @database = database.id  # TODO: decided whether or not to keep the native database string first_record[:sample_database]
          @txid_xmin = first_record[:sample_txid_xmin]
          @txid_xmax = first_record[:sample_txid_xmax]
          @txid_xip = first_record[:sample_txid_xip]

          @sessions = first_record[:pid] ? rows_to_sessions(records) : {}
        end

        private

        def rows_to_sessions(result)
          session_cache = {}

          result.map do |row|
            current_pid = row[:pid]
            session = row.slice(*BLOCKER_QUERY_COLUMN_NAMES)

            # Might have already encountered earlier blockees
            session[:blocking] = session_cache[current_pid]&.[](:blocking)
            session_cache[current_pid] = session

            session[:blocked_by]&.each do |blocker_pid|
              add_blockee(current_pid, blocker_pid, session_cache)
            end
            session
          end
        end

        def add_blockee(blockee_pid, blocker_pid, session_cache)
          blocker_session = (session_cache[blocker_pid] ||= {})
          (blocker_session[:blocking] ||= []).push(blockee_pid)
        end

        def self.build_blocker_sample_sql_const(backend_type_col_available)
          # Include inline SQL comments to document nuances of the query
          # here (they execute fine); but they break internal quoting logic
          # (that removes newlines) so strip them out for runtime use
          sql = <<-SQL
            WITH blocked_pids AS (
              -- Pids of all sessions with blockers
              SELECT
                pid blocked_pid,
                pg_blocking_pids(pid) AS blocked_by
              FROM
                pg_stat_activity
              WHERE
                CARDINALITY(pg_blocking_pids(pid)) > 0),
  
            blockers_and_blockees as (
              -- Details of all blockers and blockees; grab almost
              -- everything since catching blockers via sampling
              -- is hit and miss so forensic details are valuable
              SELECT
                psa.pid pid,
                usename,
                application_name,
                client_addr,
                client_hostname,
                client_port,
                backend_start,
                xact_start,
                query_start,
                state_change,
                wait_event_type,
                wait_event,
                state,
                backend_xid,
                backend_xmin,
                query,
                #{backend_type_col_available ? '' : 'null::text '}backend_type,
                bp.blocked_by
              FROM
                pg_stat_activity psa
              LEFT OUTER JOIN -- allows matching blockers as well as blockees
                blocked_pids bp
              ON
                psa.pid = bp.blocked_pid -- normal join matches blockees
              WHERE
                datname = current_database()
                AND (
                  bp.blocked_pid IS NOT NULL -- blockees that already matched JOIN ON
                  OR EXISTS -- adds blockers that are not also blockees
                  (SELECT * FROM blocked_pids bp2 WHERE psa.pid = ANY(bp2.blocked_by))
                )
            ),
  
            sample_set_header as (
              -- Details to record a sample set
              -- even if there were no blockers
              SELECT
                current_database() sample_database,
                NOW() sample_captured_at,
                -- Include txid snapshot details so that txid epoch for backend_xid and backend_xmin
                -- can be inferred; do not compare these directly to backend values without
                -- accounting for epoch adjustment
                txid_snapshot_xmin(txid_current_snapshot()) sample_txid_xmin,
                txid_snapshot_xmax(txid_current_snapshot()) sample_txid_xmax,
                ARRAY(SELECT txid_snapshot_xip(txid_current_snapshot())) sample_txid_xip
            )
  
            -- Sample set always return at least one row
            -- including the timestamp and database
            -- clients should check for the special case
            -- of one row with a null pid meaning there were
            -- no blockers or blockees in the sample set
            SELECT
              header.sample_database,
              header.sample_captured_at,
              header.sample_txid_xmin,
              header.sample_txid_xmax,
              header.sample_txid_xip,
              bab.pid,
              bab.usename::text "user",
              bab.application_name source,
              bab.client_addr,
              bab.client_hostname,
              bab.client_port,
              bab.backend_start,
              bab.xact_start,
              bab.query_start,
              bab.state_change,
              bab.wait_event_type,
              bab.wait_event,
              bab.state,
              bab.backend_xid::text::bigint, -- careful, wraps around, 32-bit unsigned value, no epoch
              bab.backend_xmin::text::bigint, -- careful, wraps around, 32-bit unsigned value, no epoch
              bab.query,
              bab.backend_type,
              bab.blocked_by
            FROM
              sample_set_header header
            LEFT OUTER JOIN
              blockers_and_blockees bab
            ON TRUE
            ORDER BY bab.pid
          SQL
          Basic.sql_const(sql)
        end

        def self.blocker_sample_set_sql(pg_version)
          if (pg_version >= PgConst::VERSION_10)
            # TODO: Bug in pghero.pg_stat_activity view @ Instacart where backend_type column is missing
            #@BLOCKER_SAMPLE_SET_SQL ||= build_blocker_sample_sql_const(true)
            @BLOCKER_SAMPLE_SET_SQL ||= build_blocker_sample_sql_const(false)
          else
            @BLOCKER_SAMPLE_SET_SQL_PRE10 ||= build_blocker_sample_sql_const(false)
          end
        end
      end

      private

      def supports_pg_blocking_pids?
        # pg_blocking_pids introduced in Postgresql 9.6
        # Release Note: https://www.postgresql.org/docs/9.6/release-9-6.html#AEN133618
        # Current Doc:  https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-SESSION-TABLE
        #
        # Previously complex queries on pg_locks were widely used but they are both convoluted and inferior so this
        # feature is based only on pg_blocking_pids given that Postgres 9.6 has been available since Sept. 2016.
        #
        # Technical details on the improvements pg_blocking_pids introduced:
        # https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=52f5d578d6c29bf254e93c69043b817d4047ca67
        server_version_num >= PgConst::VERSION_9_6
      end
    end
  end
  # wait_event_type
  # wait_event

end
