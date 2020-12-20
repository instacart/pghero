# frozen_string_literal: true

require_relative 'type_const'

module PgHero
  module Methods
    module QueryBlockersHistory

      BLOCKER_SAMPLE_TABLE = 'pghero_blocker_samples'

      BLOCKER_SAMPLE_SESSION_TABLE = 'pghero_blocker_sample_sessions'

      ID_COL_LIST = TypeConst.const_column_list(id: TypeConst::BIGINT)

      INSERT_BLOCKER_SAMPLE_COLS =
        TypeConst.const_column_list(
          database: TypeConst::TEXT,
          captured_at: TypeConst::DATETIME,
          txid_xmin: TypeConst::BIGINT,
          txid_xmax: TypeConst::BIGINT,
          txid_xip: TypeConst::BIGINT_ARRAY
        )

      INSERT_BLOCKER_SAMPLE_SESSION_COLS = (
            TypeConst.const_column_list(blocker_sample_id: TypeConst::BIGINT) +
            QueryBlockers::SampleSet::BLOCKER_ATTRIBUTE_COLUMNS
          ).freeze

      private_constant :BLOCKER_SAMPLE_TABLE,
                       :BLOCKER_SAMPLE_SESSION_TABLE,
                       :ID_COL_LIST,
                       :INSERT_BLOCKER_SAMPLE_COLS,
                       :INSERT_BLOCKER_SAMPLE_SESSION_COLS

      def supports_query_blocker_history?(raise_on_unsupported: false)
        return @blockers_tables_usable if @blockers_tables_usable

        missing_tables = self.missing_tables(BLOCKER_SAMPLE_TABLE, BLOCKER_SAMPLE_SESSION_TABLE)
        @blocker_tables_usable = missing_tables.empty?

        if !@blocker_tables_usable && raise_on_unsupported
          raise NotEnabled, "Missing table(s): #{missing_tables.join(', ')} are required to track blocker history"
        end

        @blocker_tables_usable
      end

      def insert_query_blockers(sample_set)
        return unless supports_query_blocker_history?(raise_on_unsupported: true)

        with_transaction do # Might already be in a transaction; that's fine
          sample_set.id = insert_query_blocker_sample(sample_set)
          unless sample_set.sessions.empty?
            # Maximum 1K records at a time to keep the SQL INSERT string "reasonable"
            sample_set.sessions.each_slice(1000) do |session_batch|
              insert_session_batch(sample_set.id, session_batch)
            end
          end
        end
        sample_set.id
      end

      # TODO: add support for querying historical data

      private

      def insert_query_blocker_sample(sample_set)
        result = insert_typed(BLOCKER_SAMPLE_TABLE,
                        INSERT_BLOCKER_SAMPLE_COLS,
                        [sample_values(sample_set)],
                        typed_return_cols: ID_COL_LIST)
        result.first[:id.to_s]
      end

      def insert_session_batch(sample_id, session_batch)
        result = insert_typed(BLOCKER_SAMPLE_SESSION_TABLE,
                        INSERT_BLOCKER_SAMPLE_SESSION_COLS,
                        session_values(sample_id, session_batch),
                        typed_return_cols: ID_COL_LIST)
        session_batch.each.with_index do |session, idx|
          session[:id] = result[idx][:id.to_s]
        end
      end

      def sample_values(sample_set)
        INSERT_BLOCKER_SAMPLE_COLS.map { |col| sample_set.send(col.name) }
      end

      def session_values(sample_id, session_batch)
        session_batch.map do |session|
          INSERT_BLOCKER_SAMPLE_SESSION_COLS.map.with_index do |col, idx|
            col.name == :blocker_sample_id ? sample_id : session[col.name]
          end
        end
      end
    end
  end
end
