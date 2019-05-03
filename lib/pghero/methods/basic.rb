# frozen_string_literal: true

require 'active_record'

module PgHero
  module Methods
    module Basic

      PG_CONNECTION_ADAPTER_NAMES = %i[postgresql postgis].freeze

      ACTIVE_RECORD_CAST_METHOD = ActiveRecord::VERSION::MAJOR < 5 ? :type_cast : :cast

      private_constant :PG_CONNECTION_ADAPTER_NAMES, :ACTIVE_RECORD_CAST_METHOD

      # from ActiveSupport
      def self.squish(str)
        str.to_s.gsub(/\A[[:space:]]+/, '').gsub(/[[:space:]]+\z/, '').gsub(/[[:space:]]+/, ' ')
      end

      def self.remove_line_comments(sql)
        sql.gsub(/[\s]*--[^\r\n]*/, '')
      end

      def self.sql_const(sql)
        make_squishable(sql, squish(remove_line_comments(sql)))
      end

      def self.make_squishable(obj, squished, freeze = true)
        squished = squished.freeze if freeze
        obj = obj.frozen? ? obj.dup : obj
        obj.define_singleton_method(:squish, -> { squished })
        freeze ? obj.freeze : obj
      end

      def execute(sql)
        connection.execute(sql)
      end

      def select_one(sql)
        select_all(sql).first.values.first
      end

      def quote(value)
        connection.quote(value)
      end

      def ssl_used?
        ssl_used = nil
        with_transaction(rollback: true) do
          begin
            execute('CREATE EXTENSION IF NOT EXISTS sslinfo')
          rescue ActiveRecord::StatementInvalid
            # not superuser
          end
          ssl_used = select_one('SELECT ssl_is_used()')
        end
        ssl_used
      end

      def database_name
        select_one('SELECT current_database()')
      end

      def server_version
        @server_version ||= select_one('SHOW server_version')
      end

      def server_version_num
        @server_version_num ||= select_one('SHOW server_version_num').to_i
      end

      def quote_ident(value)
        quote_table_name(value)
      end

      def select_all(sql)
        # squish for logs
        retries = 0
        begin
          result = connection.select_all(sql.respond_to?(:squish) ? sql.squish : squish(sql))
          result.map do |row|
            Hash[
              row.map do |col, val|
                [col.to_sym, result.column_types[col].send(ACTIVE_RECORD_CAST_METHOD, val)]
              end
            ]
          end
        rescue ActiveRecord::StatementInvalid => e
          # fix for random internal errors
          if e.message.include?('PG::InternalError') && retries < 2
            retries += 1
            sleep(0.1)
            retry
          else
            raise e
          end
        end
      end

      def select_all_size(sql)
        result = select_all(sql)
        result.each do |row|
          row[:size] = PgHero.pretty_size(row[:size_bytes])
        end
        result
      end

      def select_one(sql)
        select_all(sql).first.values.first
      end

      def squish(str)
        Basic.squish(str)
      end

      def quote(value)
        connection.quote(value)
      end

      def quote_table_name(value)
        connection.quote_table_name(value)
      end

      def unquote(part)
        if part && part.start_with?('"')
          part[1..-2]
        else
          part
        end
      end

      def with_transaction(lock_timeout: nil, statement_timeout: nil, rollback: false)
        connection_model.transaction do
          select_all "SET LOCAL statement_timeout = #{statement_timeout.to_i}" if statement_timeout
          select_all "SET LOCAL lock_timeout = #{lock_timeout.to_i}" if lock_timeout
          yield
          raise ActiveRecord::Rollback if rollback
        end
      end

      def table_exists?(table)
        postgres_connection? &&
          select_one(<<-SQL)
            SELECT EXISTS (
              #{table_exists_subquery(quote(table))}
            )
          SQL
      end

      def missing_tables(*tables)
        return tables unless postgres_connection?

        result = select_all(<<-SQL)
            SELECT
              table_name
            FROM
              UNNEST(ARRAY[#{tables.map { |table| "'#{table}'" }.join(', ')}]::varchar[]) table_name
            WHERE NOT EXISTS (
              #{table_exists_subquery('table_name')}
            )
        SQL

        result.map { |row| row[:table_name] }
      end

      def postgres_connection?
        PG_CONNECTION_ADAPTER_NAMES.include?(connection_adapter_name)
      end

      private

      def table_exists_subquery(quoted_relname)
        # quoted_relname must be pre-quoted if it's a literal
        <<-SQL
          SELECT
            1
          FROM
            pg_catalog.pg_class c
          INNER JOIN
            pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          WHERE
            n.nspname = 'public'
            AND c.relname = #{quoted_relname}
                AND c.relkind = 'r'
        SQL
      end

      def quote_column_names(col_names)
        connection = self.connection
        col_names.map do |col_name|
          connection.quote_table_name(col_name)
        end
      end

      def quote_row_values(row_values)
        row_values.map do |value|
          connection.quote(value)
        end
      end

      def quote_typed_column_names(typed_columns)
        connection = self.connection
        typed_columns.map { |col| col.quote_name(connection) }
      end

      def quote_typed_row_values(typed_columns, row_values)
        connection = self.connection
        typed_columns.map.with_index do |typed_col, idx|
          typed_col.quote_value(connection, row_values[idx])
        end
      end

      # From ActiveRecord::Type - compatible with ActiveRecord::Type.lookup
      def connection_adapter_name
        connection.adapter_name.downcase.to_sym
      end
    end
  end
end
