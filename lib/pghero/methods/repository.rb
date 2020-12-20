# frozen_string_literal: true

require 'active_record'

module PgHero
  module Methods
    module Repository

      def insert(table, column_names, values_table, return_cols: nil)
        quoted_column_list = quote_column_names(column_names)

        row_sql_list = values_table.map do |row_values|
          "(#{quote_row_values(row_values).join(',')})"
        end

        quoted_return_list = return_cols ? quote_column_names(return_cols) : nil
        _insert(table, quoted_column_list, row_sql_list, quoted_return_list)
      end

      def insert_typed(table, typed_columns, values_table, typed_return_cols: nil)
        quoted_column_list = quote_typed_column_names(typed_columns)

        row_sql_list = values_table.map do |row_values|
          "(#{quote_typed_row_values(typed_columns, row_values).join(',')})"
        end

        quoted_return_list = typed_return_cols ? quote_typed_column_names(typed_return_cols) : nil
        _insert(table, quoted_column_list, row_sql_list, quoted_return_list)
      end

      private

      def _insert(table, quoted_column_list, rows_sql_list, quoted_return_list)
        column_sql = quoted_column_list.join(',')
        values_sql = rows_sql_list.join(',')

        insert_sql = <<-SQL
          INSERT INTO #{quote_table_name(table)}
            (#{column_sql})
          VALUES
            #{values_sql}
        SQL

        if quoted_return_list
          insert_sql += <<-SQL if quoted_return_list
          RETURNING
            #{quoted_return_list.join(',')}
          SQL
        end
        connection.execute(insert_sql)
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
    end
  end
end
