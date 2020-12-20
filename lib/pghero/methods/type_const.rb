# frozen_string_literal: true

require 'active_record'
require 'active_record/connection_adapters/postgresql_adapter'

module PgHero
  module Methods
    module TypeConst

      def self.lookup_type(*args, **kwargs)
        ActiveRecord::Type.lookup(*args, adapter: :postgresql, **kwargs)
      end

      BIGINT = lookup_type(:integer, limit: 8)
      BIGINT_ARRAY = lookup_type(:integer, limit: 8, array: true) # PG specific
      BOOLEAN = lookup_type(:boolean)
      DATE = lookup_type(:date)
      DATETIME = lookup_type(:datetime)
      DECIMAL = lookup_type(:decimal)
      FLOAT = lookup_type(:float)
      INET = lookup_type(:inet) # PG specific
      INTEGER = lookup_type(:integer)
      INTEGER_ARRAY = lookup_type(:integer, array: true) # PG specific
      STRING = lookup_type(:string)
      TEXT = lookup_type(:text)
      TIME = lookup_type(:time)

      # XID is a 32-bit unsigned value that eventually wraps;
      # store in BIGINT to avoid negatives. Not the same as a
      # BIGINT txid value that is actually 64-bits with an
      # epoch to avoid wraparound
      XID = BIGINT

      def self.define_column(name, type)
        TypedColumn.new(name, type)
      end

      def self.const_column_list(**kwargs)
        kwargs.each_pair.map do |col_name, col_type|
          define_column(col_name, col_type)
        end.freeze
      end

      class TypedColumn
        attr_reader :name

        def initialize(name, type)
          @name = name.to_sym
          @type = type
          freeze
        end

        def quote_name(connection)
          connection.quote_table_name(@name)
        end

        def quote_value(connection, value)
          connection.quote(@type.serialize(value))
        end
      end

      private_class_method :lookup_type
    end
  end
end

