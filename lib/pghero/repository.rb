# frozen_string_literal: true

require 'active_record'

module PgHero
  class Repository < BaseDatabase
    # Repository extends BaseDatabase and not Database (aka MonitoredDatabase)
    # because we keep separate connections for monitoring vs repository access
    # even when monitoring the repository itself. This keeps the logic,
    # transaction isolation, etc completely segregated.

    include Methods::Repository
    include Methods::QueryBlockersHistory

    private

    def connection_model
      QueryStats
    end

    class QueryStats < ActiveRecord::Base
      self.abstract_class = true
      self.table_name = 'pghero_query_stats'
      establish_connection ENV['PGHERO_STATS_DATABASE_URL'] if ENV['PGHERO_STATS_DATABASE_URL']
    end
  end
end
