# frozen_string_literal: true

require_relative 'test_helper'

class CaptureTest < Minitest::Test
  def test_primary_database_capture_query_stats
    stats_repository.with_transaction(rollback: rollback_enabled?) do
      assert primary_database.capture_query_stats(raise_errors: true)
    end
  end

  def test_capture_query_stats
    stats_repository.with_transaction(rollback: rollback_enabled?) do
      assert PgHero.capture_query_stats(verbose: true)
    end
  end

  def test_capture_space_stats
    stats_repository.with_transaction(rollback: rollback_enabled?) do
      assert PgHero.capture_space_stats(verbose: true)
    end
  end

  def test_capture_query_stats
    stats_repository.with_transaction(rollback: rollback_enabled?) do
      assert PgHero.capture_query_stats(verbose: true)
    end
  end

  def test_capture_connection_stats
    stats_repository.with_transaction(rollback: rollback_enabled?) do
      assert PgHero.capture_connection_stats(verbose: true)
    end
  end
end
