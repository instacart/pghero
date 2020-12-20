# frozen_string_literal: true

require_relative 'test_helper'

class QueryBlockersTest < Minitest::Test

  def test_primary_database_current_blockers
    assert primary_database.sample_query_blockers
  end

  def test_primary_database_capture_blockers_return
    run_with_blockers(rollback: rollback_enabled?) do
      blocker_sample = primary_database.capture_query_blockers
      assert blocker_sample.sessions.size == 2
    end
  end

  def test_capture_blockers
    run_with_blockers(rollback: rollback_enabled?) do
      assert PgHero.capture_query_blockers(verbose: true)
    end
  end

  private

  def run_with_blockers(rollback: true)
    # TODO: ideally we would use a third thread to separate the
    # transaction/connection used for storing historical stat
    User.transaction do
      locked_user = User.all.first
      locked_user.active = !locked_user.active
      locked_user.save

      t = lock_user_in_separate_thread(locked_user.id)
      # Give thread one second to block on the DB lock;
      # test could fail if DB is too slow - increase block time if needed
      t.join(1)
      yield
      raise ActiveRecord::Rollback, 'Test - do not save anything' if rollback
    end
  end

  def lock_user_in_separate_thread(user_id)
    Thread.new do
      # No full Rails dependency so can't use Rails.application.executor.wrap; use older with_connection mechanism
      ActiveRecord::Base.connection_pool.with_connection do
        # Will block on the main thread holding the row lock until the main transaction completes
        User.transaction do
          User.find(user_id).lock!
        end
      end
    end
  end
end
