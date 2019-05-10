# frozen_string_literal: true

require_relative 'test_helper'

class CaptureTest < Minitest::Test

  def test_capture_query_blockers
    # TODO: test needs to be in a transaction rollback as soon
    # as capture_query_blockers implements real inserts
    assert PgHero.capture_query_blockers(verbose: true)
  end

end
