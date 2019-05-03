# frozen_string_literal: true

module PgHero
  class BaseDatabase

    include Methods::Basic

    # Subclasses must define connection_model returning and ActiveRecord model
    # for connection management

    def connection
      connection_model.connection
    end
  end
end