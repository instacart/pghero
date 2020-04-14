module PgHero
  module Methods
    module Connections
      def connections
        if server_version_num >= 90500
          select_all <<-SQL
            SELECT
              pg_stat_activity.pid,
              datname AS database,
              usename AS user,
              application_name AS source,
              client_addr AS ip,
              state,
              ssl
            FROM
              pg_stat_activity
            LEFT JOIN
              pg_stat_ssl ON pg_stat_activity.pid = pg_stat_ssl.pid
            ORDER BY
              pg_stat_activity.pid
          SQL
        else
          select_all <<-SQL
            SELECT
              pid,
              datname AS database,
              usename AS user,
              application_name AS source,
              client_addr AS ip,
              state
            FROM
              pg_stat_activity
            ORDER BY
              pid
          SQL
        end
      end

      def total_connections
        select_one("SELECT COUNT(*) FROM pg_stat_activity")
      end

      def connection_states
        states = select_all <<-SQL
          SELECT
            state,
            COUNT(*) AS connections
          FROM
            pg_stat_activity
          GROUP BY
            1
          ORDER BY
            2 DESC, 1
        SQL

        Hash[states.map { |s| [s[:state], s[:connections]] }]
      end

      def connection_sources
        select_all <<-SQL
          SELECT
            datname AS database,
            usename AS user,
            application_name AS source,
            client_addr AS ip,
            COUNT(*) AS total_connections
          FROM
            pg_stat_activity
          GROUP BY
            1, 2, 3, 4
          ORDER BY
            5 DESC, 1, 2, 3, 4
        SQL
      end

      def connection_sources_by_user
        select_all <<-SQL
          SELECT
            datname AS database,
            application_name AS user,
            COUNT(*) AS total_connections
          FROM
            pg_stat_activity
          GROUP BY
            1, 2
          ORDER BY
            3 DESC, 1, 2
        SQL
      end

      def recently_connected_users
        users = select_all_stats <<-SQL
          SELECT distinct username
          FROM "pghero_connection_stats" 
          WHERE database = #{quote(id)}
          AND captured_at > date_trunc('day', NOW() - interval '2 hours')
          ORDER by username
        SQL
      end

      def connection_history_for_user(username)
        history = select_all_stats <<-SQL
          SELECT date_trunc('minute', captured_at) as the_date, max(total_connections) as tot 
          FROM "pghero_connection_stats" 
          WHERE database= #{quote(id)}
          and captured_at > date_trunc('minute', NOW() - interval '2 hours') and username = '#{username}'
          GROUP by username, date_trunc('minute', captured_at) 
          ORDER by date_trunc('minute', captured_at)
        SQL
        this_history = Hash[history.map{|h| [h[:the_date].strftime("%a %l:%M %P"), h[:tot]]}] 
      end

      def capture_connection_stats
        now = Time.now
        columns = %w(database total_connections username captured_at)
        values = []
        connection_sources_by_user.each do |rs|
          values << [id, rs[:total_connections].to_i,rs[:user], now]
        end
        insert_stats("pghero_connection_stats", columns, values) if values.any?
      end
  
      def connection_stats_enabled?
        table_exists?("pghero_connection_stats")
      end

    end
  end
end
