# frozen_string_literal: true

namespace :pghero do
  desc 'capture query stats'
  task capture_query_stats: :environment do
    PgHero.capture_query_stats(verbose: true)
  end

  desc 'capture space stats'
  task capture_space_stats: :environment do
    PgHero.capture_space_stats(verbose: true)
  end

  desc 'capture connection stats'
  task capture_connection_stats: :environment do
    PgHero.capture_connection_stats(verbose: true)
  end

  desc 'capture_query_blockers'
  task :capture_query_blockers, [:dbid_filters] => :environment do |t, task_args|
    args = task_args.to_a
    filters = args.empty? ? nil : args.map { |s| Regexp.new(s) }

    PgHero.capture_query_blockers(verbose: true, filters: filters)
  end

  desc "analyze tables"
  task analyze: :environment do
    PgHero.analyze_all(verbose: true, min_size: ENV['MIN_SIZE_GB'].to_f.gigabytes)
  end

  desc 'autoindex'
  task autoindex: :environment do
    PgHero.autoindex_all(verbose: true, create: true)
  end
end
