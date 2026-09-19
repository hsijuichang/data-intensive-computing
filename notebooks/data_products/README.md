# Week 2 analytical data products

Run these notebooks from the project root after the Week 1 integration notebook has
created `data/processed/integrated_taxi_trips`. The notebooks can run independently
and in any order. They use Spark SQL and save Delta tables under
`data/processed/analytical_products/`.

| Notebook | Row grain | Output Delta table |
| --- | --- | --- |
| `zone_hourly_demand_profile.ipynb` | pickup zone × pickup hour | `zone_hourly_demand_profile` |
| `airport_transfer_summary.ipynb` | month × airport × counterpart zone × direction | `airport_transfer_summary` |
| `weather_sensitive_zone_demand.ipynb` | month × pickup zone × weather category | `weather_sensitive_zone_demand` |
| `origin_destination_flow.ipynb` | month × ordered pickup/dropoff zone pair | `origin_destination_flow` |
| `fare_and_tip_patterns.ipynb` | month × pickup borough × payment-type code | `fare_and_tip_patterns` |
| `trip_distance_distribution.ipynb` | month × pickup borough × distance band | `trip_distance_distribution` |

Each run overwrites its own output with a fresh snapshot. Every table carries
`source_table`, `created_at`, `refreshed_at`, and `schema_version`. `created_at`
is retained across refreshes; `refreshed_at` changes on each successful run.
Zone-hour demand and origin–destination flow are partitioned by pickup year and
month, allowing queries that filter those columns to prune partitions.

The airport product uses taxi-zone IDs 1 (Newark), 132 (JFK), and 138
(LaGuardia). Airport-to-airport trips are counted once in their own direction
category. The weather product uses the same weather-code categories as
`analytical_queries.ipynb`; it includes zero-trip zone-hours within the
observed citywide weather hours. It excludes hours without a matched, classified
weather observation. None of these products applies a date filter, so unusual
source timestamps will appear until an explicit analysis period is chosen.

The fare/tip product reports recorded tips only. Cash tips may be missing from
the source; payment types remain numeric source codes rather than inferred
labels. The distance product excludes null/negative distances, keeps zero
distance as its own band, and calculates band shares within each borough-month.

To check an output after running, read its Delta path and compare the sum of
`trip_count` with an equivalent count from `integrated_taxi_trips` at the
product's scope. For the airport product, compare only trips touching one of
the configured airport IDs. For the weather product, compare only trips in
classified, matched weather hours with a known pickup zone ID.
