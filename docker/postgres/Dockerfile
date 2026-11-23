# PostGIS + the H3 extension (Uber's hexagonal grid) for hex-binning bus GPS data inside the warehouse.
# The postgres base image already has the PostgreSQL apt repository (PGDG) configured, which packages h3-pg.
FROM postgis/postgis:17-3.7
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-17-h3 \
 && rm -rf /var/lib/apt/lists/*
