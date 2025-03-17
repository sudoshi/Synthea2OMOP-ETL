FROM rocker/r-ver:4.1.0

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN R -e "install.packages(c('remotes', 'DatabaseConnector', 'SqlRender', 'jsonlite'), repos = 'https://cloud.r-project.org/')"
RUN R -e "remotes::install_github('OHDSI/Achilles')"

# Create directory for JDBC drivers
RUN mkdir -p /drivers
WORKDIR /app

# Entry point script
ENTRYPOINT ["Rscript", "/app/scripts/run_achilles.R"]
