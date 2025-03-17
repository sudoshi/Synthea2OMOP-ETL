FROM openjdk:17-slim

# Install git and build tools
RUN apt-get update && apt-get install -y \
    git \
    curl \
    unzip \
    libfreetype6 \
    fontconfig \
    libfontconfig1 \
    fonts-dejavu \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /synthea

# Clone Synthea repository
RUN git clone https://github.com/synthetichealth/synthea.git .

# Build Synthea
RUN ./gradlew build

# Create output directory
RUN mkdir -p /synthea/output

# Create entrypoint script
RUN echo '#!/bin/bash\n\
echo "Starting Synthea with parameters:"\n\
echo "Population: $POPULATION"\n\
echo "Seed: $SEED"\n\
echo "State: $STATE"\n\
echo "City: $CITY"\n\
echo "Gender: $GENDER"\n\
echo "Age: $AGE"\n\
echo "Module: $MODULE"\n\
\n\
# Build command line arguments\n\
ARGS="-p $POPULATION"\n\
\n\
if [ ! -z "$SEED" ]; then\n\
  ARGS="$ARGS -s $SEED"\n\
fi\n\
\n\
if [ ! -z "$GENDER" ]; then\n\
  ARGS="$ARGS -g $GENDER"\n\
fi\n\
\n\
if [ ! -z "$AGE" ]; then\n\
  ARGS="$ARGS -a $AGE"\n\
fi\n\
\n\
if [ ! -z "$MODULE" ]; then\n\
  ARGS="$ARGS -m $MODULE"\n\
fi\n\
\n\
# Add state and city as positional arguments\n\
if [ ! -z "$STATE" ]; then\n\
  ARGS="$ARGS $STATE"\n\
  \n\
  if [ ! -z "$CITY" ]; then\n\
    # City parameter is commented out to avoid demographics errors\n\
    # ARGS="$ARGS \"$CITY\""\n\
    echo "Note: City parameter '$CITY' will be ignored as Synthea may not have demographic data for it in $STATE"\n\
  fi\n\
fi\n\
\n\
# Run Synthea\n\
echo "Running: ./run_synthea $ARGS"\n\
./run_synthea $ARGS\n\
\n\
# Copy output to shared volume\n\
echo "Copying output to shared volume..."\n\
echo "Checking for output files..."\n\
find /synthea -name "*.csv" | sort\n\
\n\
echo "Copying output to shared volume..."\n\
mkdir -p /output\n\
find /synthea -name "*.csv" -exec cp {} /output/ \\;\n\
\n\
# Create a marker file to indicate completion\n\
touch /output/.complete\n\
\n\
echo "Data generation complete. Files available in /output directory."\n\
' > /synthea/entrypoint.sh

# Make entrypoint script executable
RUN chmod +x /synthea/entrypoint.sh

# Create volume mount point
VOLUME ["/output"]

# Set entrypoint
ENTRYPOINT ["/synthea/entrypoint.sh"]
