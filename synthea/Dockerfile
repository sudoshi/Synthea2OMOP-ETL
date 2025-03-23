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

# Create a custom synthea.properties file
RUN echo 'exporter.baseDirectory = ./output/\n\
exporter.csv.export = true\n\
exporter.fhir.export = false\n\
exporter.fhir_stu3.export = false\n\
exporter.fhir_dstu2.export = false\n\
exporter.ccda.export = false\n\
exporter.text.export = false\n\
exporter.json.export = false\n\
exporter.csv.folder_per_run = false\n\
exporter.csv.append_mode = false\n\
exporter.use_uuid_filenames = false\n\
exporter.subfolders_by_id_substring = false\n\
generate.append_numbers_to_person_names = false\n\
' > /synthea/src/main/resources/synthea.properties

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
# Run Synthea directly using java command with explicit CSV export\n\
echo "Running Synthea..."\n\
java -jar build/libs/synthea-with-dependencies.jar \\\n\
  -p ${POPULATION:-10} \\\n\
  -s ${SEED:-1} \\\n\
  --exporter.csv.export=true \\\n\
  --exporter.fhir.export=false \\\n\
  --exporter.fhir_stu3.export=false \\\n\
  --exporter.fhir_dstu2.export=false \\\n\
  --exporter.ccda.export=false \\\n\
  --exporter.text.export=false \\\n\
  --exporter.json.export=false \\\n\
  --generate.append_numbers_to_person_names=false \\\n\
  ${STATE:-"Pennsylvania"} \\\n\
  ${CITY}\n\
\n\
# Check if files were generated\n\
echo "Checking for generated files..."\n\
ls -la /synthea/output/\n\
\n\
# Move all CSV files to shared volume\n\
echo "Moving CSV files to shared volume..."\n\
mkdir -p /synthea-output\n\
find /synthea/output -name "*.csv" -exec mv {} /synthea-output/ \\;\n\
\n\
# Clean up the output directory\n\
echo "Cleaning up the output directory..."\n\
rm -rf /synthea/output/*\n\
\n\
# List the moved files\n\
echo "Files moved to /synthea-output:"\n\
ls -la /synthea-output/\n\
\n\
# Create a marker file to indicate completion\n\
touch /synthea-output/.complete\n\
\n\
echo "Data generation complete. Files available in /synthea-output directory."\n\
' > /synthea/entrypoint.sh

# Make entrypoint script executable
RUN chmod +x /synthea/entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/synthea/entrypoint.sh"]
