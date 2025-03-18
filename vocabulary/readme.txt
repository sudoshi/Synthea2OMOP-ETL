CPT4 utility for CDM v5.

This utility will import the CPT4 vocabulary into concept.csv.
Internet connection is required.

Start import process from command line with:
 windows: cpt.bat APIKEY
 linux: ./cpt.sh APIKEY

Use API KEY from UMLS account profile: https://uts.nlm.nih.gov//uts.html#profile
Do not close or shutdown your PC until the end of import process,
it will cause damage to concept.csv file.

Please make sure java allowed to make http requests and has write permission to the concept.csv file.

Note (version 1.14 and higher): when working with a delta bundle, please get familiar with the instructions from the README.md file
