# Exmaple: Merging and denormalization the international and Norwegian release packages

# Setup and running the script
Requirements:
- `uv`(https://docs.astral.sh/uv/ )
- International and Norwegian release package in `input` folder

Install packages and script with:
- `uv sync`

Activate `.venv` and run installed script with:
- `snomedct --help`

For testing purposes, it can be convenient to restrict the output:
- `snomedct --limit=1`
- `snomedct --id-filter=10000006`

# SNOMED CT release packages
SNOMED release packages are available under https://mlds.ihtsdotools.org/ Each release of the "Norwegian National Extension" depends on a specific version of the "SNOMED CT International Edition".

For example:
- "Norwegian National Extension June 2025 v1.0" depends on "SNOMED CT April 2025 International Edition".

The packages need to be unpacked and placed in the `input` folder.

Each release contains different release types (https://confluence.ihtsdotools.org/display/DOCRELFMT/3.2+Release+Types ). The script only depends on the `snapshot` part of the release.

General documentation about the files can be found here: https://confluence.ihtsdotools.org/display/DOCRELFMT/SNOMED+CT+Release+File+Specifications

# Limitations
The script...

... uses only a subset of the available files, focusing on basic concept, term, and definition information.

... only load basic relation information

... only denormalizes Norwegian terms if there is a `acceptabilityId` in the regular language nb/nn refset. For example, in the termpost on  [brystsmerter med utstråling](https://browser.ihtsdotools.org/?perspective=full&conceptId1=10000006&edition=MAIN/SNOMEDCT-NO/2025-06-15&release=&languages=no,en) the term "utstrålande brystsmerter" is annotated as "akseptabel" for general practicioners, but is not referenced in the normal Nynorsk refset. This is currently not included as a Nynorsk term in the denormalized dataset. It is included as a Bokmål term. This source data in the release package might or might not be considerered valid.


