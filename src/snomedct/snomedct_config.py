"""Config for snomed processing."""

import polars as pl

extras = {
    "tables": (
        "Concept_Snapshot_NO",
        "Concept_Snapshot_INT",
        "Description_Snapshot-no_NO",
        "Description_Snapshot-en_NO",
        "Description_Snapshot-en_INT",
        "Definition_Snapshot-no_NO",
        "Definition_Snapshot-en_NO",
        "Definition_Snapshot-en_INT",
        "LanguageSnapshot-nb_NO",
        "LanguageSnapshot-nb-gp_NO",
        "LanguageSnapshot-nn_NO",
        "LanguageSnapshot-nn-gp_NO",
        "LanguageSnapshot-en_NO",
        "LanguageSnapshot-en_INT",
        "_Relationship_Snapshot_NO",
        "_Relationship_Snapshot_INT",
    ),
    "glob_pattern": "input/*/Snapshot/*/**/*.txt",
    "table_replace_values": {
        "SnomedTableDescription": [
            pl.col("typeId")
            .replace("900000000000003001", "fsn")
            .replace("900000000000013009", "synonym")
            .replace("900000000000550004", "definition"),
        ],
        "SnomedTableLanguage": [
            pl.col("refsetId").replace("900000000000508004", "en-GB").replace("900000000000509007", "en-US"),
            pl.col("acceptabilityId")
            .replace("900000000000548007", "tilrådd")
            .replace("900000000000549004", "akseptabel"),
        ],
        "SnomedTableRelationship": [pl.col("typeId").replace("116680003", "Is a (attribute)")],
    },
}
