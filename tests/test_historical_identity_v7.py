import unittest

from engine.normalizer import HISTORICAL_MATCH_LOGIC_VERSION, Normalizer


class HistoricalIdentityV7Tests(unittest.TestCase):
    def test_compact_total_dose_and_volume_matches_concentration(self):
        self.assertTrue(
            Normalizer.drug_name_validation_pass(
                "DANSETRON 2 MG/ML SOLUTION FOR INJECTION",
                "DANSETRON 4MG2ML HIKMA FARMACEUTICA",
                reference_match=True,
                wms_description="ONDANSETRON 4 MG AMPOULE",
            )
        )

    def test_explicit_combination_sum_matches_registered_total_strength(self):
        self.assertTrue(
            Normalizer.drug_name_validation_pass(
                "MADOPAR 250 MG TABS",
                "MADOPAR 250 ROCHE",
                reference_match=True,
                wms_description="LEVODOPA 200MG + BENSERAZIDE 50MG TABLET",
            )
        )

    def test_tisseel_operational_alias_is_narrowly_recognized(self):
        self.assertTrue(
            Normalizer.drug_name_validation_pass(
                "TISSEEL LYO POWDER AND SOLVENT FOR SEALANT",
                "GLUE TISSUE POWDER SOL 1505486 BAXTER",
                wms_description="GLUE TISSUE 1ML/2ML POWDER SOLUTION EA",
            )
        )

    def test_thymoglobuline_scientific_alias_is_narrowly_recognized(self):
        self.assertTrue(
            Normalizer.drug_name_validation_pass(
                "THYMOGLOBULINE",
                "ANTITHYMOCYTE GLOBULIN 25MG INJ SANOFI",
                wms_description="ANTITHYMOCYTE GLOBULIN 25MG INJ",
            )
        )

    def test_true_strength_conflict_remains_rejected(self):
        self.assertFalse(
            Normalizer.drug_name_validation_pass(
                "NORMALIX SR 1.5 MG MODIFIED-RELEASE TABLET",
                "NORMALIX 15MG TAB JPI",
                reference_match=True,
            )
        )

    def test_unresolved_valpohardt_strength_conflict_remains_rejected(self):
        self.assertFalse(
            Normalizer.drug_name_validation_pass(
                "VALPOHARDT 100 MG/ML SOLUTION FOR INJECTION OR INFUSION",
                "VALPOHARDT 400MG PWDR CP WOKHARDT",
                reference_match=True,
                wms_description="VALPROATE SODIUM 400 MG INJECTION",
                reference_match_description=True,
            )
        )

    def test_unrelated_products_remain_rejected(self):
        self.assertFalse(
            Normalizer.drug_name_validation_pass(
                "SANIDAL GEL",
                "BUTALIN INHALER",
            )
        )

    def test_logic_version_is_explicit(self):
        self.assertEqual(
            HISTORICAL_MATCH_LOGIC_VERSION,
            "SFDA_IDENTITY_V7_TARGETED_DOSE_IDENTITY_20260903",
        )


if __name__ == "__main__":
    unittest.main()
