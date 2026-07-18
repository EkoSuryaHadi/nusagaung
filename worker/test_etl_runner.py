import unittest
import pandas as pd
import numpy as np
import sys
import os

# Ensure worker directory is in sys.path
sys.path.insert(0, os.path.dirname(__file__))

from etl_runner import (
    to_numeric_clean,
    step_validate,
    translate_format,
    sanitize_identifier,
    sanitize_layer,
    find_column_robust,
    compute_data_quality,
    step_insight,
    step_anomaly_detect,
    step_forecast,
    step_classify,
)


class TestETLRunner(unittest.TestCase):

    def test_sanitize_identifier(self):
        self.assertEqual(sanitize_identifier("Table Name!"), "Table_Name_")
        self.assertEqual(sanitize_identifier("drop table;--"), "drop_table_--")
        self.assertEqual(sanitize_identifier("123column"), "123column")

    def test_sanitize_layer(self):
        self.assertEqual(sanitize_layer("bronze"), "bronze")
        self.assertEqual(sanitize_layer("SILVER"), "silver")
        self.assertEqual(sanitize_layer("GOLD"), "gold")
        with self.assertRaises(ValueError):
            sanitize_layer("invalid_layer")

    def test_translate_format(self):
        self.assertEqual(translate_format("YYYY-MM-DD"), "%Y-%m-%d")
        self.assertEqual(translate_format("DD/MM/YYYY"), "%d/%m/%Y")
        self.assertIsNone(translate_format(""))

    def test_to_numeric_clean_indonesian_currency(self):
        series = pd.Series(["Rp 1.500.000,00", "Rp 250.000", "(50.000,00)", "100 pcs", "15.5%"])
        cleaned = to_numeric_clean(series)
        self.assertAlmostEqual(cleaned[0], 1500000.0)
        self.assertAlmostEqual(cleaned[1], 250000.0)
        self.assertAlmostEqual(cleaned[2], -50000.0)
        self.assertAlmostEqual(cleaned[3], 100.0)
        self.assertAlmostEqual(cleaned[4], 15.5)

    def test_compute_data_quality(self):
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "sales": [100.0, 200.0, 150.0, 180.0, 5000.0]
        })
        dq = compute_data_quality(df)
        self.assertIn("score", dq)
        self.assertIn("details", dq)
        self.assertGreaterEqual(dq["score"], 0.0)
        self.assertLessEqual(dq["score"], 100.0)
        self.assertEqual(dq["details"]["completeness"], 100.0)

    def test_step_insight(self):
        df = pd.DataFrame({
            "sales": [100, 200, 150, 300, 250],
            "profit": [20, 40, 30, 60, 50],
            "category": ["A", "A", "B", "A", "B"]
        })
        res = step_insight(df, {})
        self.assertIn("insights", res.attrs)
        self.assertGreater(len(res.attrs["insights"]), 0)

    def test_step_anomaly_detect(self):
        df = pd.DataFrame({
            "amount": [100, 105, 98, 102, 10000]
        })
        res = step_anomaly_detect(df, {"columns": ["amount"]})
        self.assertIn("_anomaly_score", res.columns)
        self.assertIn("_anomaly_label", res.columns)
        self.assertEqual(res.loc[4, "_anomaly_label"], "ANOMALY")

    def test_step_classify(self):
        df = pd.DataFrame({
            "review": ["Pelayanan sangat bagus dan mantap", "Produk jelek dan rugi", "Biasa saja"]
        })
        res = step_classify(df, {"textColumn": "review"})
        self.assertIn("sentiment_label", res.columns)
        self.assertEqual(res.loc[0, "sentiment_label"], "POSITIF")
        self.assertEqual(res.loc[1, "sentiment_label"], "NEGATIF")
        self.assertEqual(res.loc[2, "sentiment_label"], "NETRAL")

    def test_step_validate_not_null(self):
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "", None]})
        config = {
            "rules": [{"type": "NOT_NULL", "column": "name"}],
            "validationMode": "flag",
        }
        res = step_validate(df, config)
        self.assertIn("_validation_issues", res.columns)
        self.assertEqual(res.loc[0, "_validation_issues"], "PASS")
        self.assertIn("Missing name", res.loc[1, "_validation_issues"])
        self.assertIn("Missing name", res.loc[2, "_validation_issues"])

    def test_step_validate_compare(self):
        df = pd.DataFrame({"sap_amt": [1000, 2000, 3000], "bank_amt": [1000, 2000, 3500]})
        config = {
            "rules": [{"type": "COMPARE", "col1": "sap_amt", "col2": "bank_amt", "tolerance": 0}],
            "validationMode": "flag",
        }
        res = step_validate(df, config)
        self.assertEqual(res.loc[0, "_validation_issues"], "PASS")
        self.assertEqual(res.loc[1, "_validation_issues"], "PASS")
        self.assertIn("Mismatch sap_amt vs bank_amt", res.loc[2, "_validation_issues"])

    def test_step_validate_enum(self):
        df = pd.DataFrame({"status": ["ACTIVE", "INACTIVE", "PENDING", "INVALID"]})
        config = {
            "rules": [{"type": "ENUM", "column": "status", "values": ["ACTIVE", "INACTIVE", "PENDING"]}],
            "validationMode": "flag",
        }
        res = step_validate(df, config)
        self.assertEqual(res.loc[0, "_validation_issues"], "PASS")
        self.assertEqual(res.loc[1, "_validation_issues"], "PASS")
        self.assertEqual(res.loc[2, "_validation_issues"], "PASS")
        self.assertIn("Invalid enum status", res.loc[3, "_validation_issues"])

    def test_step_validate_unique(self):
        df = pd.DataFrame({"code": ["A1", "B2", "A1"]})
        config = {
            "rules": [{"type": "UNIQUE", "column": "code"}],
            "validationMode": "flag",
        }
        res = step_validate(df, config)
        self.assertIn("Duplicate code", res.loc[0, "_validation_issues"])
        self.assertEqual(res.loc[1, "_validation_issues"], "PASS")
        self.assertIn("Duplicate code", res.loc[2, "_validation_issues"])


if __name__ == "__main__":
    unittest.main()

