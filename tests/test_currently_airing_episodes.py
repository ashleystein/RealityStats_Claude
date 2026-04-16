import unittest
from unittest.mock import patch
from datetime import date, timedelta

import pandas as pd

from src.data_pipeline.defs.assets.refresh_analytics import currently_airing_episodes

# Access the underlying function, bypassing the @dg.asset decorator.
_fn = currently_airing_episodes.op.__wrapped__

TODAY = date(2026, 4, 14)


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build an episode_info DataFrame with the columns the asset expects."""
    return pd.DataFrame(rows, columns=["Show", "Season", "Air Date"])


class TestCurrentlyAiringEpisodes(unittest.TestCase):

    def _run(self, df: pd.DataFrame) -> list | bool:
        """Call the asset function with a mocked CSV and a fixed today date."""
        with patch(
            "src.data_pipeline.defs.assets.refresh_analytics.pd.read_csv",
            return_value=df,
        ), patch(
            "src.data_pipeline.defs.assets.refresh_analytics.date"
        ) as mock_date:
            mock_date.today.return_value = TODAY
            # timedelta must remain real; only date.today is mocked
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            return _fn()

    # ------------------------------------------------------------------
    # Empty / missing data
    # ------------------------------------------------------------------

    def test_empty_csv_returns_false(self):
        df = _make_df([])
        result = self._run(df)
        self.assertFalse(result)

    def test_all_nan_dates_returns_empty_list(self):
        df = _make_df([
            {"Show": "Bachelor", "Season": "S1", "Air Date": None},
            {"Show": "Traitors", "Season": "S2", "Air Date": None},
        ])
        result = self._run(df)
        self.assertEqual(result, [])

    # ------------------------------------------------------------------
    # Episodes inside the ±30-day window
    # ------------------------------------------------------------------

    def test_episode_airing_today_is_included(self):
        df = _make_df([
            {"Show": "Bachelor", "Season": "S1", "Air Date": str(TODAY)},
        ])
        result = self._run(df)
        self.assertIn("Bachelor S1", result)

    def test_episode_30_days_ago_is_included(self):
        air_date = TODAY - timedelta(days=30)
        df = _make_df([
            {"Show": "Traitors", "Season": "S3", "Air Date": str(air_date)},
        ])
        result = self._run(df)
        self.assertIn("Traitors S3", result)

    def test_episode_30_days_from_now_is_included(self):
        air_date = TODAY + timedelta(days=30)
        df = _make_df([
            {"Show": "Survivor", "Season": "S46", "Air Date": str(air_date)},
        ])
        result = self._run(df)
        self.assertIn("Survivor S46", result)

    def test_episode_within_window_is_included(self):
        air_date = TODAY + timedelta(days=10)
        df = _make_df([
            {"Show": "BigBrother", "Season": "S25", "Air Date": str(air_date)},
        ])
        result = self._run(df)
        self.assertIn("BigBrother S25", result)

    # ------------------------------------------------------------------
    # Episodes outside the ±30-day window
    # ------------------------------------------------------------------

    def test_episode_31_days_ago_is_excluded(self):
        air_date = TODAY - timedelta(days=31)
        df = _make_df([
            {"Show": "Bachelor", "Season": "S1", "Air Date": str(air_date)},
        ])
        result = self._run(df)
        self.assertNotIn("Bachelor S1", result)

    def test_episode_31_days_from_now_is_excluded(self):
        air_date = TODAY + timedelta(days=31)
        df = _make_df([
            {"Show": "Bachelor", "Season": "S1", "Air Date": str(air_date)},
        ])
        result = self._run(df)
        self.assertNotIn("Bachelor S1", result)

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def test_duplicate_episodes_deduplicated(self):
        """Two rows for the same show/season within the window → appears once."""
        df = _make_df([
            {"Show": "Bachelor", "Season": "S1", "Air Date": str(TODAY)},
            {"Show": "Bachelor", "Season": "S1", "Air Date": str(TODAY + timedelta(days=7))},
        ])
        result = self._run(df)
        self.assertEqual(result.count("Bachelor S1"), 1)

    def test_multiple_distinct_shows_all_returned(self):
        df = _make_df([
            {"Show": "Bachelor", "Season": "S1", "Air Date": str(TODAY)},
            {"Show": "Traitors", "Season": "S3", "Air Date": str(TODAY)},
        ])
        result = self._run(df)
        self.assertIn("Bachelor S1", result)
        self.assertIn("Traitors S3", result)
        self.assertEqual(len(result), 2)

    # ------------------------------------------------------------------
    # Mixed in-window / out-of-window rows
    # ------------------------------------------------------------------

    def test_only_in_window_episodes_returned(self):
        df = _make_df([
            {"Show": "Bachelor", "Season": "S1", "Air Date": str(TODAY)},
            {"Show": "OldShow", "Season": "S99", "Air Date": str(TODAY - timedelta(days=90))},
        ])
        result = self._run(df)
        self.assertIn("Bachelor S1", result)
        self.assertNotIn("OldShow S99", result)

    # ------------------------------------------------------------------
    # Return type
    # ------------------------------------------------------------------

    def test_returns_list_when_episodes_exist(self):
        df = _make_df([
            {"Show": "Bachelor", "Season": "S1", "Air Date": str(TODAY)},
        ])
        result = self._run(df)
        self.assertIsInstance(result, list)

    def test_returns_false_not_empty_list_for_empty_csv(self):
        """Distinguish between no rows in the CSV vs rows with no matching dates."""
        df = _make_df([])
        result = self._run(df)
        self.assertIs(result, False)


if __name__ == "__main__":
    unittest.main()
