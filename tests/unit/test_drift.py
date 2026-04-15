import json
from watchdog.drift import load_expected_state, build_expected_grants_lookup


class TestLoadExpectedState:
    def test_load_valid_json(self, tmp_path):
        state = {
            "generated_at": "2026-04-14T10:00:00Z",
            "environment": "production",
            "grants": [
                {
                    "catalog": "gold",
                    "schema": "finance",
                    "table": None,
                    "principal": "finance-analysts",
                    "privileges": ["SELECT", "USE_CATALOG"],
                }
            ],
        }
        f = tmp_path / "expected_state.json"
        f.write_text(json.dumps(state))
        result = load_expected_state(str(f))
        assert result == state
        assert len(result["grants"]) == 1

    def test_load_missing_file(self, tmp_path):
        result = load_expected_state(str(tmp_path / "nonexistent.json"))
        assert result == {}

    def test_load_malformed_json(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("not json {{{")
        result = load_expected_state(str(f))
        assert result == {}

    def test_load_no_grants_section(self, tmp_path):
        state = {"generated_at": "2026-04-14T10:00:00Z", "environment": "prod"}
        f = tmp_path / "expected_state.json"
        f.write_text(json.dumps(state))
        result = load_expected_state(str(f))
        assert "grants" not in result


class TestBuildExpectedGrantsLookup:
    def test_build_lookup_from_grants(self):
        grants = [
            {"catalog": "gold", "schema": "finance", "table": None,
             "principal": "finance-analysts", "privileges": ["SELECT", "USE_CATALOG"]},
            {"catalog": "gold", "schema": "finance", "table": "gl_balances",
             "principal": "data-engineers", "privileges": ["SELECT", "MODIFY"]},
        ]
        lookup = build_expected_grants_lookup(grants)
        assert "finance-analysts" in lookup
        assert "data-engineers" in lookup
        assert len(lookup["finance-analysts"]) == 1
        assert lookup["finance-analysts"][0]["privileges"] == ["SELECT", "USE_CATALOG"]

    def test_build_lookup_empty(self):
        lookup = build_expected_grants_lookup([])
        assert lookup == {}

    def test_build_lookup_multiple_entries_same_principal(self):
        grants = [
            {"catalog": "gold", "schema": "finance", "table": None,
             "principal": "analysts", "privileges": ["SELECT"]},
            {"catalog": "silver", "schema": "raw", "table": None,
             "principal": "analysts", "privileges": ["USE_CATALOG"]},
        ]
        lookup = build_expected_grants_lookup(grants)
        assert len(lookup["analysts"]) == 2
