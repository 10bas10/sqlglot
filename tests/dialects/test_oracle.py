from tests.dialects.test_dialect import Validator


class TestOracle(Validator):
    def test_oracle(self):
        self.validate_identity("SELECT * FROM V$SESSION")
