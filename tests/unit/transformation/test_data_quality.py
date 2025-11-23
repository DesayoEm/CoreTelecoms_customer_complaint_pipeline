import pandas as pd
from datetime import datetime
import pytest
from dags.include.etl.transformation.data_cleaning import Cleaner
from dags.include.etl.transformation.data_quality import DataQualityChecker


@pytest.fixture
def cleaner():
    """Fixture providing a Cleaner instance"""
    return Cleaner()


@pytest.fixture
def dq_checker(cleaner):
    """Fixture providing a DataQualityChecker instance"""
    return DataQualityChecker(cleaner)


class TestIdentifyProblematicCustomers:
    """Tests for customer data quality checking"""

    def test_all_valid_customers(self, dq_checker):
        """Test with all valid customer data - should return empty DataFrame"""
        df = pd.DataFrame(
            {
                "customer_id": ["1g34", "3e4r", "cv5d"],
                "customer_key": ["key1", "key2", "key3"],
                "name": ["Moria Snow", "Fatai Rolling Dollars", "Odudubariba"],
                "gender": ["M", "F", "M"],
                "email": ["moria@gmail.com", "fatai@yahoo.com", "oduboy@hotmail.com"],
                "address": [
                    "123 Main St TX 75001",
                    "456 Oak Ave CA 90210",
                    "789 Pine Rd NY 10001",
                ],
            }
        )

        result = dq_checker.identify_problematic_customers(df)

        assert result.empty
        assert "problematic_fields" in result.columns
        assert "identified_at" in result.columns

    def test_invalid_gender(self, dq_checker):
        """Test with invalid gender values"""
        df = pd.DataFrame(
            {
                "customer_id": ["1", "2"],
                "customer_key": ["key1", "key2"],
                "name": ["Moria snow", "Fatai Rolling Dollars"],
                "gender": ["X", "Unknown"],
                "email": ["moria@gmail.com", "fatai@gmail.com"],
                "address": ["123 Main St TX 75001", "456 Oak Ave CA 90210"],
            }
        )

        result = dq_checker.identify_problematic_customers(df)

        assert len(result) == 2
        assert all(result["customer_key"].isin(["key1", "key2"]))

        fields = result.iloc[0]["problematic_fields"]
        assert len(fields) == 1
        assert fields[0]["field"] == "gender"
        assert fields[0]["original_value"] == "X"

    def test_invalid_email(self, dq_checker):
        """Test with invalid email addresses"""
        df = pd.DataFrame(
            {
                "customer_id": ["1", "2", "3"],
                "customer_key": ["key1", "key2", "key3"],
                "name": ["Moria Snow", "Fatai Rolling Dollars", "Odudubariba"],
                "gender": ["M", "F", "M"],
                "email": ["notanemail", "bad@@email", "valid@gmail.com"],
                "address": [
                    "123 Main St TX 75001",
                    "456 Oak Ave CA 90210",
                    "789 Pine Rd NY 10001",
                ],
            }
        )

        result = dq_checker.identify_problematic_customers(df)

        assert len(result) == 2
        assert "key1" in result["customer_key"].values
        assert "key2" in result["customer_key"].values

        assert "key3" not in result["customer_key"].values

    def test_missing_state_code(self, dq_checker):
        """Test with addresses missing valid state codes"""
        df = pd.DataFrame(
            {
                "customer_id": ["1", "2"],
                "customer_key": ["key1", "key2"],
                "name": ["Moria Snow", "Fatai Rolling Dollars"],
                "gender": ["M", "F"],
                "email": ["moria@gmail.com", "fatai@yahoo.com"],
                "address": [
                    "123 Main Street",
                    "Invalid Address Format",
                ],  # No state codes
            }
        )

        result = dq_checker.identify_problematic_customers(df)

        assert len(result) == 2

        fields = result.iloc[0]["problematic_fields"]
        assert any(f["field"] == "state_code" for f in fields)
        assert any(f["original_value"] == "123 Main Street" for f in fields)

    def test_multiple_issues_single_customer(self, dq_checker):
        """Test customer with multiple data quality issues"""
        df = pd.DataFrame(
            {
                "customer_id": ["1"],
                "customer_key": ["key1"],
                "name": ["Moria Snow"],
                "gender": ["X"],  # invalid
                "email": ["nonsenseemail"],  # invalid
                "address": ["nonsense address"],  # missing state and zip code
            }
        )

        result = dq_checker.identify_problematic_customers(df)

        assert len(result) == 1
        fields = result.iloc[0]["problematic_fields"]
        assert len(fields) == 3

        field_names = [f["field"] for f in fields]
        assert "gender" in field_names
        assert "email" in field_names
        assert "state_code" in field_names

    def test_null_values_not_flagged(self, dq_checker):
        """Test that null values are not flagged as problematic"""
        df = pd.DataFrame(
            {
                "customer_id": ["1", "2"],
                "customer_key": ["key1", "key2"],
                "name": ["Moria Snow", "Fatai Rolling Dollars"],
                "gender": [None, "M"],  # null is fine
                "email": [None, "fatai@yahoo.com"],  # null is fine
                "address": [None, "123 Main St TX 75001"],  # null is fine
            }
        )

        result = dq_checker.identify_problematic_customers(df)

        # should be empty since there are no invalid vals
        assert result.empty

    def test_identified_at_timestamp(self, dq_checker):
        """Test that identified_at timestamp is added"""
        df = pd.DataFrame(
            {
                "customer_id": ["1"],
                "customer_key": ["key1"],
                "name": ["Moria Snow"],
                "gender": ["X"],
                "email": ["moria@gmail.com"],
                "address": ["123 Main St TX 75001"],
            }
        )

        before = datetime.now().isoformat()
        result = dq_checker.identify_problematic_customers(df)
        after = datetime.now().isoformat()

        assert "identified_at" in result.columns
        timestamp = result.iloc[0]["identified_at"]
        assert before <= timestamp <= after


class TestIdentifyProblematicAgents:
    """Tests for agent data quality checking"""

    def test_all_valid_agents(self, dq_checker):
        """Test with all valid agent data"""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "agent_key": ["key1", "key2"],
                "name": ["Moria Snow", "Fatai Rolling Dollars"],
                "experience": ["Senior", "Junior"],
                "state": ["Texas", "California"],
            }
        )

        result = dq_checker.identify_problematic_agents(df)

        assert result.empty

    def test_invalid_experience(self, dq_checker):
        """Test with invalid experience levels"""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "agent_key": ["key1", "key2"],
                "name": ["Moria Snow", "Fatai Rolling Dollars"],
                "experience": ["Expert", "Manager"],  # invalid
                "state": ["Texas", "California"],
            }
        )

        result = dq_checker.identify_problematic_agents(df)

        assert len(result) == 2
        fields = result.iloc[0]["problematic_fields"]
        assert fields[0]["field"] == "experience"

    def test_invalid_state(self, dq_checker):
        """Test with invalid state names"""
        df = pd.DataFrame(
            {
                "id": ["1"],
                "agent_key": ["key1"],
                "name": ["Moria Snow"],
                "experience": ["Senior"],
                "state": ["Not A State"],  # invalid
            }
        )

        result = dq_checker.identify_problematic_agents(df)

        assert len(result) == 1
        fields = result.iloc[0]["problematic_fields"]
        assert fields[0]["field"] == "state"
        assert fields[0]["original_value"] == "Not A State"


class TestIdentifyProblematicWebComplaints:
    """Tests for web complaint data quality checking"""

    def test_all_valid_complaints(self, dq_checker):
        """Test with all valid web complaint data"""
        df = pd.DataFrame(
            {
                "request_id": ["1", "2"],
                "web_complaint_key": ["key1", "key2"],
                "complaint_category": ["Network Failure", "Payments"],
                "resolution_status": ["Resolved", "Backlog"],
            }
        )

        result = dq_checker.identify_problematic_web_complaints(df)

        assert result.empty

    def test_invalid_category(self, dq_checker):
        """Test with invalid complaint categories"""
        df = pd.DataFrame(
            {
                "request_id": ["1"],
                "web_complaint_key": ["key1"],
                "complaint_category": ["Unknown Issue"],  # invalid
                "resolution_status": ["Resolved"],
            }
        )

        result = dq_checker.identify_problematic_web_complaints(df)

        assert len(result) == 1
        fields = result.iloc[0]["problematic_fields"]
        assert fields[0]["field"] == "complaint_category"

    def test_invalid_status(self, dq_checker):
        """Test with invalid resolution statuses"""
        df = pd.DataFrame(
            {
                "request_id": ["1"],
                "web_complaint_key": ["key1"],
                "complaint_category": ["Payments"],
                "resolution_status": ["Pending"],  # invalid
            }
        )

        result = dq_checker.identify_problematic_web_complaints(df)

        assert len(result) == 1
        fields = result.iloc[0]["problematic_fields"]
        assert fields[0]["field"] == "resolution_status"


class TestIdentifyProblematicSMComplaints:
    """Tests for social media complaint data quality checking"""

    def test_all_valid_sm_complaints(self, dq_checker):
        """Test with all valid SM complaint data"""
        df = pd.DataFrame(
            {
                "complaint_id": ["1", "2"],
                "sm_complaint_key": ["key1", "key2"],
                "complaint_category": ["Network Failure", "Payments"],
                "resolution_status": ["Resolved", "Backlog"],
                "media_channel": ["Instagram", "Twitter"],
            }
        )

        result = dq_checker.identify_problematic_sm_complaints(df)

        assert result.empty

    def test_invalid_media_channel(self, dq_checker):
        """Test with invalid media channels"""
        df = pd.DataFrame(
            {
                "complaint_id": ["1"],
                "sm_complaint_key": ["key1"],
                "complaint_category": ["Payments"],
                "resolution_status": ["Resolved"],
                "media_channel": ["TikTok"],  # invalid
            }
        )

        result = dq_checker.identify_problematic_sm_complaints(df)

        assert len(result) == 1
        fields = result.iloc[0]["problematic_fields"]
        assert any(f["field"] == "media_channel" for f in fields)


class TestIdentifyProblematicCallLogs:
    """Tests for call log data quality checking"""

    def test_all_valid_call_logs(self, dq_checker):
        """Test with all valid call log data"""
        df = pd.DataFrame(
            {
                "call_id": ["1", "2"],
                "call_log_key": ["key1", "key2"],
                "complaint_category": ["Network Failure", "Payments"],
                "resolution_status": ["Resolved", "In-Progress"],
            }
        )

        result = dq_checker.identify_problematic_call_logs(df)

        assert result.empty

    def test_multiple_invalid_fields(self, dq_checker):
        """Test call log with multiple invalid fields"""
        df = pd.DataFrame(
            {
                "call_id": ["1"],
                "call_log_key": ["key1"],
                "complaint_category": ["Unknown"],  # invalid
                "resolution_status": ["Pending"],  # invalid
            }
        )

        result = dq_checker.identify_problematic_call_logs(df)

        assert len(result) == 1
        fields = result.iloc[0]["problematic_fields"]
        assert len(fields) == 2


class TestGenericProblematicRecords:
    """Tests for the generic problematic records identification method"""

    def test_generic_method_with_custom_masks(self, dq_checker):
        """Test the generic identification method with custom masks"""
        df = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "field_a": ["valid", "invalid", "valid"],
                "field_b": ["good", "good", "bad"],
            }
        )

        # Create custom masks
        mask_a = ~df["field_a"].isin(["valid"])
        mask_b = ~df["field_b"].isin(["good"])

        mask_field_pairs = [
            (mask_a, "field_a", "field_a"),
            (mask_b, "field_b", "field_b"),
        ]

        result = dq_checker.identify_problematic_records(df, mask_field_pairs)

        assert len(result) == 2  # Rows 1 and 2
        assert "1" in result["id"].values  # Has invalid field_a
        assert "2" in result["id"].values  # Has bad field_b

    def test_empty_dataframe_handling(self, dq_checker):
        """Test generic method with empty DataFrame"""
        df = pd.DataFrame({"id": [], "field_a": []})

        mask_a = pd.Series([], dtype=bool)
        mask_field_pairs = [(mask_a, "field_a", "field_a")]

        result = dq_checker.identify_problematic_records(df, mask_field_pairs)

        assert result.empty
        assert "problematic_fields" in result.columns


class TestBuildProblematicFields:
    """Tests for the build_problematic_fields helper method"""

    def test_build_fields_single_issue(self, dq_checker):
        """Test building problematic fields for a single issue"""
        df = pd.DataFrame({"id": ["1"], "field_a": ["bad_value"]})

        mask_a = pd.Series([True])
        mask_field_pairs = [(mask_a, "field_a", "field_a")]

        row = df.iloc[0]
        fields = dq_checker.build_problematic_fields(row, mask_field_pairs)

        assert len(fields) == 1
        assert fields[0]["field"] == "field_a"
        assert fields[0]["original_value"] == "bad_value"

    def test_build_fields_multiple_issues(self, dq_checker):
        """Test building problematic fields for multiple issues"""
        df = pd.DataFrame({"id": ["1"], "field_a": ["bad_a"], "field_b": ["bad_b"]})

        mask_a = pd.Series([True])
        mask_b = pd.Series([True])
        mask_field_pairs = [
            (mask_a, "field_a", "field_a"),
            (mask_b, "field_b", "field_b"),
        ]

        row = df.iloc[0]
        fields = dq_checker.build_problematic_fields(row, mask_field_pairs)

        assert len(fields) == 2
        field_names = [f["field"] for f in fields]
        assert "field_a" in field_names
        assert "field_b" in field_names


class TestDataQualityIntegration:
    """Integration tests simulating real pipeline usage"""

    def test_customer_pipeline_simulation(self, dq_checker):
        """Simulate full customer data quality check pipeline"""
        # customer data with mixed quality
        df = pd.DataFrame(
            {
                "customer_id": ["1", "2", "3", "4", "5"],
                "customer_key": ["key1", "key2", "key3", "key4", "key5"],
                "name": [
                    "Moria Snow",
                    "Fatai Rolling Dollars",
                    "Odudubariba",
                    "Alice Wonder",
                    "Charlie Brown",
                ],
                "gender": ["M", "X", "F", "M", "Unknown"],  # 2 invalid
                "email": [
                    "moria@gmail.com",
                    "jane@yahoo.com",
                    "notanemail",  # invalid
                    "alice@hotmail.com",
                    "bad@@email",  # invalid
                ],
                "address": [
                    "123 Main St TX 75001",
                    "Invalid Address",  ## missing state and zip
                    "789 Pine Rd NY 10001",
                    "101 Oak Ave CA 90210",
                    "NonsEnSe Address",  # missing state and zip
                ],
            }
        )

        result = dq_checker.identify_problematic_customers(df)

        # must identify 4 problematic records (2,3,4,5)
        # record 2: invalid gender + missing state
        # record 3: invalid email
        # record 4: invalid gender
        # record 5: invalid email + missing state
        assert len(result) >= 4

        # verify all problematic records have required columns
        assert "problematic_fields" in result.columns
        assert "identified_at" in result.columns

        # verify problematic_fields is populated
        for _, row in result.iterrows():
            assert len(row["problematic_fields"]) > 0
            for field in row["problematic_fields"]:
                assert "field" in field
                assert "original_value" in field
