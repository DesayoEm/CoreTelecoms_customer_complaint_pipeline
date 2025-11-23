import pytest
import pandas as pd
from dags.include.etl.transformation.data_cleaning import Cleaner


class TestStandardizeColumnName:
    """Tests for column name standardization"""

    @pytest.mark.parametrize(
        "input_col,expected",
        [
            # manual corrections
            ("custome_r_i_d", "customer_id"),
            ("complaint_catego_ry", "complaint_category"),
            ("COMPLAINT_catego ry", "complaint_category"),
            ("webformgenerationdate", "web_form_generation_date"),
            ("resolutionstatus", "resolution_status"),

            # spaces and camelcase
            ("CustomerName", "customer_name"),
            ("  SpacedOut  ", "spaced_out"),
            ("EmailAddress", "email_address"),
            ("ComplaintID", "complaint_id"),

            # spaces and special chars
            ("First Name", "first_name"),
            ("Email-Address", "email_address"),
            ("user@domain", "user_domain"),

            # already clean
            ("customer_id", "customer_id"),
            ("email", "email"),


        ],
    )
    def test_standardize_column_name(self, input_col, expected):
        assert Cleaner.standardize_column_name(input_col) == expected


class TestCleanEmail:
    """Tests for email cleaning and validation"""

    @pytest.mark.parametrize(
        "email,expected",
        [
            # validemails should be cleaned and returned
            ("test@gmail.com", "test@gmail.com"),
            ("Test@Gmail.COM", "test@gmail.com"),
            ("  user@example.com  ", "user@example.com"),

            # common typos should be fixed
            ("test@gmail.om", "test@gmail.com"),
            ("test@gmial.com", "test@gmail.com"),
            ("test@hotmai.com", "test@hotmail.com"),
            ("test@hotmaill.com", "test@hotmail.com"),

            # invalidpatterns should be cleaned
            ("test@123@gmail.com", "123@gmail.com"),  # Double @ with numbers
            ("test@@gmail.com", "test@gmail.com"),  # Special chars before @


            # invalidemails should return None
            ("notanemail", None),
            ("missing@domain", None),
            ("@nodomain.com", None),
            ("noat.com", None),
            ("test@", None),
            ("@domain.com", None),

            # null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_clean_email(self, email, expected):
        result = Cleaner.clean_email(email)
        assert result == expected


class TestExtractStateCode:
    """Tests for state code extraction from addresses"""

    @pytest.mark.parametrize(
        "address,expected",
        [
            # validaddresses
            ("123 Main St TX 75001", "TX"),
            ("456 Oak Ave CA 90210", "CA"),
            ("789 Pine Rd NY 10001", "NY"),
            ("PO Box 123 FL 33101", "FL"),
            # Military codes
            ("APO AE 09001", "AE"),
            ("FPO AP 96601", "AP"),
            # Territories
            ("123 Beach Rd PR 00901", "PR"),
            ("456 Island Ave GU 96910", "GU"),
            # invalidaddresses - should return None
            ("123 Main St", None),  # no state/zip
            ("invalidAddress", None),  # malformed
            ("123 Main St XX 12345", None),  # invalidstate code
            ("", None),  # empty string
            # Null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_extract_state_code(self, address, expected):
        result = Cleaner.extract_state_code(address)
        assert result == expected


class TestGenerateState:
    """Tests for state name generation from state codes"""

    @pytest.mark.parametrize(
        "state_code,expected",
        [
            # States
            ("TX", "Texas"),
            ("CA", "California"),
            ("NY", "New York"),
            ("FL", "Florida"),
            # Territories
            ("PR", "Puerto Rico"),
            ("GU", "Guam"),
            ("VI", "Virgin Islands"),
            # Military
            ("AE", "Armed Forces Europe"),
            ("AP", "Armed Forces Pacific"),
            # invalidcodes
            ("XX", None),
            ("ZZ", None),
            # Null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_generate_state(self, state_code, expected):
        result = Cleaner.generate_state(state_code)
        assert result == expected


class TestExtractZipCode:
    """Tests for zip code extraction from addresses"""

    @pytest.mark.parametrize(
        "address,expected",
        [
            # validaddresses
            ("123 Main St TX 75001", "75001"),
            ("456 Oak Ave CA 90210", "90210"),
            ("789 Pine Rd NY 10001", "10001"),
            # invalidzip codes - should return None
            ("123 Main St TX 750", None),  # Too short
            ("123 Main St TX 7500123", None),  # Too long
            ("123 Main St TX ABCDE", None),  # Not numeric
            ("123 Main St", None),  # Missing zip
            # Null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_extract_zip_code(self, address, expected):
        result = Cleaner.extract_zip_code(address)
        assert result == expected


class TestStandardizeName:
    """Tests for name standardization"""

    @pytest.mark.parametrize(
        "name,expected",
        [
            # Standard cases
            ("john doe", "John Doe"),
            ("JANE SMITH", "Jane Smith"),
            ("bob jones", "Bob Jones"),
            # Edge cases
            ("  alice  wonder  ", "Alice  Wonder"),  # Extra spaces preserved in middle
            ("mary-jane watson", "Mary-Jane Watson"),
            ("o'brien", "O'Brien"),
            # Single names
            ("madonna", "Madonna"),
            ("CHER", "Cher"),
            # Null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_standardize_name(self, name, expected):
        result = Cleaner.standardize_name(name)
        assert result == expected


class TestValidateState:
    """Tests for state validation"""

    @pytest.mark.parametrize(
        "state,expected",
        [
            # validstates - various cases
            ("texas", "Texas"),
            ("Texas", "Texas"),
            ("TEXAS", "Texas"),
            ("california", "California"),
            ("new york", "New York"),
            # validterritories
            ("puerto rico", "Puerto Rico"),
            ("guam", "Guam"),
            # invalidstates
            ("Texass", None),
            ("Not A State", None),
            ("XX", None),
            # Null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_validate_state(self, state, expected):
        result = Cleaner.validate_state(state)
        assert result == expected


class TestValidateExperienceLevel:
    """Tests for experience level validation"""

    @pytest.mark.parametrize(
        "experience,expected",
        [
            # validlevels - various cases
            ("intern", "Intern"),
            ("Intern", "Intern"),
            ("INTERN", "Intern"),
            ("junior", "Junior"),
            ("mid-level", "Mid-Level"),
            ("senior", "Senior"),
            # invalidlevels
            ("Expert", None),
            ("Manager", None),
            ("Unknown", None),
            # Null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_validate_experience_level(self, experience, expected):
        result = Cleaner.validate_experience_level(experience)
        assert result == expected


class TestValidateComplaintCategory:
    """Tests for complaint category validation"""

    @pytest.mark.parametrize(
        "category,expected",
        [
            # valid categories
            ("technician support", "Technician Support"),
            ("Technician Support", "Technician Support"),
            ("TECHNICIAN SUPPORT", "Technician Support"),
            ("network failure", "Network Failure"),
            ("router delivery", "Router Delivery"),
            ("payments", "Payments"),
            # invalid categories
            ("Unknown Issue", None),
            ("Other", None),
            # Null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_validate_complaint_category(self, category, expected):
        result = Cleaner.validate_complaint_category(category)
        assert result == expected


class TestValidateResolutionStatus:
    """Tests for resolution status validation"""

    @pytest.mark.parametrize(
        "status,expected",
        [
            # valid statuses - various cases
            ("resolved", "Resolved"),
            ("Resolved", "Resolved"),
            ("RESOLVED", "Resolved"),
            ("backlog", "Backlog"),
            ("in-progress", "In-Progress"),
            ("blocked", "Blocked"),
            # invalid statuses
            ("pending", None),
            ("closed", None),
            ("Unknown", None),
            # Null handling
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_validate_resolution_status(self, status, expected):
        result = Cleaner.validate_resolution_status(status)
        assert result == expected


class TestValidateMediaChannel:
    """Tests for media channel validation"""

    @pytest.mark.parametrize(
        "channel,expected",
        [
            ("instagram", "INSTAGRAM"),
            ("Instagram", "INSTAGRAM"),
            ("INSTAGRAM", "INSTAGRAM"),
            ("twitter", "TWITTER"),
            ("facebook", "FACEBOOK"),
            ("linkedin", "LINKEDIN"),
            ("tiktok", None),
            ("snapchat", None),
            ("Unknown", None),
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_validate_media_channel(self, channel, expected):
        result = Cleaner.validate_media_channel(channel)
        assert result == expected


class TestValidateGender:
    """Tests for gender validation"""

    @pytest.mark.parametrize(
        "gender,expected",
        [
            ("m", "M"),
            ("M", "M"),
            ("f", "F"),
            ("F", "F"),
            ("male", None),
            ("female", None),
            ("X", None),
            ("Unknown", None),
            ("", None),
            (None, None),
            (pd.NA, None),
        ],
    )
    def test_validate_gender(self, gender, expected):
        result = Cleaner.validate_gender(gender)
        assert result == expected


class TestCleanerIntegration:
    """Integration tests with actual DataFrame operations"""

    def test_email_cleaning_in_series(self):
        """Test email cleaning works with pandas Series"""
        emails = pd.Series(
            ["test@gmail.com", "BAD@@EMAIL", "valid@domain.com", None, "invalid"]
        )

        result = emails.apply(Cleaner.clean_email)

        assert result.iloc[0] == "test@gmail.com"
        assert result.iloc[1] is None
        assert result.iloc[2] == "valid@domain.com"
        assert pd.isna(result.iloc[3])
        assert result.iloc[4] is None

    def test_state_code_extraction_in_series(self):
        """Test state code extraction works with pandas Series"""
        addresses = pd.Series(
            ["123 Main St TX 75001", "456 Oak Ave CA 90210", "invalidAddress", None]
        )

        result = addresses.apply(Cleaner.extract_state_code)

        assert result.iloc[0] == "TX"
        assert result.iloc[1] == "CA"
        assert result.iloc[2] is None
        assert pd.isna(result.iloc[3])

    def test_gender_validation_in_series(self):
        """Test gender validation works with pandas Series"""
        genders = pd.Series(["m", "F", "X", "Unknown", None])

        result = genders.apply(Cleaner.validate_gender)

        assert result.iloc[0] == "M"
        assert result.iloc[1] == "F"
        assert result.iloc[2] is None
        assert result.iloc[3] is None
        assert pd.isna(result.iloc[4])
