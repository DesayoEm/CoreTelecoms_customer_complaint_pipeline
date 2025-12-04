## Double-Pass Validation Strategy

I faced a dilemma when encountering invalid data:

**Option 1: Reject Invalid Records**
-  Lose visibility into what was wrong
-  Can't investigate patterns in bad data
-  No audit trail for compliance

**Option 2: Clean and Load Everything**
-  Silently corrupt data (e.g., invalid email becomes NULL)
-  Can't distinguish "never had email" from "had invalid email"
-  Data quality issues invisible

---

### The Solution: Double-Pass Validation

The system validates data **twice**—once to identify and preserve problematic records, once to actually clean them.

The double-pass validation strategy treats **data quality as an observable process**, and not just a cleaning step.


**Pass 1 (Identify)**: Find invalid records, preserve original values, upload to S3  
**Pass 2 (Transform)**: Apply cleaning transformations to all records

**Key Insight**: We process the data twice because **observation** and **transformation** serve different purposes and need different outputs.

---


### Implementation

#### 1. DataQualityChecker (Pass 1: Identify)
```python
class DataQualityChecker:
    """Identifies data quality issues without modifying data."""
    
    def identify_problematic_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Find customer records with validation failures.
        Preserves original invalid values for investigation.
        """
        
        # Validation masks identify failures
        missing_natural_key_mask = df["customer_id"].isna()
        
        invalid_gender_mask = (
            df["gender"].notna() & 
            ~df["gender"].str.lower().isin(GENDER)  # ['m', 'f']
        )
        
        # Apply cleaning logic WITHOUT modifying df
        email_series = df["email"].apply(
            lambda x: self.cleaner.clean_email(x) if pd.notna(x) else x
        )
        invalid_email_mask = df["email"].notna() & email_series.isna()
        
        state_code_series = df["address"].apply(
            lambda x: self.cleaner.extract_state_code(x) if pd.notna(x) else x
        )
        missing_state_mask = df["address"].notna() & state_code_series.isna()
        
        # Define what to track for each failure type
        mask_field_pairs = [
            (missing_natural_key_mask, "customer_id", "customer_id"),
            (invalid_gender_mask, "gender", "gender"),
            (invalid_email_mask, "email", "email"),
            (missing_state_mask, "state_code", "address"),
        ]
        
        return self.identify_problematic_records(df, mask_field_pairs)
```

**Critical Detail**: The `email_series` and `state_code_series` are **separate Series** created by applying cleaning functions. The original `df` is **never modified**. This allows us to:
1. Test if cleaning would fail (NULL result)
2. Preserve the original invalid value
3. Apply actual cleaning in Pass 2

#### 2. Generic Problematic Record Builder
```python
def identify_problematic_records(
    self, 
    df: pd.DataFrame, 
    mask_field_pairs: List[Tuple[pd.Series, str, str]]
) -> pd.DataFrame:
    """
    Generic method to identify problematic records.
    
    Args:
        df: Source DataFrame
        mask_field_pairs: List of (mask, field_name, source_column) tuples
            - mask: Boolean Series identifying failures
            - field_name: Name to use in problematic_fields
            - source_column: Column to preserve original value from
    
    Returns:
        DataFrame with problematic records + metadata
    """
    
    # Combine all failure masks (OR logic)
    combined_mask = mask_field_pairs[0][0]
    for mask, _, _ in mask_field_pairs[1:]:
        combined_mask = combined_mask | mask
    
    # Early exit if no problems
    if not combined_mask.any():
        return pd.DataFrame(
            columns=list(df.columns) + ["problematic_fields", "identified_at"]
        )
    
    # Extract problematic records
    problematic_df = df[combined_mask].copy()
    
    # Add metadata about what failed
    problematic_df["problematic_fields"] = problematic_df.apply(
        lambda row: self.build_problematic_fields(row, mask_field_pairs), 
        axis=1
    )
    problematic_df["identified_at"] = datetime.now().isoformat()
    
    return problematic_df
```

#### 3. Field-Level Failure Tracking
```python
@staticmethod
def build_problematic_fields(
    row, 
    mask_field_pairs: List[Tuple]
) -> List[Dict]:
    """
    Identifies which specific fields failed validation for this row.
    
    Returns:
        [
            {"field": "email", "original_value": "user@@example..com"},
            {"field": "gender", "original_value": "X"}
        ]
    """
    fields = []
    idx = row.name  # Row index
    
    for mask, field_name, source_column in mask_field_pairs:
        if mask.loc[idx]:  # This field failed for this row
            fields.append({
                "field": field_name,
                "original_value": row[source_column]
            })
    
    return fields
```

**Why List of Dicts?** A single record can fail multiple validations. This structure preserves:
- Which fields failed
- What the original invalid value was
- Allows JSON serialization for S3 storage

#### 4. Cleaner (Pass 2: Transform)
```python
class Cleaner:
    """Applies cleaning transformations to DataFrames."""
    
    @staticmethod
    def clean_email(email: str) -> str | None:
        """
        Clean email or return None if uncorrectable.
        
        Handles:
        - Multiple @ symbols (take last two parts)
        - Common typos (.om → .com, gmial → gmail)
        - Invalid characters in local part
        """
        if pd.isna(email):
            return None
        
        email = email.strip().lower()
        
        # Fix multiple @ symbols
        if email.count("@") > 1:
            parts = [p for p in email.split("@") if p]
            if len(parts) < 2:
                return None
            local = parts[-2]
            domain = parts[-1]
            email = f"{local}@{domain}"
        
        # Common typos
        corrections = {
            ".om": ".com",
            "gmial": "gmail",
            "hotmaill.com": "hotmail.com",
        }
        for wrong, right in corrections.items():
            email = email.replace(wrong, right)
        
        # Validate structure
        if email.count("@") != 1:
            return None
        
        local, domain = email.split("@", 1)
        if not local or "." not in domain:
            return None
        
        return email
    
    @staticmethod
    def validate_gender(gender: str) -> str | None:
        """Validate gender against allowed values ['M', 'F']."""
        if pd.isna(gender):
            return None
        if gender.lower() not in GENDER:
            return None
        return gender.upper()
```

**Key Difference from Pass 1**: These methods **return cleaned values** (or NULL). They don't preserve originals—that's already done in Pass 1.

---

### Why This Works

#### **1. Separation of Concerns**

| Component | Purpose | Output |
|-----------|---------|--------|
| **DataQualityChecker** | Observe problems | Problematic records with metadata |
| **Cleaner** | Fix problems | Cleaned values or NULL |
| **Transformer** | Orchestrate | Coordinates both passes |

Each class has a single responsibility. Testing is straightforward.

#### **2. Performance Trade-off**

**Cost**: Processing data twice (validation + cleaning) adds latency
**Benefit**: Complete audit trail, field-level granularity, investigation capability

**Decision**: For a compliance-sensitive telecom company, this overhead is justified by:
- Regulatory audit requirements
- Data quality visibility
- Root cause analysis capability

#### **3. Field-Level Granularity**

Traditional approach:
```
 Record rejected: validation failed
```

Double-pass approach:
```json
{
  "customer_id": "C-004",
  "problematic_fields": [
    {"field": "email", "original_value": "user@@bad..com"},
    {"field": "gender", "original_value": "X"}
  ],
  "identified_at": "2024-01-15T10:30:00"
}

