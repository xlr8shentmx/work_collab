# DEBUG: Inspect Salesforce Data Before Loading
# Run this cell to diagnose the 253003 error

print("=" * 80)
print("SALESFORCE DATA INSPECTION")
print("=" * 80)

# Extract Salesforce data
df_sf = extract_salesforce(SALESFORCE_EXPORT_PATH)

print(f"\n1. DataFrame Info:")
print(f"   Rows: {len(df_sf)}")
print(f"   Columns: {list(df_sf.columns)}")

print(f"\n2. Data Types:")
print(df_sf.dtypes)

print(f"\n3. Sample Data:")
print(df_sf.head())

print(f"\n4. Null Value Counts:")
print(df_sf.isnull().sum())

print(f"\n5. After Date Normalization:")
df_sf_norm, date_cols = normalize_dates(df_sf.copy())
print(f"   Date columns detected: {date_cols}")
print(f"   Data types after normalization:")
print(df_sf_norm.dtypes)

print(f"\n6. Column Type Classification:")
for col in df_sf_norm.columns:
    if col in date_cols:
        print(f"   {col}: DATE")
    elif pd.api.types.is_integer_dtype(df_sf_norm[col]):
        print(f"   {col}: NUMBER(38,0)")
    elif pd.api.types.is_numeric_dtype(df_sf_norm[col]):
        print(f"   {col}: NUMBER(38,6)")
    else:
        print(f"   {col}: VARCHAR")

print("\n" + "=" * 80)
