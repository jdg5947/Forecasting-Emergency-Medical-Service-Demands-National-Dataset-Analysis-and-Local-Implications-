import pandas as pd
import numpy as np
from collections import defaultdict
import os

# eDispatch_01 Code Dictionary (NEMSIS v3.5.1.251001CP2)
edispatch_01_dict = {
    "2301001": "Abdominal Pain/Problems",
    "2301003": "Allergic Reaction/Stings",
    "2301005": "Animal Bite",
    "2301007": "Assault",
    "2301009": "Automated Crash Notification",
    "2301011": "Back Pain (Non-Traumatic)",
    "2301013": "Breathing Problem",
    "2301015": "Burns/Explosion",
    "2301017": "Carbon Monoxide/Hazmat/Inhalation/CBRN",
    "2301019": "Cardiac Arrest/Death",
    "2301021": "Chest Pain (Non-Traumatic)",
    "2301023": "Choking",
    "2301025": "Convulsions/Seizure",
    "2301027": "Diabetic Problem",
    "2301029": "Electrocution/Lightning",
    "2301031": "Eye Problem/Injury",
    "2301033": "Falls",
    "2301035": "Fire",
    "2301037": "Headache",
    "2301039": "Healthcare Professional/Admission",
    "2301041": "Heart Problems/AICD",
    "2301043": "Heat/Cold Exposure",
    "2301045": "Hemorrhage/Laceration",
    "2301047": "Industrial Accident/Inaccessible Incident/Other Entrapments (Non-Vehicle)",
    "2301049": "Medical Alarm",
    "2301051": "No Other Appropriate Choice",
    "2301053": "Overdose/Poisoning/Ingestion",
    "2301055": "Pandemic/Epidemic/Outbreak",
    "2301057": "Pregnancy/Childbirth/Miscarriage",
    "2301059": "Psychiatric Problem/Abnormal Behavior/Suicide Attempt",
    "2301061": "Sick Person",
    "2301063": "Stab/Gunshot Wound/Penetrating Trauma",
    "2301065": "Standby",
    "2301067": "Stroke/CVA",
    "2301069": "Traffic/Transportation Incident",
    "2301071": "Transfer/Interfacility/Palliative Care",
    "2301073": "Traumatic Injury",
    "2301075": "Well Person Check",
    "2301077": "Unconscious/Fainting/Near-Fainting",
    "2301079": "Unknown Problem/Person Down",
    "2301081": "Drowning/Diving/SCUBA Accident",
    "2301083": "Airmedical Transport",
    "2301085": "Altered Mental Status",
    "2301087": "Intercept",
    "2301089": "Nausea",
    "2301091": "Vomiting",
    "2301093": "Hanging/Strangulation/Asphyxiation",
    "2301095": "Intoxicated Subject",
    "2301097": "EMS Requested"
}

# eDispatch_02 Code Dictionary (EMD Performed)
edispatch_02_dict = {
    "2302001": "No",
    "2302003": "Yes, With Pre-Arrival Instructions",
    "2302005": "Yes, Without Pre-Arrival Instructions",
    "2302007": "Yes, Unknown if Pre-Arrival Instructions Given"
}

# File paths
file1 = r'D:\COMP 594\ASCII_64 2024\Pub_PCRevents.txt'
file2 = r'D:\COMP 594\ASCII_64 2024\ComputedElements.txt'
file3 = r'D:\COMP 594\ASCII_64 2024\FACTPCRTIME.txt'
output_file = r''
chunk_size = 1000000

# Columns to keep
cols1 = ["'PcrKey'", "'eDispatch_01'", "'eDispatch_02'"]
cols2 = ["'PcrKey'", "'USCensusRegion'", "'USCensusDivision'", "'NasemsoRegion'", "'Urbanicity'"]
cols3 = ["'PcrKey'", "'eTimes_01'", "'eTimes_03'"]

print("=" * 80)
print("OPTIMIZED EMS DATA MERGER WITH CSV EXPORT")
print("Processing aligned files - PcrKeys in same order across all files")
print("=" * 80)

# Check if file exists and remove it
if os.path.exists(output_file):
    os.remove(output_file)
    print("Removed existing output file\n")

# Initialize statistics counters
dispatch_counts = defaultdict(int)
region_counts = defaultdict(int)
emd_counts = defaultdict(int)
total_records = 0
records_written = 0

print("Processing and merging files in parallel chunks...")
print("-" * 80)

# Create iterators for all three files
iter1 = pd.read_csv(file1, sep=r"~\|~", engine="python", dtype=str,
                    usecols=cols1, chunksize=chunk_size, quotechar="'")
iter2 = pd.read_csv(file2, sep=r"~\|~", engine="python", dtype=str,
                    usecols=cols2, chunksize=chunk_size, quotechar="'")
iter3 = pd.read_csv(file3, sep=r"~\|~", engine="python", dtype=str,
                    usecols=cols3, chunksize=chunk_size, quotechar="'")

chunk_num = 0

# Process all three files simultaneously
for chunk1, chunk2, chunk3 in zip(iter1, iter2, iter3):
    chunk_num += 1

    # Clean column names and data for all chunks
    for chunk in [chunk1, chunk2, chunk3]:
        chunk.columns = [c.strip().strip("'") for c in chunk.columns]
        for col in chunk.columns:
            if chunk[col].dtype == 'object':
                chunk[col] = chunk[col].str.strip()

    # Verify PcrKeys match (safety check)
    if not chunk1['PcrKey'].equals(chunk2['PcrKey']) or not chunk1['PcrKey'].equals(chunk3['PcrKey']):
        print(f"WARNING: PcrKeys don't match in chunk {chunk_num}!")
        # If they don't match, we can still merge on PcrKey
        merged = chunk1.merge(chunk2, on='PcrKey').merge(chunk3, on='PcrKey')
    else:
        # Direct concatenation since PcrKeys are aligned
        merged = pd.concat([
            chunk1,
            chunk2.drop('PcrKey', axis=1),
            chunk3.drop('PcrKey', axis=1)
        ], axis=1)

    # Map dispatch codes to descriptions
    merged['eDispatch_01_Description'] = merged['eDispatch_01'].map(edispatch_01_dict)
    merged['eDispatch_02_Description'] = merged['eDispatch_02'].map(edispatch_02_dict)

    # Rename columns for clarity
    merged = merged.rename(columns={
        'eDispatch_01': 'eDispatch_01_Code',
        'eDispatch_02': 'eDispatch_02_Code',
        'eTimes_01': 'PSAP_Call_DateTime',
        'eTimes_03': 'Unit_Notified_DateTime'
    })

    # Reorder columns
    merged = merged[[
        'PcrKey',
        'eDispatch_01_Code',
        'eDispatch_01_Description',
        'eDispatch_02_Code',
        'eDispatch_02_Description',
        'USCensusRegion',
        'USCensusDivision',
        'NasemsoRegion',
        'Urbanicity',
        'PSAP_Call_DateTime',
        'Unit_Notified_DateTime'
    ]]

    # Update statistics
    dispatch_counts_chunk = merged['eDispatch_01_Code'].value_counts()
    for code, count in dispatch_counts_chunk.items():
        if pd.notna(code):
            dispatch_counts[code] += count

    region_counts_chunk = merged['USCensusRegion'].value_counts()
    for region, count in region_counts_chunk.items():
        if pd.notna(region):
            region_counts[region] += count

    emd_counts_chunk = merged['eDispatch_02_Code'].value_counts()
    for code, count in emd_counts_chunk.items():
        if pd.notna(code):
            emd_counts[code] += count

    # Write to CSV
    if records_written == 0:
        merged.to_csv(output_file, index=False, mode='w')
    else:
        merged.to_csv(output_file, index=False, mode='a', header=False)

    records_written += len(merged)
    total_records += len(merged)
    print(f"Chunk {chunk_num}: Processed and wrote {len(merged):,} records (Total: {records_written:,})")

print(f"\n✓ Processing complete! Total records written: {records_written:,}")

# Display summary statistics
print("\n" + "=" * 80)
print("EXPORT SUMMARY")
print("=" * 80)

print(f"\nTotal Records Exported: {records_written:,}")
print(f"Output File: {output_file}")

# Get file size
file_size_bytes = os.path.getsize(output_file)
file_size_mb = file_size_bytes / (1024 * 1024)
file_size_gb = file_size_bytes / (1024 * 1024 * 1024)

if file_size_gb >= 1:
    print(f"File Size: {file_size_gb:.2f} GB")
else:
    print(f"File Size: {file_size_mb:.2f} MB")

print("\nColumns in exported file:")
print("  1. PcrKey")
print("  2. eDispatch_01_Code")
print("  3. eDispatch_01_Description")
print("  4. eDispatch_02_Code")
print("  5. eDispatch_02_Description")
print("  6. USCensusRegion")
print("  7. USCensusDivision")
print("  8. NasemsoRegion")
print("  9. Urbanicity")
print("  10. PSAP_Call_DateTime")
print("  11. Unit_Notified_DateTime")

# Display top dispatch codes
print("\n" + "=" * 80)
print("TOP 10 DISPATCH CODES IN EXPORTED DATA")
print("=" * 80)
sorted_dispatch = sorted(dispatch_counts.items(), key=lambda x: x[1], reverse=True)[:10]
for code, count in sorted_dispatch:
    description = edispatch_01_dict.get(code, "Unknown Code")
    pct = (count / records_written) * 100
    print(f"{code}: {description}")
    print(f"  Count: {count:,} ({pct:.2f}%)\n")

# Display EMD distribution
print("=" * 80)
print("EMD PERFORMED DISTRIBUTION")
print("=" * 80)
sorted_emd = sorted(emd_counts.items(), key=lambda x: x[1], reverse=True)
for code, count in sorted_emd:
    description = edispatch_02_dict.get(code, "Unknown Code")
    pct = (count / records_written) * 100
    print(f"{code}: {description}")
    print(f"  Count: {count:,} ({pct:.2f}%)\n")

# Display top regions
print("=" * 80)
print("TOP CENSUS REGIONS IN EXPORTED DATA")
print("=" * 80)
sorted_regions = sorted(region_counts.items(), key=lambda x: x[1], reverse=True)
for region, count in sorted_regions:
    pct = (count / records_written) * 100
    print(f"  {region}: {count:,} ({pct:.2f}%)")

print("\n" + "=" * 80)
print("CSV export complete! You can now use this file for further analysis.")
print("=" * 80)

# Display sample of how to load the data
print("\nTo load this data in future analyses, use:")
print(f"df = pd.read_csv('{output_file}')")
print("\nOr for large files, use chunking:")
print(f"for chunk in pd.read_csv('{output_file}', chunksize=100000):")
print("    # process chunk")