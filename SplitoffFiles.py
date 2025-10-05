import pandas as pd
import numpy as np
from collections import defaultdict
import os

# File paths
input_file = r'D:\COMP 594\merged_ems_data.csv'
output_dir = r'D:\COMP 594'

print("=" * 80)
print("EMS DATA REGIONAL-URBANICITY BREAKDOWN ANALYSIS")
print("Creating separate datasets by NASEMSO Region and Urbanicity pairs")
print("=" * 80)

# Verify input file exists
if not os.path.exists(input_file):
    print(f"\nERROR: Input file not found: {input_file}")
    exit(1)

print(f"\nInput file: {input_file}")
print(f"Output directory: {output_dir}")

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Initialize tracking
chunk_size = 500000
region_urbanicity_counts = defaultdict(int)
region_urbanicity_files = {}
total_records = 0
chunk_num = 0

print("\n" + "-" * 80)
print("Phase 1: Identifying all Region-Urbanicity pairs...")
print("-" * 80)

# First pass: identify all unique combinations
for chunk in pd.read_csv(input_file, chunksize=chunk_size, dtype=str):
    chunk_num += 1
    total_records += len(chunk)

    # Count combinations
    for _, row in chunk.iterrows():
        nasemso = row['NasemsoRegion'] if pd.notna(row['NasemsoRegion']) and row['NasemsoRegion'].strip() else 'Unknown'
        urbanicity = row['Urbanicity'] if pd.notna(row['Urbanicity']) and row['Urbanicity'].strip() else 'Unknown'

        key = (nasemso, urbanicity)
        region_urbanicity_counts[key] += 1

    if chunk_num % 5 == 0:
        print(f"   Processed {total_records:,} records...")

print(f"\n✓ Scanned {total_records:,} total records")
print(f"✓ Found {len(region_urbanicity_counts)} unique Region-Urbanicity pairs")

# Display combinations
print("\n" + "=" * 80)
print("REGION-URBANICITY COMBINATIONS FOUND")
print("=" * 80)

sorted_combinations = sorted(region_urbanicity_counts.items(), key=lambda x: x[1], reverse=True)
for (region, urbanicity), count in sorted_combinations:
    pct = (count / total_records) * 100
    print(f"{region} / {urbanicity}: {count:,} records ({pct:.2f}%)")

# Create file handles for each combination
print("\n" + "-" * 80)
print("Phase 2: Creating output files and splitting data...")
print("-" * 80)

for region, urbanicity in region_urbanicity_counts.keys():
    # Create safe filename
    region_clean = region.replace('/', '-').replace(' ', '_').replace('\\', '-')
    urbanicity_clean = urbanicity.replace('/', '-').replace(' ', '_').replace('\\', '-')

    filename = f"{region_clean}_{urbanicity_clean}.csv"
    filepath = os.path.join(output_dir, filename)

    region_urbanicity_files[(region, urbanicity)] = {
        'filepath': filepath,
        'filename': filename,
        'written': 0
    }

print(f"Created {len(region_urbanicity_files)} output file paths")

# Second pass: write data to appropriate files
print("\nWriting records to regional-urbanicity files...")
chunk_num = 0
records_processed = 0

for chunk in pd.read_csv(input_file, chunksize=chunk_size, dtype=str):
    chunk_num += 1

    # Group chunk by region-urbanicity combination
    chunk['NasemsoRegion_clean'] = chunk['NasemsoRegion'].apply(
        lambda x: x if pd.notna(x) and str(x).strip() else 'Unknown'
    )
    chunk['Urbanicity_clean'] = chunk['Urbanicity'].apply(
        lambda x: x if pd.notna(x) and str(x).strip() else 'Unknown'
    )

    # Group and write
    grouped = chunk.groupby(['NasemsoRegion_clean', 'Urbanicity_clean'])

    for (region, urbanicity), group_df in grouped:
        key = (region, urbanicity)
        file_info = region_urbanicity_files[key]

        # Drop the temporary columns
        group_df = group_df.drop(['NasemsoRegion_clean', 'Urbanicity_clean'], axis=1)

        # Write with header only on first write
        if file_info['written'] == 0:
            group_df.to_csv(file_info['filepath'], index=False, mode='w')
        else:
            group_df.to_csv(file_info['filepath'], index=False, mode='a', header=False)

        file_info['written'] += len(group_df)

    records_processed += len(chunk)
    if chunk_num % 5 == 0:
        print(f"   Processed {records_processed:,} records...")

print(f"\n✓ Completed writing all {records_processed:,} records")

# Generate summary report
print("\n" + "=" * 80)
print("FILE CREATION SUMMARY")
print("=" * 80)

summary_data = []

for (region, urbanicity), file_info in sorted(region_urbanicity_files.items(),
                                              key=lambda x: x[1]['written'],
                                              reverse=True):
    filepath = file_info['filepath']
    record_count = file_info['written']

    if os.path.exists(filepath):
        file_size_bytes = os.path.getsize(filepath)
        file_size_mb = file_size_bytes / (1024 * 1024)
    else:
        file_size_mb = 0

    pct = (record_count / total_records) * 100

    summary_data.append({
        'Region': region,
        'Urbanicity': urbanicity,
        'Filename': file_info['filename'],
        'Records': record_count,
        'Percentage': pct,
        'Size_MB': file_size_mb
    })

    print(f"\n{region} / {urbanicity}")
    print(f"  File: {file_info['filename']}")
    print(f"  Records: {record_count:,} ({pct:.2f}%)")
    print(f"  Size: {file_size_mb:.2f} MB")

# Create summary CSV
summary_df = pd.DataFrame(summary_data)
summary_file = os.path.join(output_dir, 'region_urbanicity_summary.csv')
summary_df.to_csv(summary_file, index=False)

print("\n" + "=" * 80)
print("SUMMARY STATISTICS")
print("=" * 80)

print(f"\nTotal files created: {len(region_urbanicity_files)}")
print(f"Total records distributed: {sum(f['written'] for f in region_urbanicity_files.values()):,}")
print(f"Summary report saved to: {summary_file}")

# Calculate totals
total_size_mb = sum(s['Size_MB'] for s in summary_data)
total_size_gb = total_size_mb / 1024

print(f"\nTotal storage used: {total_size_gb:.2f} GB ({total_size_mb:.2f} MB)")

# Top 5 largest files
print("\n" + "=" * 80)
print("TOP 5 LARGEST FILES")
print("=" * 80)

top_5 = sorted(summary_data, key=lambda x: x['Records'], reverse=True)[:5]
for i, item in enumerate(top_5, 1):
    print(f"\n{i}. {item['Region']} / {item['Urbanicity']}")
    print(f"   {item['Filename']}")
    print(f"   {item['Records']:,} records ({item['Percentage']:.2f}%)")
    print(f"   {item['Size_MB']:.2f} MB")

print("\n" + "=" * 80)
print("BREAKDOWN COMPLETE!")
print("=" * 80)

print("\nAll files are located in:", output_dir)
print("\nTo load a specific regional file:")
print("df = pd.read_csv(r'D:\\COMP 594\\{filename}')")
print("\nTo load summary:")
print(f"summary = pd.read_csv(r'{summary_file}')")