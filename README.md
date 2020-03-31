Script based tools for publishing MIMS file based metadata to the SAEON Metadata manager

# Dependencies:
pandas

# To run
<base_dir>$ python mims_excel_importer.py --excel-file mims.spreadsheet.schema.mappings.2019.12.5.xlsx --sheet CKAN_Geographic --publish

@Note: only include the --publish flag if you intend to publish the records through to the metadata manager

# Configuration
Set both ckan_base_url and odp_ckan_api_key in metadata_publisher.py


