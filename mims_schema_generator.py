from datetime import datetime
import mims_excel_importer


class MIMSSchemaFormatError(Exception):
    pass


class MIMSSchemaGenerator:
    def __init__(self):
        self.record = {}
        self.record["responsibleParties"] = []
        self.record["extent"] = {
            "geographicElements": [{"geographicIdentifier": None,
                                    "boundingBox": None,
                                    "boundingPolygon": []}],
            "verticalElement": {},
            "temporalElement": {}
        }
        self.record["distributionFormats"] = []
        self.record["descriptiveKeywords"] = []
        self.record["onlineResourceDescription"] = []

    def set_title(self, title):
        self.record["title"] = title
        # "title": "Example MIMS Metadata Record",

    def set_date(self, date):
        if type(date) != datetime:
            raise MIMSSchemaFormatError("Invalid date type, must be datetime")
        # self.record["date"] = "{}-{}-{}".format(date.year, date.month, date.day)
        self.record["date"] = date.strftime("%Y-%2m-%2d")
        # "date": "2019-11-02",

    def add_responsible_party(self, name='', organization='', contact_info='', role='', position_name='',
                              online_resource=None):
        responsible_party = {
            "individualName": name,
            "organizationName": organization,
            "contactInfo": contact_info,
            'positionName': position_name,
            'role': role,
        }

        if online_resource:
            link = {"linkage": online_resource}
            responsible_party['onlineResource'] = link

        self.record["responsibleParties"].append(responsible_party)

    def set_geographic_identifier(self, identifier):
        self.record["extent"]["geographicElements"][0]["geographicIdentifier"] = identifier

    def set_bounding_box_extent(self, west_bound_longitude, east_bound_longitude, south_bound_latitude,
                                north_bound_latitude):
        bounding_box = {
            "westBoundLongitude": west_bound_longitude,
            "eastBoundLongitude": east_bound_longitude,
            "southBoundLatitude": south_bound_latitude,
            "northBoundLatitude": north_bound_latitude
        }
        self.record["extent"]["geographicElements"][0]["boundingBox"] = bounding_box

    def add_bounding_polygon(polygon):
        if type(polygon) != list or len(polygon) < 5:
            raise MIMSSchemaFormatError("Invalid polygon type, must be a list with 5 elements")

        valid_keys = ['longitude', 'latitude']
        for point in polygon:
            if type(point) != dict:
                raise MIMSSchemaFormatError("Invalid polygon element, must be a dict")
            for k in point.keys():
                if k not in valid_keys:
                    raise MIMSSchemaFormatError("Invalid polygon key name, must be 'longitude' or 'latitude'")

        self.record["extent"]["geographicElements"][0]["boundingPolygon"].append(polygon)

    def set_vertical_extent(self, minimum_value, maximum_value, unit_of_measure, vertical_datum):
        vertical_extent = {
            "minimumValue": minimum_value,
            "maximumValue": maximum_value,
            "unitOfMeasure": unit_of_measure,
            "verticalDatum": vertical_datum
        }
        self.record["extent"]["verticalElement"] = vertical_extent

    def set_temporal_extent(self, start_time, end_time):
        if type(start_time) != datetime or type(end_time) != datetime:
            raise MIMSSchemaFormatError("Invalid start/end time type, must be a datetime")
        format = "%Y-%2m-%2dT%H:%M:%S"
        start_time_str = start_time.strftime(format) + "+02:00"
        end_time_str = end_time.strftime(format) + "+02:00"
        temporal_extent = {
            "startTime": start_time_str,
            "endTime": end_time_str
        }
        self.record["extent"]["temporalElement"] = temporal_extent

    def set_languages(self, language):
        self.record["languages"] = [language]

    def set_characterset(self, characterset):
        self.record["characterSet"] = characterset

    def set_topic_categories(self, categories):
        if type(categories) != list:
            raise MIMSSchemaFormatError("Invalid categories type, must be a list")
        self.record["topicCategories"] = categories

    def set_spatial_resolution(self, resolution):
        self.record["spatialResolution"] = resolution

    def set_abstract(self, abstract):
        self.record["abstract"] = abstract

    def add_distribution_format(self, format_name, format_version=None):
        format = {"formatName": format_name}
        # if format_version != list:
        #     raise MIMSSchemaFormatError("Invalid distribution format type, must be a list")
        #format["formatVersion"] = format_version
        self.record["distributionFormats"].append(format)

    def set_spatial_representation_type(self, represenation):
        if type(represenation) != list:
            raise MIMSSchemaFormatError("Invalid spatial representation type, must be a list")
        self.record["spatialRepresentationTypes"] = represenation

    def set_reference_system_name(self, codespace, version):
        self.record["referenceSystemName"] = {"codeSpace": codespace, "version": version}

    def set_lineage_statement(self, lineage):
        self.record["lineageStatement"] = lineage

    def add_online_resources(self, name, description, link):
        self.record["onlineResources"].append({
            "name": name,
            "description": description,
            "linkage": link})

    def set_file_identifier(self, file_identifier):
        if type(file_identifier) != str:
            raise MIMSSchemaFormatError("Invalid file_identifier, must be a string")
        self.record["fileIdentifier"] = file_identifier

    def set_metadata_standard_name(self, metadata_standard):
        self.record["metadataStandardName"] = metadata_standard

    def set_metadata_standard_version(self, standard_version):
        self.record["metadataStandardVersion"] = standard_version

    def set_metadata_language(self, language):
        self.record["metadataLanguage"] = language

    def set_metadata_characterset(self, characterset):
        self.record["metadataCharacterSet"] = characterset

    def set_metadata_time_stamp(self, timestamp):
        if type(timestamp) != datetime:
            raise MIMSSchemaFormatError("Invalid metadata timestamp, must be datetime")
        format = "%Y-%m-%dT%H:%M:%S"
        # format="%Y-%m-%d %H:%M:%S"
        timestamp_str = timestamp.strftime(format) + "+02:00"
        self.record["metadataTimestamp"] = timestamp_str

    def set_purpose(self, purpose):
        self.record["purpose"] = purpose

    def set_scope(self, scope):
        self.record["scope"] = scope

    def set_status(self, status):
        if type(status) != list:
            raise MIMSSchemaFormatError("Invalid status type, must be a list")
        self.record["status"] = status

    def add_descriptive_key_words(self, keyword_type, keyword):
        self.record["descriptiveKeywords"].append({
            "keywordType": keyword_type,
            "keyword": keyword
        })

    def set_constraints(self, rights, rights_uri, access_constraints, use_constraints='', classification='',
                        use_limitations=''):
        if use_constraints != '' and type(use_constraints) != list:
            raise MIMSSchemaFormatError("Invalid use_constraints type, must be a list")
        self.record["constraints"] = [{
            "rights": rights,
            "rightsURI": rights_uri,
            "useLimitations": [use_limitations],
            "accessConstraints": [access_constraints],
            "useConstraints": use_constraints,
            "classification": classification}]

    def set_related_identifiers(self, identifier, id_type, relation_type):
        self.record["relatedIdentifiers"] = [{
            "relatedIdentifier": identifier,
            "relatedIdentifierType": id_type,
            "relationType": relation_type
        }]

    def get_filled_schema(self):
        return self.record


if __name__ == "__main__":
    mims_sheet_file = './MIMS.Metadata.Master.Sheet.xlsx'
    importer = mims_excel_importer.MIMSExcelImporter()
    imported_records = importer.read_excel_to_json(mims_sheet_file, "CKAN_Geographic")
    schema_generator = MIMSSchemaGenerator()
