from datetime import datetime
import argparse
import pandas
import sys
import traceback
import mims_schema_generator
import metadata_publisher
import warnings
import pprint

#mims_sheet_file='./mims.spreadsheet.schema.mappings.2019.12.5.xlsx'#'./MIMS.Metadata.Master.Sheet.xlsx'

class RecordParseError(Exception):
    pass

class MIMSExcelImporter:
    _required_columns = \
        ['fileIdentifier', 'DOI', 'date', 'metadataStandardName', 'metadataStandardVersion',
         'metadataTimestamp', 'accessConstraints', 'descriptiveKeywords', 'title', 'responsibleParties',
         'responsibleParties.1','responsibleParties.Publisher','keyword','instrumentKeywords (CV)','status','topicCategories', 'abstract',
         'languages', 'formatName', 'spatialRepresentationType', 'spatialResolution', 'referenceSystemName', 'scope',
         'geographicIdentifier','placeKeywords (CV)', 'boundingBox', 'verticalElement', 'startTime', 'endTime', 'rights',
         'rightsURI', 'lineageStatement', 'onlineResources', 'relatedIdentifiers']
        #['ID','AlternateID MIMS full accession', 'DOI', 'Publication Date',\
        # 'MetaData Standard', 'Metadata date stamp', 'Access', 'Project / Collection',\
        # 'Title','Cited Authors','Responsible parties (Contributors)','Publisher',\
        # 'Keywords (free text)','Keywords (Data types CV)','Category','Abstract',\
        # 'Language','Format','Spatial Representation Type','Spatial Resolution',\
        # 'Reference System','Resource Type','Location (CV)','Bounding Box',\
        # 'Vertical Extent','Start Date','End Date','License','Rights URL',\
        # 'Lineage','Online Resource','Supplementary Links','Related','Notes']

    def __init__(self):
        pass

    def read_excel_to_json(self, spreadsheet_file, sheet):
        raw_record = None
        try:
            df = pandas.read_excel(spreadsheet_file,sheet)
            raw_records = []
            for index, row in df.iterrows():
                try:
                    raw_record = {}
                    col_titles = row.keys()
                    for title in col_titles:
                        raw_record[title] = row[title]
                    self.parse_raw_record(raw_record)
                    raw_records.append(raw_record)
                except RecordParseError as e:
                    print("{}    Record id: {}".format(e, raw_record['fileIdentifier']))
        except Exception as e:
            print("Error while reading excel speadsheet. {}".format(e))
            traceback.print_exc(file=sys.stdout)
        return raw_records

    def parse_raw_record(self, record):
        for col in record.keys():
            if col not in self._required_columns:
                #raise Exception("required column missing: {}".format(col))
                warnings.warn("Extra column found: {}".format(col))

        # parse where necessary
        self.parse_file_identifier(record)
        self.parse_responsible_parties(record, 'responsibleParties')
        self.parse_responsible_parties(record, 'responsibleParties.1',True)
        self.parse_responsible_parties(record, 'responsibleParties.Publisher',True)

        self.parse_column_list(record, 'keyword')
        self.parse_column_list(record, 'topicCategories')
        self.parse_field_to_dict(record,'relatedIdentifiers',
                                       ['relatedIdentifier', 'relatedIdentifierType', 'relationType'])
        self.parse_field_to_dict(record,'onlineResources',
                                       ['name', 'description', 'linkage'])
        self.parse_field_to_dict(record,'referenceSystemName',
                                       ['codeSpace', 'version'])
        self.parse_place_keywords(record,'descriptiveKeywords',['keywordType', 'keyword'],False)
        self.parse_place_keywords(record,'placeKeywords (CV)',['keywordType', 'keyword'],True)
        self.parse_place_keywords(record, 'instrumentKeywords (CV)',['keywordType', 'keyword'],True)
        self.parse_field_to_dict(record,'boundingBox',
                                       ['northBoundLatitude', 'southBoundLatitude',
                                       'eastBoundLongitude', 'westBoundLongitude'],True)

    def parse_file_identifier(self, record):
        if type(record['fileIdentifier']) == float:
            record['fileIdentifier'] = str(int( record['fileIdentifier']))

    def parse_responsible_parties(self, record, field, append_mode=False):
        valid_keys = ['individualName','organizationName','positionName','contactInfo','role','email']
        responsible_parties = []
        try:
            raw_str = record[field]
            for detail_str in raw_str.split("\n"):
                if len(detail_str.replace(" ","")) > 0:
                    detail = {'individualName':'','organizationName':'','positionName':'',
                              'contactInfo':'','role':'','email':''}
                    for item in detail_str.split("|"):
                        if 'email' in item and 'contactInfo' in item:
                            #print(item)
                            parts = item.split(',')
                            email_str = parts[-1]
                            email_k, email_v = email_str.split(":")
                            addr_str = ','.join(parts[0:len(parts) - 1])
                            addr_k, addr_v = addr_str.split(":")
                            email_k = email_k.replace(" ","")
                            addr_k = addr_k.replace(" ","")
                            if email_k not in valid_keys or addr_k not in valid_keys:
                                #print(k)
                                raise RecordParseError("bad field: %r" % item)
                            detail[email_k] = email_v
                            detail[addr_k] = addr_v
                        else:
                            parts = item.split(":")
                            if len(parts) != 2:
                                raise RecordParseError("bad field: %r" % item)
                            k,v = item.split(":")
                            k = k.replace(" ","")
                            if k not in valid_keys:
                                #print(k)
                                raise RecordParseError("bad field: {}".format(item))
                            if k == 'role':
                                v = v.replace(' ','')
                            detail[k] = v.replace(";","")
                    responsible_parties.append(detail)
        except RecordParseError as e:
            raise RecordParseError("Invalid responible party - {}".format(e))
        except Exception as e:
            raise RecordParseError("Invalid responible party - {}".format(item))

        if not append_mode:
            record['responsibleParties'] = responsible_parties
        else:
            record['responsibleParties'] = record['responsibleParties'] + responsible_parties


    def parse_column_list(self, record, column):
        if ',' in record[column]:
            keywords = record[column].split(',')
            record[column] = keywords
        else:
            record[column] = [record[column]]

    def parse_bounding_box(self, record):
        parsed_box = {'North':'','South':'','East':'','West':''}
        box_parts = []
        box_str = record['Bounding Box']
        sep = None
        if ";" in box_str:
            sep = ";"
        elif ',' in box_str:
            sep = ','
        if sep:
            try:
                box_parts = box_str.split(sep)
                for part in box_parts:
                    #print(part)
                    if len(part.replace(" ","")) == 0:
                        continue
                    k,v = part.split(":")
                    k = k.replace(' ','')
                    if k not in parsed_box:
                        raise Exception()
                record['Bounding Box'] = parsed_box
            except:
                traceback.print_exc(file=sys.stdout)
                raise RecordParseError("Invalid bounding box: {}".format(box_str))

    def parse_place_keywords(self, record, field_name, valid_fields, append_mode=False):
        #valid_keys = []
        descriptive_keywords = []
        if str(field_name) == "descriptiveKeywords":
            if str(record[field_name]) == 'nan':
                return
            else:
                detail={'keywordType':'theme','keyword':''}
                raw_str = record[field_name]
                for item in raw_str.split("|"):
                    parts = item.split(':')
                    k, v = item.split(":")
                    k = k.replace(" ", "")
                    # if k not in valid_keys:
                    #     # print(k)
                    #     raise RecordParseError("bad field: {}".format(item))
                    detail[k] = v.replace(";", "")
                descriptive_keywords.append(detail)
                if not append_mode:
                    record['descriptiveKeywords'] = descriptive_keywords
                else:
                    record['descriptiveKeywords'] = record['descriptiveKeywords'] + descriptive_keywords

        elif str(field_name) == "instrumentKeywords (CV)":
            if str(record[field_name]) == 'nan':
                return
            else:
                raw_str = record[field_name]
                for item in raw_str.split(","):
                    detail = {'keywordType': 'stratum', 'keyword': ''}
                    detail['keyword'] = item
                    descriptive_keywords.append(detail)
                if not append_mode:
                    record['descriptiveKeywords'] = descriptive_keywords
                else:
                    record['descriptiveKeywords'] = record['descriptiveKeywords'] + descriptive_keywords

        elif str(field_name) == "placeKeywords (CV)":
            if str(record[field_name]) == 'nan':
                return
            else:
                raw_str = record[field_name]
                for item in raw_str.split(","):
                    detail = {'keywordType': 'place', 'keyword': ''}
                    detail['keyword'] = item
                    descriptive_keywords.append(detail)
                if not append_mode:
                    record['descriptiveKeywords'] = descriptive_keywords
                else:
                    record['descriptiveKeywords'] = record['descriptiveKeywords'] + descriptive_keywords

        else:
            print("Invalid Keywords field")

    def parse_field_to_dict(self, record, field_name, valid_fields,all_fields=False):
        related_ids_str = record[field_name]
        if str(related_ids_str) == "nan":
            record[field_name] = None
            return
        related_ids = {}
        #print(related_ids_str)
        for item in related_ids_str.split("|"):
            parts = item.split(":")
            k = parts[0]
            v = ":".join(parts[1:len(parts)])

            k = k.replace(" ","")
            if len(k) == 0:
                continue
            if k not in valid_fields:
                #print(item)
                raise RecordParseError("Invalid %s field: %r" % (field_name,item))
            related_ids[k] = v
        record[field_name] = related_ids

        if all_fields:
            for field in valid_fields:
                if field not in record[field_name]:
                    raise RecordParseError("Invalid %r format: %r" % (field_name,str(record[field_name])))

    def parse_date(self,record):
        for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%Y'):
            try:
                return datetime.strptime(str(record), fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--excel-file", required=True, help="location of the input Excel file")
    parser.add_argument("--sheet", required=True, help="sheet name inside Excel file to process")
    parser.add_argument("--publish", required=False, help="add publish option to publish records to metadata manager", action="store_true")
    args = parser.parse_args()

    importer = MIMSExcelImporter()
    imported_records = importer.read_excel_to_json(args.excel_file, args.sheet)
    converted_records = []

    for record in imported_records:
        schema_generator = mims_schema_generator.MIMSSchemaGenerator()
        schema_generator.set_title(record['title'])

        if type(record['date']) == str:
            #date = datetime.strptime(record['date'],"%Y-%m-%d")
            date = importer.parse_date(record['date'])
            schema_generator.set_date(date)
        elif type(record['date']) == int:
            date = importer.parse_date(record['date'])
            schema_generator.set_date(date)
        elif type(record['date']) == float:
            date = importer.parse_date(record['date'])
            schema_generator.set_date(date)
        elif type(record['date']) == datetime:
            date = record['date']
            schema_generator.set_date(date)
        else:
            date = importer.parse_date(record['date'])
            schema_generator.set_date(date)

        for rparty in record['responsibleParties']:
            contactInfo = "%r" % rparty['contactInfo']
            #contactInfo = rparty['contactInfo']
            if contactInfo == "''":
                contactInfo = ''
                #print("Invalid contact info {} {}".format(rparty, record['fileIdentifier']))
                #continue
            if len(rparty['email']) > 0:
                contactInfo = contactInfo + "," + rparty['email']

            role_fixes = {'':'','resourceprovider':'resourceProvider', 'custodian':'custodian', 'owner':'owner',
                          'user':'user', 'distributor':'distributor', 'originator':'originator',
                          'pointofcontact':'pointOfContact', 'principleinvestigator':'principalInvestigator',
                          'principalinvestigator':'principalInvestigator','processor':'processor', 'publisher':'publisher'}

            schema_generator.add_responsible_party("%r" % rparty['individualName'], rparty['organizationName'],
                                                   contactInfo, role_fixes[rparty['role'].lower()],
                                                   rparty['positionName'])#, online_resource)

        schema_generator.set_geographic_identifier(record['geographicIdentifier'])
        #print((record['boundingBox']))
        schema_generator.set_bounding_box_extent(float(record['boundingBox']['westBoundLongitude']),
                                                 float(record['boundingBox']['eastBoundLongitude']),
                                                 float(record['boundingBox']['southBoundLatitude']),
                                                 float(record['boundingBox']['northBoundLatitude']))

        def convert_date(date_input):
            def convert_str_to_date(date_str, str_format):
                formatted_date = None
                try:
                    #print("trying to convert {} to {}".format(date_str, str_format))
                    formatted_date = datetime.strptime(date_input,str_format)
                    #print("result {}".format(formatted_date))
                except:
                    #print("Could not convert {} to {}".format(date_str, str_format))
                    pass
                return formatted_date
            if type(date_input) == int:
                return datetime.strptime(str(date_input),"%Y")
            elif type(date_input) == str:
                supported_formats = ["%Y/%m/%d %H:%M","%Y-%m-%d %H:%M:%S","%Y-%m-%d"] #2015/03/12 12:00
                converted_date = None
                for fmt in supported_formats:
                    converted_date = convert_str_to_date(date_input, fmt)
                return converted_date

        start_time = record['startTime']
        end_time = record['endTime']

        if (type(start_time) == datetime) and (type(end_time) == datetime):
            schema_generator.set_temporal_extent(start_time, end_time)
        else:
            if type(start_time) != datetime:
                start_time = convert_date(start_time)
            if type(end_time) != datetime:
                end_time = convert_date(end_time)
            if start_time != None and end_time != None:
                schema_generator.set_temporal_extent(start_time, end_time)
            else:
                print("Error! could not convert start/end times: {}/{}".format(start_time, end_time))

        schema_generator.set_languages(record['languages'])
        schema_generator.set_characterset('utf8')

        schema_generator.set_topic_categories(record['topicCategories'])
        spatial_resolution = record['spatialResolution']
        if str(spatial_resolution) == 'nan':
            spatial_resolution = ''
        schema_generator.set_spatial_resolution(spatial_resolution)
        schema_generator.set_abstract("%r" % record['abstract'])
        #schema_generator.set_abstract(record['abstract'].encode('ascii','ignore'))
        format_name = record['formatName']
        if str(format_name) == 'nan':
            format_name = ''
        schema_generator.add_distribution_format(format_name)

        spatial_representation_type = record['spatialRepresentationType']
        if str(spatial_representation_type) == 'nan':
            spatial_representation_type = ''
        rep_type_fixes = {'':'','vector':'vector', 'grid':'grid', \
                          'texttable':'textTable', 'tin':'tin', 'stereomodel':'stereoModel', \
                          'video':'video', 'image':'image'}
        schema_generator.set_spatial_representation_type([rep_type_fixes[spatial_representation_type.lower()]])

        schema_generator.set_reference_system_name(record['referenceSystemName']['codeSpace'].replace(' ',''),
                                                   record['referenceSystemName']['version'].replace(' ',''))
        lineage_statement = record['lineageStatement']
        if str(lineage_statement) == 'nan':
            lineage_statement = ''
        schema_generator.set_lineage_statement(lineage_statement)

        online_resources = record['onlineResources']
        if str(online_resources) == 'None':
            pass
        else:
            schema_generator.add_online_resources(record['onlineResources']['name'],
                                              record['onlineResources']['description'].replace(' ',''),
                                              record['onlineResources']['linkage'].replace(' ',''))  #name, description, link)
        schema_generator.set_file_identifier(record['fileIdentifier'])
        schema_generator.set_metadata_standard_name(record['metadataStandardName'])
        schema_generator.set_metadata_standard_version(str(record['metadataStandardVersion']))
        schema_generator.set_metadata_language("en")
        schema_generator.set_metadata_characterset('utf8')
        #date = datetime.strptime(record['metadataTimestamp'],"%Y-%m-%d")
        if (str(record['metadataTimestamp'])) != "NaT":
            schema_generator.set_metadata_time_stamp(datetime.strptime((record['metadataTimestamp']),"%Y-%m-%d"))
        else:
            print("Invalid metadata timestamp {} - Record id {}".format(record['metadataTimestamp'],
                  record['fileIdentifier']))
        schema_generator.set_purpose('')
        schema_generator.set_scope(record['scope'])
        schema_generator.set_status([record['status']])
        for word in record['descriptiveKeywords']:
            schema_generator.add_descriptive_key_words(word['keywordType'].replace(' ',''),word['keyword'])

        rights_uri = record['rights']
        if str(rights_uri) == 'restricted':
            rights_uri = 'restricted'
        schema_generator.set_constraints(record['rights'], rights_uri, record['accessConstraints'])



        if record['relatedIdentifiers']:
            def remove_ri_errors(input):
                input = input.replace(' ','').replace(';','').replace('\n','')
                return input
            identifier = remove_ri_errors(record['relatedIdentifiers']['relatedIdentifier'])
            id_type = remove_ri_errors(record['relatedIdentifiers']['relatedIdentifierType'])
            relation_type = remove_ri_errors(record['relatedIdentifiers']['relationType'])
            schema_generator.set_related_identifiers(identifier,
                                                    id_type,
                                                    relation_type)


        converted_records.append(schema_generator.get_filled_schema())

    #pprint.pprint(converted_records)

    if args.publish:
        print("Attempting to push records")
        for rec in converted_records:
            try:
                print("Pushing record: {}".format(rec['fileIdentifier']))
                metadata_publisher.add_a_record_to_ckan(rec,'dea','sadco-test','sans-1878-1')
            except Exception as e:
                print(e)
        print(len(converted_records))


