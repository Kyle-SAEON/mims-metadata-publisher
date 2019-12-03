from datetime import datetime
import pandas
import sys
import traceback
import mims_schema_generator
import metadata_publisher

mims_sheet_file='./MIMS.Metadata.Master.Sheet.xlsx'

class RecordParseError(Exception):
    pass

class MIMSExcelImporter:
    _required_columns = \
        ['fileIdentifier', 'DOI', 'date', 'metadataStandardName', 'metadataStandardVersion', \
         'metadataTimestamp', 'accessConstraints', 'descriptiveKeywords', 'title', 'responsibleParties', \
         'responsibleParties.1','responsibleParties.2','keyword', 'topicCategories', 'abstract', 'languages', \
         'formatName', 'spatialRepresentationType', 'spatialResolution', 'referenceSystemName', 'scope', \
         'geographicIdentifier', 'boundingBox', 'verticalElement', 'startTime', 'endTime', 'rights', \
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
                raise Exception("Invalid column found: {}".format(col))
        # parse where necessary
        self.parse_file_identifier(record)
        self.parse_responsible_parties(record, 'responsibleParties')
        self.parse_responsible_parties(record, 'responsibleParties.1',True)
        self.parse_responsible_parties(record, 'responsibleParties.2',True)

        self.parse_column_list(record, 'keyword')
        self.parse_column_list(record, 'topicCategories')
        self.parse_field_to_dict(record,'relatedIdentifiers',
                                       ['relatedIdentifier', 'relatedIdentifierType', 'relationType'])
        self.parse_field_to_dict(record,'onlineResources',
                                       ['name', 'description', 'linkage'])
        self.parse_field_to_dict(record,'referenceSystemName',
                                       ['codeSpace', 'version'])
        self.parse_field_to_dict(record,'descriptiveKeywords',
                                       ['keywordType', 'keyword'])
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



if __name__ == "__main__":
    importer = MIMSExcelImporter()
    imported_records = importer.read_excel_to_json(mims_sheet_file, "CKAN_Geographic")
    converted_records = []

    for record in imported_records:
        schema_generator = mims_schema_generator.MIMSSchemaGenerator()
        schema_generator.set_title(record['title'])
        if type(record['date']) == str:
            date = datetime.strptime(record['date'],"%Y-%m-%d")
            schema_generator.set_date(date)
        elif type(record['date']) == int:
            date = datetime.strptime(str(record['date']),"%Y")
            schema_generator.set_date(date)
        elif type(record['date']) == datetime:
            date = record['date']
            schema_generator.set_date(date)
        else:
            date = datetime.strptime(record['date'],"%Y-%m-%d")
            schema_generator.set_date(date)

        for rparty in record['responsibleParties']:
            contactInfo = "%r" % rparty['contactInfo']
            if contactInfo == "''":
                contactInfo = ''
                #print("Invalid contact info {} {}".format(rparty, record['fileIdentifier']))
                #continue
            if len(rparty['email']) > 0:
                contactInfo = contactInfo + "," + rparty['email']
            schema_generator.add_responsible_party("%r" % rparty['individualName'], rparty['organizationName'], 
                                                   contactInfo, rparty['role'],
                                                   rparty['positionName'])#, online_resource)

        schema_generator.set_geographic_identifier(record['geographicIdentifier'])
        #print((record['boundingBox']))
        schema_generator.set_bounding_box_extent(float(record['boundingBox']['westBoundLongitude']), 
                                                 float(record['boundingBox']['eastBoundLongitude']), 
                                                 float(record['boundingBox']['southBoundLatitude']), 
                                                 float(record['boundingBox']['northBoundLatitude']))

        if (type(record['startTime']) == datetime) and (type(record['endTime']) == datetime):
            schema_generator.set_temporal_extent(record['startTime'], record['endTime'])
        elif (type(record['startTime']) == int) and (type(record['endTime']) == int):
            start_date = datetime.strptime(str(record['startTime']),"%Y")
            end_date = datetime.strptime(str(record['endTime']),"%Y")
            schema_generator.set_temporal_extent(start_date, end_date)
        else:
            print("Invalid start/end-times {} {}".format(record['startTime'], record['endTime']))


        schema_generator.set_languages(record['languages'])
        schema_generator.set_characterset('utf8')

        schema_generator.set_topic_categories(record['topicCategories'])
        spatial_resolution = record['spatialResolution']
        if str(spatial_resolution) == 'nan':
            spatial_resolution = ''
        schema_generator.set_spatial_resolution(spatial_resolution)
        schema_generator.set_abstract("%r" % record['abstract'])

        schema_generator.add_distribution_format(record['formatName'])

        spatial_representation_type = record['spatialRepresentationType']
        if str(spatial_representation_type) == 'nan':
            spatial_representation_type = ''
        schema_generator.set_spatial_representation_type([spatial_representation_type])
        
        schema_generator.set_reference_system_name(record['referenceSystemName']['codeSpace'].replace(' ',''),
                                                   record['referenceSystemName']['version'].replace(' ',''))
        schema_generator.set_lineage_statement("%r" % record['lineageStatement'])
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
            schema_generator.set_metadata_time_stamp(record['metadataTimestamp'].to_pydatetime())
        else:
            print("Invalid metadata timestamp {} - Record id {}".format(record['metadataTimestamp'], 
                  record['fileIdentifier']))
        schema_generator.set_purpose('')
        schema_generator.set_scope(record['scope'])
        schema_generator.set_status(['completed'])
        schema_generator.add_descritive_key_words(record['descriptiveKeywords']['keywordType'].replace(' ',''),
                                                  record['descriptiveKeywords']['keyword'])

        rights_uri = record['rightsURI']
        if str(rights_uri) == 'nan':
            rights_uri = ''
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

    #print("\n\n First record \n\n")
    #for key in converted_records[0].keys():
    #    k,v = key, converted_records[0][key]
    #    print("{} {}".format(k,v))

    print("Attempting to push records")
    for rec in converted_records:
        try:
            print("Pushing record: {}".format(rec['fileIdentifier']))
            metadata_publisher.add_a_record_to_ckan(
                rec,
                'dea',
                'mims-metadata',
                ['mims'],
                'sans-1878-1')
            
        except Exception as e:
            print(e)
            #break
            #pass

    print(len(converted_records))
    #print(len(imported_records))

    #for record in imported_records:
    #    print("*")
        #for auth in record['responsibleParties']:
        #    #print(auth)
        #    for k in auth.keys():
        #        print("%r : %r" %(k, auth[k]))
            
        #print(record['Bounding Box'])
        #print("\n")

