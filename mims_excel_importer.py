import pandas
import sys
import traceback

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
                                       'eastBoundLongitude', 'westBoundLongitude'])

    def parse_responsible_parties(self, record, field, append_mode=False):
        valid_keys = ['individualName','organizationName','positionName','contactInfo','role','email']
        responsible_parties = []
        try:
            raw_str = record[field]
            for detail_str in raw_str.split("\n"):
                if len(detail_str.replace(" ","")) > 0:
                    detail = {}
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

    def parse_field_to_dict(self, record, field_name, valid_fields):
        related_ids_str = record[field_name]
        if str(related_ids_str) == "nan":
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


if __name__ == "__main__":
    importer = MIMSExcelImporter()
    imported_records = importer.read_excel_to_json(mims_sheet_file, "CKAN_Geographic")


    #print(len(imported_records))

    #for record in imported_records:
    #    print("*")
        #for auth in record['responsibleParties']:
        #    #print(auth)
        #    for k in auth.keys():
        #        print("%r : %r" %(k, auth[k]))
            
        #print(record['Bounding Box'])
        #print("\n")

