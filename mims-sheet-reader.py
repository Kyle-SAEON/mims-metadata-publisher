import pandas
import sys
import traceback

mims_sheet_file='./MIMS.Metadata.Master.Sheet.xlsx'

class RecordParseError(Exception):
    pass

class MIMSExcelImporter:
    _required_columns = \
        ['ID','AlternateID MIMS full accession', 'DOI', 'Publication Date',\
         'MetaData Standard', 'Metadata date stamp', 'Access', 'Project / Collection',\
         'Title','Cited Authors','Responsible parties (Contributors)','Publisher',\
         'Keywords (free text)','Keywords (Data types CV)','Category','Abstract',\
         'Language','Format','Spatial Representation Type','Spatial Resolution',\
         'Reference System','Resource Type','Location (CV)','Bounding Box',\
         'Vertical Extent','Start Date','End Date','License','Rights URL',\
         'Lineage','Online Resource','Supplementary Links','Related','Notes']

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
                    print("{}    Record id: {}".format(e, raw_record['ID']))                    
        except Exception as e:
            print("Error while reading excel speadsheet. {}".format(e))
            traceback.print_exc(file=sys.stdout)
        return raw_records
    
    def parse_raw_record(self, record):
        for col in record.keys():
            if col not in self._required_columns:
                raise Exception("Invalid column found: {}".format(col))
        # parse where necessary
        self.parse_contact_details(record, 'Cited Authors')
        self.parse_contact_details(record, 'Responsible parties (Contributors)')   
        self.parse_column_list(record, 'Keywords (free text)')
        self.parse_column_list(record, 'Category')
        self.parse_column_list(record, 'Format')
        self.parse_bounding_box(record)

    def parse_contact_details(self, record, contact_type):
        if contact_type not in record:
            raise Exception("Invalid contact type: {}".format(contact_type))
        raw_str = record[contact_type]
        author_details_list = []
        for author_line_str in raw_str.split("\n"):
            if len(author_line_str) > 0:
                # print(author_line_str)
                parts = author_line_str.split("~")
                author_details = {"Organisation":'',"Position":'',"Address":'',"Email":'',"Role":''}

                for part in parts:
                    if ":" in part:                                                
                        try:
                            key, value = part.split(":")
                            author_details[key] = value
                        except:
                            raise RecordParseError("Invalid author details: {}".format(part))

                if len(parts[0]) > 0 and ':' not in parts[0]:
                    author_details["Name"] = parts[0]
                author_details_list.append(author_details)
        record[contact_type] = author_details_list    


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
                        raise
                record['Bounding Box'] = parsed_box
            except:
                raise RecordParseError("Invalid bounding box: {}".format(box_str))
                traceback.print_exc(file=sys.stdout)
            


importer = MIMSExcelImporter()
imported_records = importer.read_excel_to_json(mims_sheet_file, "Geographic metadata")

"""
for record in imported_records:
    print("--")
    for auth in record['Cited Authors']:
        #print(auth)
        for k in auth.keys():
            print("%r : %r" %(k, auth[k]))
        
    print(record['Bounding Box'])
    #print("\n")
"""
