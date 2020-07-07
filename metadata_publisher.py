import json
import requests
import logging
import time

from requests.models import Response

#ckan_base_url = 'https://odp.saeon.dvn/api' #dev
ckan_base_url = 'https://odp.saeon.stg/api' #staging

#odp_ckan_api_key = 'Roj8GL6UCwoNNgAkM2pceUSLFVXSMq38pgnahxthD-s.uP1kUSkwGIcBJ9DKv7ztmKiGDgOWB3SkThmmJBGHSUM' #dev
odp_ckan_api_key = '0s5vmAmk9QNOi1Q5mYAseLGGrhTQxbVzQsPIair6NfU.h1Z72joBMO_XIPJxPA1BkCge66e7xXaTbiqaUlQ-MUg' #staging
method='POST'

logging.basicConfig(level=logging.INFO)

UPDATE_METRICS = {
    'update_count':0,
    'records_added':0,
    'validated_successfully':0,
    'validation_errors': 0,
    'published':0,
    'unpublished':0,
}

def add_a_record_to_ckan(metadata_record, institution, collection, metadata_standard):
    #url = "{}/metadata/".format(ckan_base_url)
    metadata_record.pop("hierarchy")
    print("Trying to add record into {}".format(institution))
    record_data = {
        'collection_key': collection,
        'schema_key':metadata_standard,
        'metadata': metadata_record,
        'terms_conditions_accepted':"True",
        'data_agreement_accepted':"True",
        'data_agreement_url':"https://www.environment.gov.za/branches/oceans_coast",
        'capture_method':"harvester"
    }

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + odp_ckan_api_key,
    }
    url = ckan_base_url + f'/{institution}/metadata/'
    response = requests.post(url,json=record_data, headers=headers)
    result = json.loads(response.text)

    if response.status_code == 200:
        print("Added record {} successfuly".format(result['pid']))
    else:
        print('Metadata ID: {}'.format(result['pid']))
        print('Organization: {}'.format(result['institution_key']))
        print('Metadata Collection: {}'.format(result['collection_key']))

        #print(record_data)
        #raise RuntimeError('Request failed with return code: %s' % (response.status_code))

    #print("Response keys {}".format(result.keys()))
    #TODO: create error checks for check_ckan_added
    # if check_ckan_added(institution, result):
    #     msg = 'Added Successfully'
    #     logging.info(msg)
    #
    #     UPDATE_METRICS['records_added'] = UPDATE_METRICS['records_added'] + 1
    #
    # else:
    #     msg = 'Record not found'
    #     logging.info(msg)
    #
    # record_id = result['id']
    #
    # accepted_errors = []#[u'spatialRepresentationTypes', u'purpose', u'spatialResolution',
    #                   # u'metadataTimestamp', u'responsibleParties', u'constraints']
    #                    # u'lineageStatement',u'extent',u'topicCategories',u'abstract'u'relatedIdentifiers'
    # errors = result['errors'].keys()
    # bad_errors =[]
    # for err in errors:
    #     if err not in accepted_errors:
    #         bad_errors.append(err)
    # if len(bad_errors) > 0:
    #     #print("Bad errors {}".format(result['errors']['responsibleParties']))
    #     print(bad_errors)
    #     print(result['errors'])
    #     for error in bad_errors:
    #         print(result['metadata'][error])
    #     raise Exception
    #
    # record_id = result['id']
    # updated = set_workflow_state('mims-published', record_id)
    # UPDATE_METRICS['validated_successfully'] = UPDATE_METRICS['validated_successfully'] + 1
    # if updated:
    #     UPDATE_METRICS['published'] = UPDATE_METRICS['published'] + 1

    """
    if result['validated'] and (len(result['errors'].keys()) == 0):#result['validate_status'] == 'success':
        msg = "Validated successfully, advancing state"
        logging.info(msg)
        updated = set_workflow_state('plone-published', record_id, organization, result)
        UPDATE_METRICS['validated_successfully'] = UPDATE_METRICS['validated_successfully'] + 1
        if updated:
            UPDATE_METRICS['published'] = UPDATE_METRICS['published'] + 1

    elif result['validated'] and (len(result['errors'].keys()) > 0):
        msg = "Validation errors:\n {}\nAttempting published state".format(result['errors'])
        logging.error(msg)
        #logging.error(result)
        updated = set_workflow_state('plone-published', record_id, organization, result)
        if updated:
            msg = "Successfully published with validation errors"
            logging.error(msg)
            UPDATE_METRICS['validated_successfully'] = UPDATE_METRICS['validated_successfully'] + 1
            UPDATE_METRICS['published'] = UPDATE_METRICS['published'] + 1
        else:
        #print(metadata_json)
            msg = "Unable to publish with validation errors, setting state to provisional"
            logging.error(msg)
            logging.error(result)
            updated = set_workflow_state('plone-provisional', record_id, organization, result)
            UPDATE_METRICS['validation_errors'] = UPDATE_METRICS['validation_errors'] + 1
            if updated:
                UPDATE_METRICS['unpublished'] = UPDATE_METRICS['unpublished'] + 1

    else:
        msg = 'Request to add record failed'
        logging.error(msg)
        logging.error(result)
    #    #print(result)
    """
    return result

def check_ckan_added(institution, result):
    time.sleep(1)
    # Find the record via jsonContent
    record_id = result['id']
    url = "{}/{}/metadata/{}".format(ckan_base_url,institution, record_id)
    try:
        response = requests.get(
            url=url,
            headers={'Authorization': 'Bearer ' + odp_ckan_api_key},
            verify=False
        )
    except Exception as e:
        print(e)
        return False

    if response.status_code != 200:
        return False

    found = False
    result = json.loads(response.text)
    if result['id'] == record_id:
        found = True
    return found

def set_workflow_state(record,institution,state):

    print("Trying to change record workflow state into {}".format(state))

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + odp_ckan_api_key,
    }
    # url='http://odpapi-migration.saeon.dvn/dea/metadata/workflow/b67bf8cb-55c3-426b-ac33-82ea3be62bd8?state=Published'
    url = ckan_base_url + f"/{institution}/metadata/workflow/{record['fileIdentifier']}?state={state}"
    response = requests.post(url, headers=headers)
    result = json.loads(response.text)

    # if response['success']:
    #     print('lul')
    # if response.status_code == 200 and response.success == 'False':
    #     raise RuntimeError('Request failed with return code: %s' % (response.errors))
    # if response.status_code != 200 and ('message' in result['detail']) and \
    #     (result['detail']['message'] != \
    #         'The metadata record is already assigned the specified workflow state'):
    #     raise RuntimeError('Request failed with return code: %s' % (
    #         response.status_code))
    # elif response.status_code != 200 and ('message' in result['detail']) and \
    #     (result['detail']['message'] == \
    #         'The metadata record is already assigned the specified workflow state'):
    #     logging.info('The metadata record is already assigned the specified workflow state {}'.format(state))
    #     state_unchanged = True
    #
    # if not state_unchanged and result['success']:#
    #     msg = 'Workflow status updated to {}'.format(state)
    #     logging.info(msg)
    #     return True
    # else:
    #     if not state_unchanged:
    #         msg = 'Workflow status could not be updated!\n Error {}'.format(result)
    #         logging.error(msg)
    #         return False
    #TODO Write the changing state error codes