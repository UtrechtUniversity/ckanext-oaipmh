# encoding=utf8
from __future__ import unicode_literals
import sys

import json
import logging
import re
import unicodedata
import urllib

import requests

import oaipmh.client
from ckan import model
from ckan.lib.munge import munge_tag, munge_title_to_name
from ckan.logic import get_action
from ckan.model import Session
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject
from .metadata import (datacite_ckan_importer, 
                       oai_dc_reader, oai_ddi_reader, dif_reader2)
from oaipmh.metadata import MetadataRegistry

log = logging.getLogger(__name__)


class OaipmhHarvester(HarvesterBase):
    '''
    OAI-PMH Harvester
    '''

    # dict to hold all data related to a package
    package_dict = {}

    def info(self):
        '''
        Return information about this harvester.
        '''
        return {
            'name': 'OAI-PMH',
            'title': 'OAI-PMH',
            'description': 'Harvester for OAI-PMH data sources'
        }

    def gather_stage(self, harvest_job):
        '''
        The gather stage will recieve a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its source and job.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''
        try:
            harvest_obj_ids = []

            self._set_config(harvest_job.source.config)

            # Registry content is made dependant on info
            # fetched from datasource.
            registry = self._create_metadata_registry()

            client = oaipmh.client.Client(
                harvest_job.source.url,
                registry,
                self.credentials,
                force_http_get=self.force_http_get
            )

            log.debug('URL: ' + harvest_job.source.url)

            client.identify()  # check if identify works
            counter = 1
            for header in self._identifier_generator(client):
                harvest_obj = HarvestObject(
                    guid=header.identifier(),
                    job=harvest_job
                )
                harvest_obj.save()
                log.debug("HDR in gather stage -harvest_obj.id: %s"
                          % harvest_obj.id)
                harvest_obj_ids.append(harvest_obj.id)
                
                counter += 1
                if counter > 1:
                    break                
                
        except urllib.error.HTTPError as e:
            log.exception(
                'Gather stage failed on %s (%s): %s, %s'
                % (
                    harvest_job.source.url,
                    e.fp.read(),
                    e.reason,
                    e.hdrs
                )
            )
            self._save_gather_error(
                'Could not gather anything from %s' %
                harvest_job.source.url, harvest_job
            )
            return None
        except Exception as e:
            log.exception(
                'Gather stage failed on %s: %s'
                % (
                    harvest_job.source.url,
                    str(e),
                )
            )
            self._save_gather_error(
                'Could not gather anything from %s' %
                harvest_job.source.url, harvest_job
            )
            return None
        return harvest_obj_ids

    def _identifier_generator(self, client):
        """
        pyoai generates the URL based on the given method parameters
        Therefore one may not use the set parameter if it is not there
        """
        if self.set_spec:
            for header in client.listIdentifiers(
                    metadataPrefix=self.md_format,
                    set=self.set_spec):
                yield header
        else:
            for header in client.listIdentifiers(
                    metadataPrefix=self.md_format):
                yield header

    def _create_metadata_registry(self):
        registry = MetadataRegistry()

        # md_application is added as a configurational item from the harvest source (as manually added by ...)
        if self.md_application == 'ckan_importer':
            self.md_format = 'datacite'
            registry.registerReader(self.md_format, datacite_ckan_importer)
            log.debug('ckan_importer Format=datacite')
        else:
            registry.registerReader('oai_dc', oai_dc_reader)
            registry.registerReader('oai_ddi', oai_ddi_reader)
            registry.registerReader('dif', dif_reader2)

        return registry

    def _set_config(self, source_config):
        try:
            # Set config to empty JSON object
            if not source_config:
                source_config = '{}'

            config_json = json.loads(source_config)
            #  log.debug('config_json: %s' % config_json)
            try:
                username = config_json['username']
                password = config_json['password']
                self.credentials = (username, password)
            except (IndexError, KeyError):
                self.credentials = None

            self.user = 'harvest'
            self.set_spec = config_json.get('set', None)
            self.md_format = config_json.get('metadata_prefix', 'datacite')

            # Possibility to differentiation for the metadata handling methods.
            # This is dealt with through the configuration field in a ckan harvest source
            # that can be manually added by a ckan-maintainer.

            # md_application defaults to 'ckan_importer' requiring a json file with the configuration
            self.md_application = config_json.get('application', 'ckan_importer')

            # Additional info adds possibities to differentiate - this is in essence only for EPOS
            # within a metadata_prefix.
            # Maybe call this variable namespace_info.
            self.additional_info = config_json.get('additional_info',
                                                   'kernel4')
            # TODO: Change default back to 'oai_dc'
            self.force_http_get = config_json.get('force_http_get', False)

        except ValueError:
            pass

    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        log.debug("HDR: Fetch url %s" % harvest_object.job.source.url)

        try:
            self._set_config(harvest_object.job.source.config)
            # Registry creation is dependant on job.source.config
            # because of differentiation possibilities in
            # namespaces for equal md_prefix.

            log.debug('Application: ' + self.md_application)
            log.debug('Md_format: ' + self.md_format)
            log.debug('AddInfo: ' + self.additional_info)

            registry = self._create_metadata_registry()
            client = oaipmh.client.Client(
                harvest_object.job.source.url,
                registry,
                self.credentials,
                force_http_get=self.force_http_get
            )
            record = None
            try:
                self._before_record_fetch(harvest_object)

                record = client.getRecord(
                    identifier=harvest_object.guid,
                    metadataPrefix=self.md_format
                )
                self._after_record_fetch(record)

            except Exception:
                log.exception('getRecord failed')
                self._save_object_error('Get record failed!', harvest_object)
                return False

            header, metadata, _ = record

            log.debug(record)

            try:
                metadata_modified = header.datestamp().isoformat()
            except Exception:
                metadata_modified = None

            try:
                content_dict = metadata.getMap()

                content_dict['set_spec'] = header.setSpec()
                if metadata_modified:
                    content_dict['metadata_modified'] = metadata_modified

                content = json.dumps(content_dict,
                                     ensure_ascii=False)
            except Exception:
                log.exception('Dumping the metadata failed!')
                self._save_object_error(
                    'Dumping the metadata failed!',
                    harvest_object
                )
                return False

            harvest_object.content = content
            harvest_object.save()
        except Exception:
            log.exception('Something went wrong 1!')
            self._save_object_error(
                'Exception in fetch stage',
                harvest_object
            )
            return False

        return True

    def _before_record_fetch(self, harvest_object):
        pass

    def _after_record_fetch(self, record):
        pass

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g
              create a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package must be added to the HarvestObject.
              Additionally, the HarvestObject must be flagged as current.
            - creating the HarvestObject - Package relation (if necessary)
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''

        # log.debug("in import stage: %s" % harvest_object.guid)
        if not harvest_object:
            log.error('No harvest object received')
            self._save_object_error('No harvest object received')
            return False

        try:
            self._set_config(harvest_object.job.source.config)

            context = {
                'model': model,
                'session': Session,
                'user': self.user,
                'ignore_auth': True  # TODO: Remove, just to test
            }

            # Main dictonary holding all package data to be sent to CKAN.
            self.package_dict = {}

            content = json.loads(harvest_object.content)

            log.debug(content)

            # Work out default organization based upon current harvest job.
            harvest_source = get_action('harvest_source_show')(
                context,
                {'id': harvest_object.job.source.id}
            )

            # Pass extra information (default organization) to handling methods
            content['owner_org'] = harvest_source['owner_org']

            harvest_source_organization = get_action('organization_show')(
                context,
                {'id': harvest_source['owner_org']}
            )

            # Maintainer info (name/email) )to be collected for EPOS through current harvest source organization
            content['maintainer'] = ''
            content['maintainer_email'] = ''

            for index in harvest_source_organization:
                if index == 'extras':  # capture email of maintainer
                    for extra_avu in harvest_source_organization['extras']:
                        if extra_avu['key'] == 'email' and extra_avu['state'] == 'active':
                            content['maintainer_email'] = extra_avu['value']
                            break
                        break
                    break
                elif index == 'display_name':  # capture name of maintainer
                    content['maintainer'] = harvest_source_organization['display_name']

            log.info('Maintainer: ' + content['maintainer'])
            log.info('Maintainer: ' + content['maintainer_email'])

            # This is part of CKAN itself - should not be dealt with within the configuration file when in ckan_importer mode!!
            # !!?? maybe move it to the end down below
            self.package_dict['id'] = munge_title_to_name(harvest_object.guid)
            self.package_dict['name'] = self.package_dict['id']

            # Differentiate handling according to md_application in configuration of harvest_source
            if self.md_application == 'ckan_importer':
                # Get the configuration settings for the metadata.
                ckan_uu_json_file = '/srv/app/src_extensions/ckanext-oaipmh/ckanext/oaipmh/ckan_importer_config_file.json'
                ckan_uu_config = {}
                with open(ckan_uu_json_file) as f:
                    ckan_uu_config = json.load(f)
                    log.info('ckan_uu_config: %s' % (ckan_uu_config))

                # for k,v in ckan_uu_config.items():
                #    log.info(k)

                # Merge package_dict with newly created data dict.
                # Possibly maintainer / maintainer_email is overwritten
                data_transformed_dict = self._handle_import_configuration(ckan_uu_config, content, context)
                self.package_dict.update(data_transformed_dict)


            self.package_dict['owner_org'] = content['owner_org']
           
            # nodig???
            self.package_dict['groups'] = []
            
            # log.debug('Create/update package using dict: %s'
            #          % self.package_dict)
            if 'title' in self.package_dict and self.package_dict['title']:
                self._create_or_update_package(
                    self.package_dict,
                    harvest_object,
                    package_dict_form='package_show'
                )

            Session.commit()
        except Exception:
            log.exception('Something went wrong!')
            self._save_object_error(
                'Exception in import stage',
                harvest_object
            )
            return False
        return True

    def _handle_import_configuration(self, ckan_uu_config, data, context):
        """
          Step through all data configurations and build a dict that CKAN can handle.
          The keys MUST correspond to the names in CKAN data schema

        """
        # Holds the resulting data for CKAN
        ckan_package_dict = {}

        # ckan_base_key: the key that should correspond with the highest level in ckan - schema
        for ckan_base_key, v in ckan_uu_config.items():
            try:
                # Get the select_base as defined in the configuration
                conf_select_base = ckan_uu_config[ckan_base_key]['select_base']

                # typeL FIXED handling: allows for hardcoded values assigned to
                if ckan_uu_config[ckan_base_key]['type'] == 'fixed':
                    # Assign the value to the key
                    ckan_package_dict[ckan_base_key] = conf_select_base

                # type: SINGLE handling
                elif ckan_uu_config[ckan_base_key]['type'] == 'single':
                    prefix = ''
                    if 'prefix' in ckan_uu_config[ckan_base_key]:
                        prefix = ckan_uu_config[ckan_base_key]['prefix']

                    data_parts = conf_select_base.split('>')
                    if len(data_parts) > 1:
                        # DIt is een nog onuitgewerkte case - ff laten staan zo
                        found_val = None
                        if isinstance(data[data_parts[0]][data_parts[1]], list):
                            # Look at the first data item in the list!
                            # this can be a dict or a string
                            if isinstance(data[data_parts[0]][data_parts[1]][0], dict):
                                if len(data_parts)>2:
                                    found_val = data[data_parts[0]][data_parts[1]][0][data_parts[2]]
                                else:
                                    found_val = data[data_parts[0]][data_parts[1]][0]['#text']
                            elif isinstance(data[data_parts[0]][data_parts[1]][0], str):
                                found_val = data[data_parts[0]][data_parts[1]][0]
                        else:
                            if isinstance(data[data_parts[0]][data_parts[1]], dict):
                                if len(data_parts)>2:
                                    found_val = data[data_parts[0]][data_parts[1]][data_parts[2]]
                                else:
                                    found_val = data[data_parts[0]][data_parts[1]]['#text']
                            elif isinstance(data[data_parts[0]][data_parts[1]], str):
                                found_val = data[data_parts[0]][data_parts[1]]

                        if found_val is not None:
                            ckan_package_dict[ckan_base_key] = found_val

                    else:
                        # Get the relevant data
                        select_base_data = data[conf_select_base]

                        # Will hold the actual definition of a set - if present
                        set_definition = {}

                        # Will hold the data corresponding to the
                        set_data = {}

                        # Check whether a set_definition exists
                        if 'set_definition' in ckan_uu_config[ckan_base_key]:
                            set_definition = ckan_uu_config[ckan_base_key]['set_definition']

                        if isinstance(select_base_data, dict):
                            # ckan_package_dict[ckan_base_key] = the_data['#text'] + "->#text"
                            # If a set definition exists => step through all definitions and assign the correct value in set_data-dict
                            if set_definition:
                                for set_key, set_value in set_definition.items():
                                    set_data[set_key] = select_base_data[set_value]
                                # Add it to the central dict
                                ckan_package_dict[ckan_base_key] = set_data
                            else:
                                # As it is a dict, the main text can only be found with '#text' as a key.
                                ckan_package_dict[ckan_base_key] = prefix + select_base_data['#text']
                        elif isinstance(select_base_data, str):
                            ckan_package_dict[ckan_base_key] = prefix + data[conf_select_base]

                # type: ARRAY handling
                elif ckan_uu_config[ckan_base_key]['type'] == 'array':
                    data_parts = conf_select_base.split('>')
                    # At this moment we know no further depth will be required than 2 deep.
                    if len(data_parts) > 1:
                        select_base_data = data[data_parts[0]][data_parts[1]]
                    else:
                        select_base_data = data[data_parts[0]]

                    # Force select_base_data to be a list
                    if not isinstance(select_base_data, (list)):
                        select_base_data = [select_base_data]

                    if 'set_definition' in ckan_uu_config[ckan_base_key]:
                        set_definition = ckan_uu_config[ckan_base_key]['set_definition']

                        # Will hold the entire resulting list
                        the_entire_list = []

                        # Step through each data dict
                        for select_base_data_dict in select_base_data:
                            set_data = {}
                            # print(select_base_data_dict)
                            if set_definition:
                                # build a list of dicts using the set_definition
                                for set_key, set_value in set_definition.items():
                                    # set_data[set_key] = select_base_data_dict[set_value]

                                    # It is possible that even more levels are taken into account.
                                    if set_value.startswith(tuple(['#', '@'])):
                                        # direct assignment
                                        set_data[set_key] = select_base_data_dict[set_value]
                                    else:
                                        # check presence of # or @ as this indicates a deeper level to be taken into account
                                        set_value_parts = set_value.split('#')
                                        if len(set_value_parts) == 2:
                                            # set_value_parts[0] possibly holds a filter. Like relatedIdentifier[scheme=ORCID]
                                            # Only the first part is of interest as a key.
                                            # The filter is one step later.
                                            subset_parts = set_value_parts[0].split('[')
                                            filter_key = ''
                                            filter_value = ''
                                            if len(subset_parts) == 1:
                                                dict_key = set_value_parts[0]
                                            else:
                                                dict_key = subset_parts[0]
                                                # figure out filter parts
                                                temp = subset_parts[1][0:-1].split('=')
                                                filter_key = temp[0]
                                                filter_value = temp[1]

                                            if isinstance(select_base_data_dict[dict_key], list):
                                                # find the correct row in the list, based on the filter that is in the configuration
                                                the_value = ''
                                                for the_dict in select_base_data_dict[dict_key]:
                                                    if the_dict[filter_key] == filter_value:
                                                        the_value = the_dict['#text']
                                                        break
                                                set_data[set_key] = the_value
                                                # print(the_value)
                                            else:
                                                set_data[set_key] = select_base_data_dict[set_value_parts[0]][
                                                    '#' + set_value_parts[1]]
                                        else:
                                            set_value_parts = set_value.split('@')
                                            if len(set_value_parts) == 2:
                                                set_data[set_key] = select_base_data_dict[set_value_parts[0]][
                                                    '@' + set_value_parts[1]]
                                            else:
                                                set_data[set_key] = select_base_data_dict[set_value]
                                        # set_value_parts = set_value.split('@')

                                # Add the resulting set_data dict to the list
                                the_entire_list.append(set_data)

                        # Add the entire resulting list to the key as used within ckan
                        ckan_package_dict[ckan_base_key] = the_entire_list

                    else:
                        # no explicit SET definition. List can be used directly as is.
                        ckan_package_dict[ckan_base_key] = select_base_data


            except KeyError:
                log.info("key error: " + ckan_base_key)
                # Go to next in the loop
                continue

        return ckan_package_dict

    # Handle data where metadata prefix in
    # (dif, oai_dc, oai_ddi) -> this is not EPOS oriented
    def _handle_nonEpos(self, content, context, harvest_object):
        # AUTHOR
        self.package_dict['author'] = self._nonEpos_extract_author(content)

        # ORGANIZATION
        source_dataset = get_action('package_show')(
            context,
            {'id': harvest_object.source.id}
        )
        owner_org = source_dataset.get('owner_org')
        # log.debug(owner_org)
        self.package_dict['owner_org'] = owner_org

        # LICENSE
        self.package_dict['license_id'] = self._nonEpos_extract_license_id(
            content)

        # FORMATS
        # TODO: Need to map to CKAN author field
        formats = self._nonEpos_extract_formats(content)
        self.package_dict['formats'] = formats

        # RESOURCES
        url = self._nonEpos_get_possible_resource(harvest_object, content)
        self.package_dict['resources'] = self._nonEpos_extract_resources(
            url, content)

        # groups aka projects
        groups = []

        # create group based on set
        if content['set_spec']:
            #  log.debug('set_spec: %s' % content['set_spec'])
            groups.extend(
                self._find_or_create_entity(
                    'group',
                    content['set_spec'],
                    context
                )
            )

        # add groups from content
        groups.extend(
            self._nonEpos_extract_groups(content, context)
        )

        self.package_dict['groups'] = groups

        # extract tags from 'type' and 'subject' field
        # everything else is added as extra field
        tags, extras = self._nonEpos_extract_tags_and_extras(content)
        self.package_dict['tags'] = tags
        self.package_dict['extras'] = extras

    def _get_mapping(self):
        if self.md_format == 'datacite':
            return {
                'title': 'title',
                'notes': 'description',
                'license_id': 'rights'
            }
        elif self.md_format == 'iso19139':
            return {
                # 'title': 'title'
            }

        elif self.md_format == 'dif':
            # CKAN fields explained here:
            # http://docs.ckan.org/en/ckan-1.7.4/domain-model-dataset.html
            # https://github.com/ckan/ckan/blob/master/ckan/logic/schema.py
            # TODO: Are there more fields to add?
            return {
                'title': 'Entry_Title',
                'notes': 'Summary/Abstract',
                #  'name': '',
                # Thredds catalog?
                #  'url': '',
                #  'author_email': '',
                #  'maintainer': '',
                'maintainer_email': 'Personnel/Email',
                # Dataset version
                #  'version': '',
                #  'groups': '',
                #  'type': '',
            }
        else:
            return {
                'title': 'title',
                'notes': 'description',
                'maintainer': 'publisher',
                'maintainer_email': 'maintainer_email',
                'url': 'source',
            }

    def _nonEpos_extract_author(self, content):
        if self.md_format == 'dif':
            dataset_creator = ', '.join(
                content['Data_Set_Citation/Dataset_Creator'])
            # TODO: Remove publisher? Is not part of mapping...
            dataset_publisher = ', '.join(
                content['Data_Set_Citation/Dataset_Publisher'])
            if 'not available' not in dataset_creator.lower():
                return dataset_creator
            elif 'not available' not in dataset_publisher.lower():
                return dataset_publisher
            else:
                return 'Not available'
        else:
            return ', '.join(content['creator'])

    def _nonEpos_extract_license_id(self, content):
        if self.md_format == 'dif':
            use_constraints = ', '.join(content['Use_Constraints'])
            access_constraints = ', '.join(content['Access_Constraints'])
            # TODO: Generalize in own function to check for both
            #       'Not available' and None value
            if ('not available' not in use_constraints.lower() and
               'not available' not in access_constraints.lower()):
                return '{0}, {1}'.format(use_constraints, access_constraints)
            elif 'not available' not in use_constraints.lower():
                return use_constraints
            elif 'not available' not in access_constraints.lower():
                return access_constraints
        else:
            return content['rights']

    def _nonEpos_extract_tags_and_extras(self, content):
        extras = []
        tags = []
        for key, value in content.iteritems():
            if key in self._get_mapping().values():
                continue
            if key in ['type', 'subject']:
                if type(value) is list:
                    tags.extend(value)
                else:
                    tags.extend(value.split(';'))
                continue
            if value and type(value) is list:
                value = value[0]
            if not value:
                value = None
            extras.append((key, value))

        tags = [munge_tag(tag[:100]) for tag in tags]

        return (tags, extras)

    def _nonEpos_extract_formats(self, content):
        if self.md_format == 'dif':
            formats = []
            urls = content['Related_URL/URL']
            for url in urls:
                if 'wms' in url.lower():
                    formats.append('wms')
                elif 'dods' in url.lower():
                    formats.append('opendap')
                elif 'catalog' in url.lower():
                    # thredds catalog
                    formats.append('thredds')
                else:
                    formats.append('HTML')
                # TODO: Default is html

            # TODO: wcs, netcdfsubset, 'fou-hi'?
            return formats
        else:
            return content['format']

    def _nonEpos_get_possible_resource(self, harvest_obj, content):
        if self.md_format == 'dif':
            urls = content['Related_URL/URL']
            if urls:
                return urls
        else:
            url = []
            candidates = content['identifier']
            candidates.append(harvest_obj.guid)
            for ident in candidates:
                if ident.startswith('http://') or ident.startswith('https://'):
                    url.append(ident)
                    break
            return url

    # TODO: Refactor
    def _nonEpos_extract_resources(self, urls, content):
        if self.md_format == 'dif':
            resources = []
            if urls:
                try:
                    resource_formats = self._nonEpos_extract_formats(content)
                except (IndexError, KeyError):
                    print('IndexError: ', IndexError)
                    print('KeyError: ', KeyError)

                for index, url in enumerate(urls):
                    resources.append({
                        'name': content['Related_URL/Description'][index],
                        'resource_type': resource_formats[index],
                        'format': resource_formats[index],
                        'url': url
                    })
            return resources
        else:
            resources = []
            # url = urls[0]
            if False:  # url
                try:
                    # TODO: Use _nonEpos_extract_formats to get format
                    resource_format = content['format'][0]
                except (IndexError, KeyError):
                    # TODO: Remove. This is only needed for DIF
                    if 'thredds' in url:
                        resource_format = 'thredds'
                    else:
                        resource_format = 'HTML'
                resources.append({
                    'name': content['title'][0],
                    'resource_type': resource_format,
                    'format': resource_format,
                    'url': url
                })
            return resources

    def _nonEpos_extract_groups(self, content, context):
        if 'series' in content and len(content['series']) > 0:
            return self._find_or_create_entity(
                'group',
                content['series'],
                context
            )
        return []

    # For EPOS - do not create new entities but fall back to default if not found .
    # in EPOS case used for Labs (i.e. groups)
    def _find_first_entity(self, entityType, entityNames, context):
        log.debug('------------ find first')
        log.debug(entityType + ' names: %s' % entityNames)

        entityId = '-1'   # Not found - should not be possible
        for entity_name in entityNames:
            log.debug('search lab: ' + entity_name)
            # log.debug( self._utf8_and_remove_diacritics(entity_name) )
            # log.debug( munge_title_to_name(entity_name) )
            data_dict = {
                'id': munge_title_to_name(entity_name),
            }
            log.debug(data_dict)
            try:
                entity = get_action(entityType + '_show')(context, data_dict)
                log.info('Try: found the ' + entityType + ' with id' + entity['id'])
                entityId = entity['id']
                break

            except Exception as e:
                # Get rid of auth audit on the context otherwise we'll get an unwanted exception next time around
                # exception
                context.pop('__auth_audit', None)

                log.info('Exception: ' + entity_name)
                log.info(str(e))
                continue

        return entityId

    # generic function for finding/creation of multiple entities (groups/organizations)

    def _find_or_create_entity(self, entityType, entityNames, context):
        log.debug(entityType + ' names: %s' % entityNames)
        entity_ids = []
        for entity_name in entityNames:
            data_dict = {
                'id': self._utf8_and_remove_diacritics(entity_name),
                'name': munge_title_to_name(entity_name),
                'title': entity_name
            }
            try:
                entity = get_action(entityType + '_show')(context, data_dict)
                log.info('found the ' + entityType + ' with id' + entity['id'])
            except Exception:
                entity = self._create_entity(entityType, data_dict, context)

            entity_ids.append(entity['id'])

            log.debug(entityType + ' ids: %s' % entity_ids)
        return entity_ids

    # Generic function to create either a group or organization.
    # Dict requires diacritics removed on id
    def _create_entity(self, entityType, entityDict, context):
        try:
            newEntity = get_action(entityType + '_create')(context, entityDict)
            log.info('Created ' + entityType + ' with id: ' + newEntity['id'])
        except Exception:
            # entityDict already holds the correct id
            # So if problems during creations
            # return the value already known.
            # Log it though
            log.info('Creation of ' + entityType +
                     ' was troublesome-revert to: ' + entityDict['id'])
            newEntity = {
                'id': entityDict['id']
            }

        return newEntity

    def _utf8_and_remove_diacritics(self, input_str):
        # nkfd_form = unicodedata.normalize('NFKD', str(input_str))
        nkfd_form = unicodedata.normalize('NFKD', input_str)
        return (u"".join([c for c in nkfd_form if not unicodedata.combining(c)]))
