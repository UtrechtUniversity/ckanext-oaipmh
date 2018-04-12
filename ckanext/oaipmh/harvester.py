import logging
import json
import urllib2

from ckan.model import Session
from ckan.logic import get_action
from ckan import model

from ckanext.harvest.harvesters.base import HarvesterBase
from ckan.lib.munge import munge_tag
from ckan.lib.munge import munge_title_to_name
from ckanext.harvest.model import HarvestObject

import oaipmh.client
from oaipmh.metadata import MetadataRegistry

from metadata import oai_ddi_reader
from metadata import oai_dc_reader
from metadata import dif_reader, dif_reader2
from metadata import datacite_reader
from pprint import pprint

import traceback

log = logging.getLogger(__name__)


class OaipmhHarvester(HarvesterBase):
    '''
    OAI-PMH Harvester
    '''

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
        log.debug("HDR in gather stage: %s" % harvest_job.source.url)
        try:
            harvest_obj_ids = []
            registry = self._create_metadata_registry()
            self._set_config(harvest_job.source.config)
            client = oaipmh.client.Client(
                harvest_job.source.url,
                registry,
                self.credentials,
                force_http_get=self.force_http_get
            )

            client.identify()  # check if identify works
            for header in self._identifier_generator(client):
                harvest_obj = HarvestObject(
                    guid=header.identifier(),
                    job=harvest_job
                )
                harvest_obj.save()
		log.debug("HDR in gather stage -harvest_obj.id: %s" % harvest_obj.id)
                harvest_obj_ids.append(harvest_obj.id)
        except urllib2.HTTPError, e:
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
        except Exception, e:
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
        registry.registerReader('oai_dc', oai_dc_reader)
        registry.registerReader('oai_ddi', oai_ddi_reader)
        # TODO: Change back?
        registry.registerReader('dif', dif_reader2)
	# HDR
        registry.registerReader('datacite', datacite_reader)
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
            self.md_format = config_json.get('metadata_prefix', 'dif')
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
        #  log.debug("in fetch stage: %s" % harvest_object.guid)

        log.debug("HDR: Fetch url %s" % harvest_object.job.source.url)

        try:
            self._set_config(harvest_object.job.source.config)
            registry = self._create_metadata_registry()
            client = oaipmh.client.Client(
                harvest_object.job.source.url,
                registry,
                self.credentials,
                force_http_get=self.force_http_get
            )
            record = None
            try:
                log.debug(
                    "HDR Load %s with metadata prefix '%s'" %
                    (harvest_object.guid, self.md_format)
                )

                self._before_record_fetch(harvest_object)

                record = client.getRecord(
                    identifier=harvest_object.guid,
                    metadataPrefix=self.md_format
                )
                self._after_record_fetch(record)

                #  log.debug('record found!')
            except:
                log.exception('getRecord failed')
                self._save_object_error('Get record failed!', harvest_object)
                return False

            header, metadata, _ = record
            log.debug('metadata %s' % metadata)
            log.debug('header %s' % header)

            try:
                metadata_modified = header.datestamp().isoformat()
            except:
                metadata_modified = None

            try:
                # TODO: This fails for some resources
                content_dict = metadata.getMap()
                content_dict['set_spec'] = header.setSpec()
                if metadata_modified:
                    content_dict['metadata_modified'] = metadata_modified
                #  log.debug(content_dict)
                content = json.dumps(content_dict)
                # log.debug(content)
            except:
                log.exception('Dumping the metadata failed!')
                self._save_object_error(
                    'Dumping the metadata failed!',
                    harvest_object
                )
                return False

            harvest_object.content = content
            harvest_object.save()
        except:
            log.exception('Something went wrong!')
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

        #  log.debug("in import stage: %s" % harvest_object.guid)
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

            package_dict = {}


            # log.error(harvest_object.content)
            content = json.loads(harvest_object.content)
            # log.error(content)


            package_dict['id'] = munge_title_to_name(harvest_object.guid)
            package_dict['name'] = package_dict['id']

            mapping = self._get_mapping()

	    # HIER ALLEEN ENKELVOUDIGE VELDEN!!
	    # DE MAPPING GAAT ALLEEN OVER ENKELE VELDEN
            for ckan_field, oai_field in mapping.iteritems():
                log.debug('HDR ckan_field %s' % ckan_field)
                log.debug('HDR oai_field %s' % oai_field)
                try:
                    if ckan_field == 'maintainer_email' and '@' not in content[oai_field][0]:
                        # Email not available.
                        # Do not set email field as it will break validation.
                        continue
                    else:
                        package_dict[ckan_field] = content[oai_field][0]

                except (IndexError, KeyError):
                    continue

            # HdR from here package_dict has CKAN - keys
	    # conntent is still based on json response -> i.e. GFZ
	    # ------------------------------------------------------------


            # add author
            # TODO: Remove Dataset_Creator and/or Dataset_Publisher as it is redundant information
            package_dict['author'] =  ', '.join(content['creator'])

            # add owner_org
            source_dataset = get_action('package_show')(
              context,
              {'id': harvest_object.source.id}
            )
            owner_org = source_dataset.get('owner_org')
            package_dict['owner_org'] = owner_org

#            # add license
#            package_dict['license_id'] = self._extract_license_id(content)

            # TODO: Need to map to CKAN author field
            # formats = self._extract_formats(content)
            package_dict['formats'] = 'datacite' #formats

            # add resources
            # TODO: Make list
            # url = self._get_possible_resource(harvest_object, content)
            # package_dict['resources'] = self._extract_resources(url, content)

            # extract tags from 'type' and 'subject' field
            # everything else is added as extra field
            tags, extras = self._extract_tags_and_extras(content)
            package_dict['tags'] = tags
            package_dict['extras'] = extras

            # groups aka projects
            groups = [] 

            # create group based on set
#            if content['set_spec']:
#                log.debug('set_spec: %s' % content['set_spec'])
#                groups.extend(
#                    self._find_or_create_groups(
#                        content['set_spec'],
#                        context
#                    )
#                )
            # add groups from content
#            groups.extend(
#                self._extract_groups(content, context)
#            )



#            # create group based on set
#            if content['groups']:
#                #  log.debug('groups: %s' % content['groups'])
#                groups.extend(
#                    self._find_or_create_groups(
#                        content['groups'],
#                        context
#                    )
#                )


	
	    subjects = []
            
            # hdrref
	    # content['subjects'] = [] #['subject1', 'subject2']
	    
	    subjects = content['groups']
	    # create groups based on subjects
            if subjects:
                log.debug('subjects: %s' % subjects)
                groups.extend(
                    self._find_or_create_groups(
                        subjects,
                        context
                    )
                )

	    # add the groups that were found to the package
	    package_dict['groups'] = groups

#            # extract tags from 'type' and 'subject' field
#            # everything else is added as extra field
            # tags, extras =  self._extract_tags_and_extras(content)
	    # tags, extras = content['gfz-tags']	
            package_dict['tags'] = content['tags'] #['Tag1', 'Tag2'] #tags
            
	    extras = []
	    #key ='key'
            #value = 'value'
            #for i in range(3):
            #    extras.append((key + str(i),value + str(i)))
            #    extras.append((key + str(i),value + 'secondkey' + str(i)))

	    if content['doi']:
                extras.append(('Source',content['doi'][0]))	
            if content['created']:
                extras.append(('Created',content['created'][0]))
            if content['publicationYear']:
                extras.append(('Year of publication',content['publicationYear'][0])) 
	    if content['supplementTo']:
		extras.append(('Is supplement to', content['supplementTo'][0]))
	    if content['cites']:
                extras.append(('Cites',content['cites'][0]))
            if content['westBoundLongitude']:
                extras.append(('geobox-wLong',content['westBoundLongitude'][0]))
            if content['eastBoundLongitude']:
                extras.append(('geobox-eLong',content['eastBoundLongitude'][0]))
            if content['northBoundLatitude']:
                extras.append(('geobox-nLat',content['northBoundLatitude'][0]))
            if content['southBoundLatitude']:
                extras.append(('geobox-sLat',content['southBoundLatitude'][0]))

            if content['contact']:
                extras.append(('Contact', content['contact'][0] + '-' + content['contactAffiliation'][0]))
            if content['contactEmail']:
                extras.append(('Contact email', 'blabla@blabla.com'))
            if content['publisher']:
                extras.append(('Publisher',content['publisher'][0]))

            # test regel

	    package_dict['extras'] = extras #['custom key1','custom val1'] #[] #['11', '22'] #extras
		

# HIER GEBEURT NOG NIKS
#            # allow sub-classes to add additional fields
#            package_dict = self._extract_additional_fields(
#                content,
#                package_dict
#            )


            # log.debug('Create/update package using dict: %s' % package_dict)
            self._create_or_update_package(
                package_dict,
                harvest_object
            )


            Session.commit()

            #  log.debug("Finished record")
        except:
            log.exception('Something went wrong!')
            self._save_object_error(
                'Exception in import stage',
                harvest_object
            )
            return False
        return True

    def _get_mapping(self): # ALLEEN ENKELVOUDIGE VELDEN
        if self.md_format == 'dif':
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
		#'author': 'creator', # is a multi-field -> requires different handling
		'license_id': 'rights',
		#'groups': 'subjects',
                #'maintainer': 'publisher',
                #'maintainer_email': 'maintainer_email',
                #'url': 'source',
            }

#    def _extract_author(self, content):
#        if self.md_format == 'dif':
#            dataset_creator = ', '.join(content['Data_Set_Citation/Dataset_Creator'])
#            # TODO: Remove publisher? Is not part of mapping...
#            dataset_publisher = ', '.join(content['Data_Set_Citation/Dataset_Publisher'])
#            if 'not available' not in dataset_creator.lower():
#                return dataset_creator
#            elif 'not available' not in dataset_publisher.lower():
#                return dataset_publisher
#            else:
#                return 'Not available'
#        else:
#            return ', '.join(content['creator'])

    def _extract_license_id(self, content):
        if self.md_format == 'dif':
            use_constraints = ', '.join(content['Use_Constraints'])
            access_constraints = ', '.join(content['Access_Constraints'])
            # TODO: Generalize in own function to check for both
            #       'Not available' and None value
            if 'not available' not in use_constraints.lower() and 'not available' not in access_constraints.lower():
                return '{0}, {1}'.format(use_constraints, access_constraints)
            elif 'not available' not in use_constraints.lower():
                return use_constraints
            elif 'not available' not in access_constraints.lower():
                return access_constraints
        else:
            return content['rights']

    def _extract_tags_and_extras(self, content):
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

    def _extract_formats(self, content):
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

    def _get_possible_resource(self, harvest_obj, content):
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
    def _extract_resources(self, urls, content):
        if self.md_format == 'dif':
            resources = []
            if urls:
                try:
                    resource_formats = self._extract_formats(content)
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
            url = urls[0]
            if url:
                try:
                    # TODO: Use _extract_formats to get format
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

    def _extract_groups(self, content, context):
        if 'series' in content and len(content['series']) > 0:
            return self._find_or_create_groups(
                content['series'],
                context
            )
        return []

    # TODO: Move custom DIF handling to this function
    def _extract_additional_fields(self, content, package_dict):
        # This method is the ideal place for sub-classes to
        # change whatever they want in the package_dict
        return package_dict

    def _find_or_create_groups(self, groups, context):
        log.debug('Group names: %s' % groups)
        group_ids = []
        for group_name in groups:
            data_dict = {
                'id': group_name,
                'name': munge_title_to_name(group_name),
                'title': group_name
            }
            try:
                group = get_action('group_show')(context, data_dict)
                #  log.info('found the group ' + group['id'])
            except:
                group = get_action('group_create')(context, data_dict)
                log.info('created the group ' + group['id'])
            group_ids.append(group['id'])

        #  log.debug('Group ids: %s' % group_ids)
        return group_ids
