import logging
import json
import urllib2
import re

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
from metadata import datacite_ilab
from metadata import iso19139_reader
from pprint import pprint

import traceback

import unicodedata

import requests

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
            for header in self._identifier_generator(client):
                harvest_obj = HarvestObject(
                    guid=header.identifier(),
                    job=harvest_job
                )
                harvest_obj.save()
                log.debug("HDR in gather stage -harvest_obj.id: %s"
                          % harvest_obj.id)
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

        if self.md_format == 'iso19139' and self.md_application == 'EPOS':
            registry.registerReader(self.md_format, iso19139_reader)
            log.debug('Format -> iso19139')
        elif self.md_format == 'datacite' and self.md_application == 'ILAB':
            registry.registerReader(self.md_format, datacite_ilab)
            log.debug('->datacite ILAB reader')
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
            # Differentiation for the metadata handling methods.
            # In essence now there are only two tastes: ILAB and EPOS (which is default).
            self.md_application = config_json.get('application', 'EPOS')

            # Additional info adds possibities to differentiate - this is in essence only for EPOS
            # within a metadata_prefix.
            # Maybe call this variable namespace_info.
            self.additional_info = config_json.get('additional_info',
                                                   'kernel4')
            # TODO: Change default back to 'oai_dc'
            self.force_http_get = config_json.get('force_http_get', False)

	    # EPOS harvesting can reach to GFZ for extra info. This only when configured right
	    self.collect_extra_info_from_gfz = config_json.get('collect_extra_info_from_gfz', '')

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
	    
	    # EPOS - trick to collect extra info via GFZ - solely intended for harvesting of GFZ 
	    log.debug('Extra citation info URL: ' + self.collect_extra_info_from_gfz)

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

            except:
                log.exception('getRecord failed')
                self._save_object_error('Get record failed!', harvest_object)
                return False

            header, metadata, _ = record

            log.debug(record)

            try:
                metadata_modified = header.datestamp().isoformat()
            except:
                metadata_modified = None

            try:
                content_dict = metadata.getMap()

                # HDR? required still?
                content_dict['set_spec'] = header.setSpec()
                if metadata_modified:
                    content_dict['metadata_modified'] = metadata_modified

                content = json.dumps(content_dict,
                                     ensure_ascii=False,
                                     encoding="utf-8")
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

            # Main dictonary holding all package data.
            self.package_dict = {}

            content = json.loads(harvest_object.content)

	    log.debug(content)


	    # Work out default organization based upon current harvest job.
            harvest_source = get_action('harvest_source_show')(
                    context,
                    {'id': harvest_object.job.source.id}
            )

	    content['owner_org'] =  harvest_source['owner_org']  ## Pass extra information (default organization) to handling methods

	    harvest_source_organization = get_action('organization_show')(
		    context,
		    {'id': harvest_source['owner_org'] }
	    )	

	    # Maintainer info (name/email) )to be collected for EPOS through current harvest source organization
            content['maintainer'] = '';
            content['maintainer_email'] = ''
 
	    for index in harvest_source_organization:		
		if index=='extras': # capture email of maintainer
                    for extra_avu in harvest_source_organization['extras']:
                        if extra_avu['key'] == 'email' and extra_avu['state'] == 'active':
                            content['maintainer_email'] = extra_avu['value']
			    break
		        break
		    break
                elif index == 'display_name': # capture name of maintainer
		    content['maintainer'] =  harvest_source_organization['display_name']

            log.info('Maintainer: ' + content['maintainer'])
            log.info('Maintainer: ' + content['maintainer_email'])

            self.package_dict['id'] = munge_title_to_name(harvest_object.guid)
            self.package_dict['name'] = self.package_dict['id']

            if self.md_format == 'datacite' and self.md_application == 'ILAB':
                content['mode'] = 'ILAB'
            elif self.md_format == 'iso19139' and self.md_application == 'EPOS':
                content['mode'] = 'EPOS'

	    # BUILD DATA PACKAGE INCLUDING ALL DEPENDANT DATA (GROUPS ETC)
            self._handleMaintainer(content, context)

            self._handleTitle(content, context)

            self._handleNotes(content, context)

            self._handleLicense(content, context)

            self._handleAuthor(content, context)

            self._handleOwner(content, context)

            self._handleFormats(content, context)

            self._handleUrl(content, context)
	    
            self._handleGroups(content, context)

            self._handleTags(content, context)

            self._handleExtras(content, context)

            # Differentiate further package creation
            # dependent on metadataPrefix.

            '''
            if self.md_format == 'datacite' and self.md_application == 'ILAB':
                self._handle_dataciteILAB(content, context)
            elif self.md_format == 'iso19139' and self.md_application == 'EPOS':
                self._handle_ISO19139EPOS(content, context)
            elif (self.md_format == 'dif' or
                  self.md_format == 'oai_dc' or
                  self.md_format == 'oai_ddi'):
                self._handle_nonEpos(content, context,harvest_object)

                # Add fields according to mapping
                mapping = self._get_mapping()  # mapping used only for exotic formats, no longer for datacite and iso

                for ckan_field, oai_field in mapping.iteritems():
                   try:
                       if (ckan_field == 'maintainer_email' and
                           '@' not in content[oai_field][0]):
                           # Email not available.
                           # Do not set email field as it will break validation.
                           continue
                       else:
                           self.package_dict[ckan_field] = content[oai_field][0]

                except (IndexError, KeyError):
                    continue

            log.debug('after mapping execution')
            '''

            '''
            When using Datacite 3 / 4 this delivers an object queue
            disregarding the namespace. The consequence is that,
            when actually harvesting, not all information is fetched.
            When datacite3 is used->datacite4 records will be empty and
            vice versa. Consequence is that empty records were written
            (added/updated) where this was not valid. By simply checking
            the presence of 'title' should solve this.
            '''
            #log.debug('Create/update package using dict: %s'
            #          % self.package_dict)
            if 'title' in self.package_dict and self.package_dict['title']:
                self._create_or_update_package(
                    self.package_dict,
                    harvest_object
                )

            Session.commit()
        except:
            log.exception('Something went wrong!')
            self._save_object_error(
                'Exception in import stage',
                harvest_object
            )
            return False
        return True

    def _handleMaintainer(self, content, context):
        if content['mode']=='ILAB':
            self.package_dict['maintainer'] = 'Utrecht University'
            self.package_dict['maintainer_email'] = 'info@uu.nl'
        elif content['mode']=='EPOS':
            self.package_dict['maintainer'] = content['maintainer']
            self.package_dict['maintainer_email'] = content['maintainer_email']

    def _handleTitle(self, content, context):
        if content['mode']=='ILAB':
            self.package_dict['title'] = content['title'][0]
        elif content['mode']=='EPOS':
            self.package_dict['title'] = ' '.join(content['title'])

    def _handleNotes(self, content, context):
        if content['mode']=='ILAB':
            self.package_dict['notes'] = content['description'][0]
        elif content['mode']=='EPOS':
            self.package_dict['notes'] = ' '.join(content['description'])

    def _handleLicense(self, content, context):
        self.package_dict['license_id'] = content['rights'][0]

    def _handleAuthor(self, content, context):
        if content['mode']=='ILAB':
            self.package_dict['author'] = '; '.join(content['creator'])
        elif content['mode']=='EPOS':
            authorList = []
            citationContent = ''.join(content['citationContent'])

            citationContent = citationContent.replace('\n', ' ')
            citationContent = citationContent.replace('\t', ' ')
            citationContent = citationContent.strip()
            citationContent = re.sub(' +',' ',citationContent)

            for author in content['creator']:
                # search author and find the university
                parts = citationContent.split(author)

                # search for institute now
                institute = parts[1].split('author')
                instituteName = institute[0].strip()

                # how to make sure that this really is an institute???

                reference = author + ' ' + instituteName + ' ' + 'author'
                # the complete reference string must be found.
                # if not then  the institutename is not an institute

                if (citationContent.find(reference)==-1):
                    authorList.append(author)
                else:
                    authorList.append(author + ' (' + instituteName + ')')

            self.package_dict['author'] = ', '.join(authorList)

    def _handleOwner(self, content, context):
        if content['mode']=='ILAB':
            self.package_dict['owner_org'] = content['owner_org']
        elif content['mode']=='EPOS':
            # OWNER ORGANIZATION (LABS in EPOS)  with 'other-lab' as a default (this must be present in Catalog)
            # Prepare organizations list with default value as last possibility
            organizations = []

            log.debug('Organization: ')
            log.debug(content['org_uuidref'])
            if content['org_uuidref']:
                organizations = content['org_uuidref']

            organizations.append('Other lab')  # 'other-lab' default value

            org_id = self._find_first_entity('organization',
                                             organizations, context)
            log.debug('found org:' + org_id)

            self.package_dict['owner_org'] = org_id

    def _handleFormats(self, content, context):
        if content['mode']=='ILAB':
	    self.package_dict['formats'] = 'datacite'
        elif content['mode']=='EPOS':
            self.package_dict['formats'] = 'ISO19115'

    def _handleUrl(self, content, context):
        if content['doi']:
            self.package_dict['url'] = 'http://doi.org/' + content['doi'][0]

    def _handleGroups(self, content, context):
        groups = []
        if content['groups']:
            log.debug('subjects: %s' % content['groups'])
            groups.extend(
                self._find_or_create_entity('group',
                                            content['groups'],
                                            context)
            )
        self.package_dict['groups'] = groups

    def _handleTags(self, content, context):
        if content['mode']=='ILAB':
            log.debug('Tags:')
            log.debug(content['tags'])
            x = content['tags']
            x = [s.replace('(', '') for s in x]
            x = [s.replace(')', '') for s in x]

            self.package_dict['tags'] = x

        elif content['mode']=='EPOS':
            # TAGS - Hierarchical 'A > B > C > D' to be transformed to separated A, B, C, D
            tags = []
            for tag in content['tags']:
                for token in tag.split('>'):
                    tagItem = token.strip()
                    if tagItem <> "EARTH SCIENCE" and tagItem <> "SOLID EARTH": # as indicated in mappings V1.2, cannot be part of metadata xpath
                        tags.append(tagItem)

            # remove unwanted characters
            tags = [s.replace('(', '') for s in tags]
            tags = [s.replace(')', '') for s in tags]
            tags = [s.replace('/', ' ') for s in tags]
            tags = [s.replace(u'\u2019', ' ') for s in tags]
            tags = [s.replace(u'\u2018', ' ') for s in tags]

            self.package_dict['tags'] = tags

    def _handleExtras(self, content, context):
        extras = []
	if content['mode']=='ILAB':
            if content['geolocationPlaces']:
                extras.append(('Locations covered',
                              ', '.join(content['geolocationPlaces'])))
            if content['contact']:
                extras.append(('Dataset contact',
                              content['contact'][0] + '-' + content['contactAffiliation'][0]))
            if content['created']:
                extras.append(('Created at repository',
                               content['created'][0]))
            if content['publicationYear']:
                extras.append(('Year of publication',
                               content['publicationYear'][0]))
            if content['publisher']:
                extras.append(('Publisher', content['publisher'][0]))
            if content['collectionPeriod']:
                extras.append(('Collection period', content['collectionPeriod'][0]))
            # Add access type
            # This will work with Yoda MOAI as there we control the order of 'rights'
            # 0: license_id   (see above as well
            # 1: Access type
            if len(content['rights'])==2:
                extras.append(('Access type',  content['rights'][1]))
            else:
                extras.append(('Access type',  'Access type not present'))

        elif content['mode']=='EPOS':
            # EXTRAS - for datacite for EPOS -> KEYWORDS -> i.e. customization
            log.debug('-------contactString-----------------------------')
            log.debug(''.join(content['contactString']))
            #-------------------------------------------------- Find contact info within contactString : institute / email
            contactList = []
            contactContent = ''.join(content['contactString'])
            contactContent = contactContent.replace('\n', ' ')
            contactContent = contactContent.replace('\t', ' ')
            contactContent = contactContent.strip()
            contactContent = re.sub(' +',' ', contactContent)
            log.debug(contactContent)

            for contact in content['contact']:
                log.debug('Search contact: ' + contact)
                # search author and find the university
                parts = contactContent.split(contact)

                parts2 = parts[1].split('pointOfContact')

                log.debug(parts2[0])

                contactList.append(contact + ' (' + parts2[0].replace('information','') + ')')

                break       # for now only 1 contact

            #--------------------------------------------------------------
            contactsJoined =  ', '.join(contactList)
            if contactsJoined:
                extras.append(('Dataset contact', contactsJoined))
            if content['created']:
                extras.append(('Created at repository', content['created'][0]))
            if content['publicationYear']:
                extras.append(('Publication date', content['publicationYear'][0]))

            # Fetch extra external information regarding supplement on DOI
            urlDoiBaseGFZ = self.collect_extra_info_from_gfz

            log.info('url: ' + urlDoiBaseGFZ)

            citationTypes = ['supplementTo', 'cites', 'references']

            # cites holds all externally collected info per citationType
            cites = {
                    'supplementTo':'',
                    'cites': '',
                    'references': ''
            }


            for citationType in citationTypes:
                count = 0

                for doi in content[citationType]:
                    count += 1
                    data = ''
                    if len(urlDoiBaseGFZ): # Only perform this for GFZ
                        r = requests.get(urlDoiBaseGFZ + doi)
                        citeData = json.loads(r.text)
                        data = citeData['citation'].replace('https://doi.org/','doi:')
                    prefix = ''
                    if count > 1:
                        prefix= ' -------- '
                    cites[citationType] += prefix + str(count) + ') ' + data

            if cites['supplementTo']:
                extras.append(('Is supplement to',
                               cites['supplementTo']))
            if cites['cites']:
                extras.append(('Cites',
                                cites['cites']))
            if cites['references']:
                extras.append(('References',
                                cites['references']))

            if content['westBoundLongitude']:
                extras.append(('geobox-wLong',
                               content['westBoundLongitude'][0]))
            if content['eastBoundLongitude']:
                extras.append(('geobox-eLong',
                               content['eastBoundLongitude'][0]))
            if content['northBoundLatitude']:
                extras.append(('geobox-nLat',
                               content['northBoundLatitude'][0]))
            if content['southBoundLatitude']:
                extras.append(('geobox-sLat',
                               content['southBoundLatitude'][0]))

            if content['publisher']:
                extras.append(('Publisher', content['publisher'][0]))

        self.package_dict['extras'] = extras

    def _handle_dataciteILAB(self, content, context):
        # MAINTAINER info - datacite for ILAB - harcoded
        self.package_dict['maintainer'] = 'Utrecht University'
        self.package_dict['maintainer_email'] = 'info@uu.nl'

        # TITLE
        self.package_dict['title'] = content['title'][0]
        # NOTES
        self.package_dict['notes'] = content['description'][0]
        # LICENSE_ID
        self.package_dict['license_id'] = content['rights'][0]

        # AUTHOR
        self.package_dict['author'] = '; '.join(content['creator'])

        # OWNER ORGANIZATION through owner of harvest source
	log.debug('dataciteILAB: OWNER_ORG: ' + content['owner_org'])
	self.package_dict['owner_org'] = content['owner_org']

        # FORMAT HDR -> voor datacite niet juist geimplmenteerd
        self.package_dict['formats'] = 'datacite'

        # URL - datacite
        if content['doi']:
            # hardcoded now, do inventory where to find this
            self.package_dict['url'] = 'http://doi.org/' + content['doi'][0]


        # GROUPS/TOPICS
        groups = []
        # create groups based on subjects
        if content['groups']:
            log.debug('subjects: %s' % content['groups'])
            groups.extend(
                    self._find_or_create_entity('group',
                                                content['groups'],
                                                context)
            )
        self.package_dict['groups'] = groups

        # TAGS
        log.debug('Tags:')
        log.debug(content['tags'])
        x = content['tags']
        x = [s.replace('(', '') for s in x]
        x = [s.replace(')', '') for s in x]

        self.package_dict['tags'] = x


        # EXTRAS - for datacite for EPOS -> KEYWORDS -> i.e. customization
        extras = []

        if content['geolocationPlaces']:
            extras.append(('Locations covered',
                          ', '.join(content['geolocationPlaces'])))

        if content['contact']:
            extras.append(('Dataset contact',
                          content['contact'][0] + '-' + content['contactAffiliation'][0]))

        if content['created']:
            extras.append(('Created at repository',
                           content['created'][0]))

        if content['publicationYear']:
            extras.append(('Year of publication',
                           content['publicationYear'][0]))

        if content['publisher']:
            extras.append(('Publisher', content['publisher'][0]))

        if content['collectionPeriod']:
            extras.append(('Collection period', content['collectionPeriod'][0]))

	# Add access type 
	# This will work with Yoda MOAI as there we control the order of 'rights'
	# 0: license_id   (see above as well
	# 1: Access type
	
        if len(content['rights'])==2:
	    extras.append(('Access type',  content['rights'][1]))
        else:
	    extras.append(('Access type',  'Access type not present'))

        self.package_dict['extras'] = extras


    # handle data where metadata prefix = iso - to be defined yet
    def _handle_iso(self, content, context):
        return False  # not yet implemented

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
        self.package_dict['license_id'] = self._nonEpos_extract_license_id(content)

        # FORMATS
        # TODO: Need to map to CKAN author field
        formats = self._nonEpos_extract_formats(content)
        self.package_dict['formats'] = formats

        # RESOURCES
        url = self._nonEpos_get_possible_resource(harvest_object, content)
        self.package_dict['resources'] = self._nonEpos_extract_resources(url, content)

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
                #'title': 'title'
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
            dataset_creator = ', '.join(content['Data_Set_Citation/Dataset_Creator'])
            # TODO: Remove publisher? Is not part of mapping...
            dataset_publisher = ', '.join(content['Data_Set_Citation/Dataset_Publisher'])
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
            if ('not available' not in use_constraints.lower()
            and 'not available' not in access_constraints.lower()):
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
            if False: # url
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
        log.debug(entityType + ' names: %s' % entityNames)

        entityId = '-1'   # Not found - should not be possible
        for entity_name in entityNames:
            #log.debug('labname: ' + entity_name)
            #log.debug( self._utf8_and_remove_diacritics(entity_name) )
            #log.debug( munge_title_to_name(entity_name) )
            data_dict = {
                'id': munge_title_to_name(entity_name),
            }
            log.debug(data_dict)
            try:
                entity = get_action(entityType + '_show')(context.copy(), data_dict)
                log.info('Try: found the ' + entityType + ' with id' + entity['id'])
                entityId = entity['id']
                break
            except Exception as e:
                #log.info('Exception: ' + entity_name)
                #log.info(str(e))
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
            except:
                entity = self._create_entity(entityType, data_dict, context)

            entity_ids.append(entity['id'])

            log.debug(entityType + ' ids: %s' % entity_ids)
        return entity_ids

    # Generic function to create either a group or organization.
    # Dict requires diacritics removed on id
    def _create_entity(self, entityType, entityDict, context):
        try:
            newEntity = get_action(entityType + '_create')(context, entityDict)
            log.info('Created ' + entityTpe + ' with id: ' + newEntity['id'])
        except:
            # entityDict already holds the correct id
            # So if problems during creations
            # return the value already known.
            # Log it though
            log.info('Creation of ' + entityType + ' was troublesome-revert to: ' + entityDict['id'])
            newEntity = {
                'id': entityDict['id']
            }

        return newEntity

    def _utf8_and_remove_diacritics(self, input_str):
        nkfd_form = unicodedata.normalize('NFKD', unicode(input_str))
        return (u"".join([c for c in nkfd_form if not unicodedata.combining(c)])).encode('utf-8')
