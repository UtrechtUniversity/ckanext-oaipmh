import sys
import logging

# from oaipmh.metadata import MetadataReader
from oaipmh import common
from lxml import etree

log = logging.getLogger(__name__)

if sys.version_info[0] == 3:
    text_type = str
else:
    text_type = unicode  # noqa


class Error(Exception):
    pass


class MetadataReader(object):
    """A default implementation of a reader based on fields.
    """
    def __init__(self, fields, namespaces=None):
        self._fields = fields
        self._namespaces = namespaces or {}

    # TODO: Debug the paths for DIF
    def __call__(self, element):
        map = {}
        # create XPathEvaluator for this element
        xpath_evaluator = etree.XPathEvaluator(element,
                                               namespaces=self._namespaces)

        e = xpath_evaluator.evaluate
        # now extra field info according to xpath expr
        for field_name, (field_type, expr) in list(self._fields.items()):
            if field_type == 'bytes':
                value = str(e(expr))
            elif field_type == 'bytesList':
                value = [str(item) for item in e(expr)]
            elif field_type == 'text':
                # make sure we get back unicode strings instead
                # of lxml.etree._ElementUnicodeResult objects.
                value = text_type(e(expr))
            elif field_type == 'textList':
                # make sure we get back unicode strings instead
                # of lxml.etree._ElementUnicodeResult objects.
                value = [unicode(v) for v in e(expr)]  # [text_type(v) for v in e(expr)]
            else:
                raise Error("Unknown field type: %s" % field_type)
            map[field_name] = value
        return common.Metadata(element, map)


# general fields for datacite as used by different readers
datacite_ilabfields = {
         'title':             ('textList', 'datacite:resource/datacite:titles/datacite:title/text()'),  # noqa
         'description':       ('textList', 'datacite:resource/datacite:descriptions/datacite:description/text()'),
         'creator':           ('textList', 'datacite:resource/datacite:creators/datacite:creator/datacite:creatorName/text()'),  # noqa
         'rights':            ('textList', 'datacite:resource/datacite:rightsList/datacite:rights/text()'),  # noqa
         'groups':            ('textList', 'datacite:resource/datacite:subjects/datacite:subject[@subjectScheme="Keyword"]/text()'),
         'tags':              ('textList', 'datacite:resource/datacite:subjects/datacite:subject[@subjectScheme="OECD FOS 2007"]/text()'),
         'doi':               ('textList', 'datacite:resource/datacite:identifier[@identifierType="DOI"]/text()'),
         'created':           ('textList', 'datacite:resource/datacite:dates/datacite:date[@dateType="Created"]/text()'),
	 'collectionPeriod':  ('textList', 'datacite:resource/datacite:dates/datacite:date[@dateType="Collected"]/text()'),
         'publicationYear':   ('textList', 'datacite:resource/datacite:publicationYear/text()'),
#        'supplementTo':      ('textList', 'default:resource/default:relatedIdentifiers/default:relatedIdentifier[@relatedIdentifierType="DOI" and @relationType="IsSupplementTo"]/text()'),
#        'cites':             ('textList', 'default:resource/default:relatedIdentifiers/default:relatedIdentifier[@relatedIdentifierType="DOI" and @relationType="Cites"]/text()'),
#        'references':        ('textList', 'default:resource/default:relatedIdentifiers/default:relatedIdentifier[@relatedIdentifierType="DOI" and @relationType="References"]/text()'),
#        'westBoundLongitude':('textList', 'default:resource/default:geoLocations/default:geoLocation/default:geoLocationBox/default:westBoundLongitude/text()'),
#        'eastBoundLongitude':('textList', 'default:resource/default:geoLocations/default:geoLocation/default:geoLocationBox/default:westBoundLongitude/text()'),
#        'southBoundLatitude':('textList', 'default:resource/default:geoLocations/default:geoLocation/default:geoLocationBox/default:southBoundLatitude/text()'),
#        'northBoundLatitude':('textList', 'default:resource/default:geoLocations/default:geoLocation/default:geoLocationBox/default:northBoundLatitude/text()'),
         'contact':           ('textList', 'datacite:resource/datacite:contributors/datacite:contributor[@contributorType="ContactPerson"]/datacite:contributorName/text()'),
#        'contactAffiliation':('textList', 'default:resource/default:contributors/default:contributor[@contributorType="ContactPerson"]/default:affiliation/text()'),
        'contactEmail':      ('textList', 'datacite:resource/datacite:titles/datacite:title/text()'),
        'publisher':         ('textList', 'datacite:resource/datacite:publisher/text()'),
        'organizations':     ('textList', 'datacite:resource/datacite:contributors/datacite:contributor[@contributorType="HostingInstitution"]/datacite:contributorName/text()'),
        'orgAffiliations':    ('textList', 'datacite:resource/datacite:contributors/datacite:contributor[@contributorType="HostingInstitution"]/datacite:affiliation/text()'),
        'geolocationPlaces':  ('textList', 'datacite:resource/datacite:geoLocations/datacite:geoLocation/datacite:geoLocationPlace/text()'),
    }


datacite_fields = {
        'title':             ('textList', 'default:resource/default:titles/default:title/text()'),  # noqa
        'description':       ('textList', 'default:resource/default:descriptions/default:description/text()'),  # noqa
        'creator':           ('textList', 'default:resource/default:creators/default:creator/default:creatorName/text()'),  # noqa
        'rights':            ('textList', 'default:resource/default:rightsList/default:rights/text()'),  # noqa
        'groups':            ('textList', 'default:resource/default:subjects/default:subject[text()="rock and melt physical properties" or text()="analogue models of geologic processes"]/text()'),
        'tags':              ('textList', 'default:resource/default:subjects/default:subject[not(text()="rock and melt physical properties") and not(text()="analogue models of geologic processes")]/text()'),
        'doi':               ('textList', 'default:resource/default:identifier[@identifierType="DOI"]/text()'),
        'created':           ('textList', 'default:resource/default:dates/default:date[@dateType="Created"]/text()'),
        'publicationYear':   ('textList', 'default:resource/default:publicationYear/text()'),
        'supplementTo':      ('textList', 'default:resource/default:relatedIdentifiers/default:relatedIdentifier[@relatedIdentifierType="DOI" and @relationType="IsSupplementTo"]/text()'),
        'cites':             ('textList', 'default:resource/default:relatedIdentifiers/default:relatedIdentifier[@relatedIdentifierType="DOI" and @relationType="Cites"]/text()'),
        'references':        ('textList', 'default:resource/default:relatedIdentifiers/default:relatedIdentifier[@relatedIdentifierType="DOI" and @relationType="References"]/text()'),
        'westBoundLongitude':('textList', 'default:resource/default:geoLocations/default:geoLocation/default:geoLocationBox/default:westBoundLongitude/text()'),
        'eastBoundLongitude':('textList', 'default:resource/default:geoLocations/default:geoLocation/default:geoLocationBox/default:westBoundLongitude/text()'),
        'southBoundLatitude':('textList', 'default:resource/default:geoLocations/default:geoLocation/default:geoLocationBox/default:southBoundLatitude/text()'),
        'northBoundLatitude':('textList', 'default:resource/default:geoLocations/default:geoLocation/default:geoLocationBox/default:northBoundLatitude/text()'),
        'contact':           ('textList', 'default:resource/default:contributors/default:contributor[@contributorType="ContactPerson"]/default:contributorName/text()'),
        'contactAffiliation':('textList', 'default:resource/default:contributors/default:contributor[@contributorType="ContactPerson"]/default:affiliation/text()'),
        'contactEmail':      ('textList', 'default:resource/default:titles/default:title/text()'),
        'publisher':         ('textList', 'default:resource/default:publisher/text()'),
        'organizations':     ('textList', 'default:resource/default:contributors/default:contributor[@contributorType="HostingInstitution"]/default:contributorName/text()'),
        'orgAffiliations':    ('textList', 'default:resource/default:contributors/default:contributor[@contributorType="HostingInstitution"]/default:affiliation/text()')
    }


iso19139_fields_II= {
        'title':             ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()'),
        'description':       ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString/text()'),
	'creator': 	     ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString'),	
        #'creator':           ('textList', 'string-join(/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="author"]]/gmd:individualName/gco:CharacterString, " (", /gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="author"]]/gmd:organisationName/gco:CharacterString, ") ", ";")'),
	
	'orgAffiliations':    ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString'),
        'organizations':     ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="originator"]]/gmd:organisationName/gco:CharacterString'),
        'doi':               ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString'),
        #'groups':            ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString[text()="rock and melt physical properties" OR text()="analogue models of geologic processes" OR text()="paleomagnetic and magnetic data" OR text()="Geochemical data (elemental and isotope geochemistry)" ]'),

}


iso19139_fields = {
        'title':             ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()'),
        'description':       ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString/text()'),
#        'creator': 	     ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()'),

        'creator':           ('textList', 'concat(gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="author"]]/gmd:individualName/gco:CharacterString/text(), " (", gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="author"]]/gmd:organisationName/gco:CharacterString/text(), ") ")'),

        'rights':            ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString/text()'),
        'groups':	     ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString[text()="rock and melt physical properties" or text()="analogue models of geologic processes" or text()="paleomagnetic and magnetic data" or text()="Geochemical data (elemental and isotope geochemistry)" ]/text()'),
	'tags':              ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()'),
        'doi':               ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()'),
        'created':           ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date[gmd:dateType/gmd:CI_DateTypeCode[text()="creation"]]/gmd:date/gco:Date/text()'),
        'publicationYear':   ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:Date/text()'),


        'supplementTo':      ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation[gmd:associationType/gmd:DS_AssociationTypeCode[text()="IsSupplementTo"]]/gmd:aggregateDataSetIdentifier/gmd:RS_Identifier[gmd:codeSpace/gco:CharacterString[text()="DOI"]]/gmd:code/gco:CharacterString/text()'),

        'references':      ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation[gmd:associationType/gmd:DS_AssociationTypeCode[text()="References"]]/gmd:aggregateDataSetIdentifier/gmd:RS_Identifier[gmd:codeSpace/gco:CharacterString[text()="DOI"]]/gmd:code/gco:CharacterString/text()'),

        'cites':      ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation[gmd:associationType/gmd:DS_AssociationTypeCode[text()="Cites"]]/gmd:aggregateDataSetIdentifier/gmd:RS_Identifier[gmd:codeSpace/gco:CharacterString[text()="DOI"]]/gmd:code/gco:CharacterString/text()'),


#	'cites':             ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation[gmd:associationType/gmd:DS_AssociationTypeCode[text()="Cites"]]/gmd:aggregateDataSetIdentifier/gmd:RS_Identifier[gmd:codeSpace/gco:CharacterString[text()="DOI"]]/gmd:code/gco:CharacterString/text()'),

#	'references':        ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation[gmd:associationType/gmd:DS_AssociationTypeCode[text()="References"]]/gmd:aggregateDataSetIdentifier/gmd:RS_Identifier[gmd:codeSpace/gco:CharacterString[text()="DOI"]]/gmd:code/gco:CharacterString/text()'),

        'westBoundLongitude':('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal/text()'),
        'eastBoundLongitude':('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal/text()'),
        'southBoundLatitude':('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal/text()'),
        'northBoundLatitude':('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal/text()'),
	'contact':           ('textList', 'concat(gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="pointOfContact"]]/gmd:individualName/gco:CharacterString/text(), " (", gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="pointOfContact"]]/gmd:organisationName/gco:CharacterString/text(), ") ", gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="pointOfContact"]]/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString/text())'),
        'contactAffiliation':('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()'),
        'contactEmail':      ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()'),
        'publisher':         ('textList', 'gmd:MD_Metadata/gmd:contact/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact/gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL/text()'),
	'organizations':     ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="originator"]]/gmd:organisationName/gco:CharacterString/text()'), 
        'orgAffiliations':   ('textList', 'gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()')
    }



iso19139_fields__org = {
        'title':             ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString'),
        'description':       ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString'),
        #'creator':           ('textList', 'string-join(/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="author"]]/gmd:individualName/gco:CharacterString, " (", /gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="author"]]/gmd:organisationName/gco:CharacterString, ") ", ";")'), 
        'rights':            ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString'), 
        'groups':            ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString[text()="rock and melt physical properties" OR text()="analogue models of geologic processes" OR text()="paleomagnetic and magnetic data" OR text()="Geochemical data (elemental and isotope geochemistry)" ]'),
        #'tags':              ('textList', 'tokenize(/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString[text() <>"rock and melt physical properties" AND text() <> "analogue models of geologic processes"] AND text()="paleomagnetic and magnetic data" AND text()="Geochemical data (elemental and isotope geochemistry)", " > ")'),
        'doi':               ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString'),
        'created':           ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date[gmd:dateType/gmd:CI_DateTypeCode[text()="creation"]]/gmd:date/gco:Date'),
        'publicationYear':   ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:Date'),
        #'supplementTo':      ('textList', 'string-join("https://doi.org/", /gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation[gmd:associationType/gmd:DS_AssociationTypeCode[text()="IsSupplementTo"]]/gmd:aggregateDataSetIdentifier/gmd:RS_Identifier[codeSpace/gco:CharacterString[text()="DOI"]]/gmd:code/gco:CharacterString)'),
        #'cites':             ('textList', 'string-join("https://doi.org/", /gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation[gmd:associationType/gmd:DS_AssociationTypeCode[text()="Cites"]]/gmd:aggregateDataSetIdentifier/gmd:RS_Identifier[codeSpace/gco:CharacterString[text()="DOI"]]/gmd:code/gco:CharacterString)'),
        #'references':        ('textList', 'string-join("https://doi.org/", /gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation[gmd:associationType/gmd:DS_AssociationTypeCode[text()="References"]]/gmd:aggregateDataSetIdentifier/gmd:RS_Identifier[codeSpace/gco:CharacterString[text()="DOI"]]/gmd:code/gco:CharacterString)'),
        'westBoundLongitude':('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal'),
        'eastBoundLongitude':('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal'),
        'southBoundLatitude':('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal'),
        'northBoundLatitude':('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal'),
        #'contact':           ('textList', 'string-join(/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="pointOfContact"]]/gmd:individualName/gco:CharacterString, " (", /gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="pointOfContact"]]/gmd:organisationName/gco:CharacterString, ") ", /gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="pointOfContact"]]/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString)'),
        'contactAffiliation':('textList', ''),
        'contactEmail':      ('textList', ''),
        'publisher':         ('textList', '/gmd:MD_Metadata/gmd:contact/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact/gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL'),
        'organizations':     ('textList', '/gmd:MD_Metadata/gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[text()="originator"]]/gmd:organisationName/gco:CharacterString'),
        'orgAffiliations':    ('textList', '')
    }
# nieuw toegevoegd: 
# domain

# Anders toegevoegd:



datacite_reader3 = MetadataReader(
    fields =  datacite_fields,
    namespaces={
        'default': 'http://datacite.org/schema/kernel-3'
    }
)

datacite_reader4 = MetadataReader(
    fields =  datacite_fields,
    namespaces={
        'default': 'http://datacite.org/schema/kernel-4',
    }
)


iso19139_reader = MetadataReader(
    fields =  iso19139_fields,
    namespaces={
	'gmd': 'http://www.isotc211.org/2005/gmd', 
	'gco': 'http://www.isotc211.org/2005/gco'
    }
)

datacite_ilab = MetadataReader(
    fields =  datacite_ilabfields,
    namespaces={
        'datacite': 'http://datacite.org/schema/kernel-4',
    }
)



oai_ddi_reader = MetadataReader(
    fields={
        'title':        ('textList', 'oai_ddi:codeBook/stdyDscr/citation/titlStmt/titl/text()'),  # noqa
        'creator':      ('textList', 'oai_ddi:codeBook/stdyDscr/citation/rspStmt/AuthEnty/text()'),  # noqa
        'subject':      ('textList', 'oai_ddi:codeBook/stdyDscr/stdyInfo/subject/keyword/text()'),  # noqa
        'description':  ('textList', 'oai_ddi:codeBook/stdyDscr/stdyInfo/abstract/text()'),  # noqa
        'publisher':    ('textList', 'oai_ddi:codeBook/stdyDscr/citation/distStmt/contact/text()'),  # noqa
        'contributor':  ('textList', 'oai_ddi:codeBook/stdyDscr/citation/contributor/text()'),  # noqa
        'date':         ('textList', 'oai_ddi:codeBook/stdyDscr/citation/prodStmt/prodDate/text()'),  # noqa
        'series':       ('textList', 'oai_ddi:codeBook/stdyDscr/citation/serStmt/serName/text()'),  # noqa
        'type':         ('textList', 'oai_ddi:codeBook/stdyDscr/stdyInfo/sumDscr/dataKind/text()'),  # noqa
        'format':       ('textList', 'oai_ddi:codeBook/fileDscr/fileType/text()'),  # noqa
        'identifier':   ('textList', "oai_ddi:codeBook/stdyDscr/citation/titlStmt/IDNo/text()"),  # noqa
        'source':       ('textList', 'oai_ddi:codeBook/stdyDscr/dataAccs/setAvail/accsPlac/@URI'),  # noqa
        'language':     ('textList', 'oai_ddi:codeBook/@xml:lang'),  # noqa
        'tempCoverage': ('textList', 'oai_ddi:codeBook/stdyDscr/stdyInfo/sumDscr/timePrd/text()'),  # noqa
        'geoCoverage':  ('textList', 'oai_ddi:codeBook/stdyDscr/stdyInfo/sumDscr/geogCover/text()'),  # noqa
        'rights':       ('textList', 'oai_ddi:codeBook/stdyInfo/citation/prodStmt/copyright/text()')   # noqa
    },
    namespaces={
        'oai_ddi': 'http://www.icpsr.umich.edu/DDI',
    }
)

# Note: maintainer_email is not part of Dublin Core
oai_dc_reader = MetadataReader(
    fields={
        'title':            ('textList', 'oai_dc:dc/dc:title/text()'),  # noqa
        'creator':          ('textList', 'oai_dc:dc/dc:creator/text()'),  # noqa
        'subject':          ('textList', 'oai_dc:dc/dc:subject/text()'),  # noqa
        'description':      ('textList', 'oai_dc:dc/dc:description/text()'),  # noqa
        'publisher':        ('textList', 'oai_dc:dc/dc:publisher/text()'),  # noqa
        'maintainer_email': ('textList', 'oai_dc:dc/oai:maintainer_email/text()'),  # noqa
        'contributor':      ('textList', 'oai_dc:dc/dc:contributor/text()'),  # noqa
        'date':             ('textList', 'oai_dc:dc/dc:date/text()'),  # noqa
        'type':             ('textList', 'oai_dc:dc/dc:type/text()'),  # noqa
        'format':           ('textList', 'oai_dc:dc/dc:format/text()'),  # noqa
        'identifier':       ('textList', 'oai_dc:dc/dc:identifier/text()'),  # noqa
        'source':           ('textList', 'oai_dc:dc/dc:source/text()'),  # noqa
        'language':         ('textList', 'oai_dc:dc/dc:language/text()'),  # noqa
        'relation':         ('textList', 'oai_dc:dc/dc:relation/text()'),  # noqa
        'coverage':         ('textList', 'oai_dc:dc/dc:coverage/text()'),  # noqa
        'rights':           ('textList', 'oai_dc:dc/dc:rights/text()')  # noqa
    },
    namespaces={
    'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'dc': 'http://purl.org/dc/elements/1.1/'}
)


xpath_prefix = "//*[name()='metadata']/*[name()='DIF']"
# TODO: Can add what ever fields are needed
dif_reader = MetadataReader(
    fields={
        'title':            ('textList', "//*[name()='Entry_Title']/text()"), # noqa
        #  'title':            ('textList', "OAI-PMH"), # noqa
        'creator':          ('textList', xpath_prefix + "/*[name()='Data_Set_Citation']/*[name()='Dataset_Creator']/text()"), # noqa
        'subject':          ('textList', xpath_prefix + "/*[name()='Keyword']/text()"), # noqa
        'description':      ('textList', xpath_prefix + "/*[name()='Summary']/*[name()='Abstract']/text()"), # noqa
        'publisher':        ('textList', xpath_prefix + "/*[name()='Data_Set_Citation']/*[name()='Dataset_Publisher']/text()"), # noqa
        'maintainer_email': ('textList', xpath_prefix + "/*[name()='Personnel']/*[name()='Email']/text()"), # noqa
        'contributor':      ('textList', xpath_prefix + "/*[name()='Personnel']/*[name()='Last_Name']/text()"), # noqa TODO
        'date':             ('textList', xpath_prefix + "/*[name()='Data_Set_Citation']/*[name()='Dataset_Release_Date']/text()"), # noqa
        #  'type':             ('textList', ""), # noqa TODO
        'format':           ('textList', xpath_prefix + "/*[name()='Related_URL']/*[name()='URL_Content_Type']/*[name()='Subtype']/text()"), # noqa TODO
        # Identifier is actually resource url...
        'identifier':       ('textList', xpath_prefix + "/*[name()='Related_URL']/*[name()='URL']/text()"), # noqa TODO
        'source':           ('textList', xpath_prefix + "/*[name()='Related_URL']/*[name()='URL']/text()"), # noqa
        'language':         ('textList', xpath_prefix + "/*[name()='Data_Set_Language']/text()"), # noqa
        #  'relation':         ('textList', ""), # noqa TODO
        'coverage':         ('textList', xpath_prefix + "/*[name()='Location']/*[name()='Location_Type']/text()"), # noqa TODO
        'rights':           ('textList', xpath_prefix + "/*[name()='Access_Constraints']/text()"), # noqa
    },
    namespaces={
        # TODO: Not used...
        'dif': 'https://gcmd.nasa.gov/Aboutus/xml/dif/'
    }
)


# Builds an xpath based on a list of elements
# Meant for xml parsing without namespaces
def _xpath_bulder(elms):
    path = ""
    for i, elm in enumerate(elms):
        if i == len(elms) - 1:
            path += "/"
            path += elm
        else:
            path += "//*[name()='"
            path += elm
            path += "']"
    return path


def _eval_builder(field_type, elms):
    return (field_type, _xpath_bulder(elms))


# TODO: Should use absolute paths whenever possible.
#       Helps avoid namespace conflicts.
dif_reader2 = MetadataReader(
    fields={
        # Basic info
        "Entry_ID": _eval_builder('textList', ['Entry_ID', 'text()']),
        "Entry_Title": _eval_builder('textList', ['Entry_Title', 'text()']),

        # Dataset citation
        "Data_Set_Citation/Dataset_Creator": _eval_builder('textList', ['Dataset_Creator', 'text()']),
        "Data_Set_Citation/Dataset_Title": _eval_builder('textList', ['Dataset_Title', 'text()']),
        "Data_Set_Citation/Dataset_Release_Date": _eval_builder('textList', ['Dataset_Release_Date', 'text()']),
        "Data_Set_Citation/Dataset_Release_Place": _eval_builder('textList', ['Dataset_Release_Place', 'text()']),
        "Data_Set_Citation/Dataset_Publisher": _eval_builder('textList', ['Dataset_Publisher', 'text()']),
        "Data_Set_Citation/Version": _eval_builder('textList', ['Version', 'text()']),

        # Personnel
        "Personnel/Role": _eval_builder('textList', ['Personnel', 'Role', 'text()']),
        "Personnel/First_Name": _eval_builder('textList', ['Personnel', 'First_Name', 'text()']),
        "Personnel/Last_Name": _eval_builder('textList', ['Personnel', 'Last_Name', 'text()']),
        "Personnel/Email": _eval_builder('textList', ['Personnel', 'Email', 'text()']),
        "Personnel/Phone": _eval_builder('textList', ['Personnel', 'Phone', 'text()']),

        # Personnel contact address
        "Personnel/Contact_Address/Address": _eval_builder('textList', ['Personnel', 'Contact_Address', 'Address', 'text()']),
        "Personnel/Contact_Address/City": _eval_builder('textList', ['Personnel', 'Contact_Address', 'City', 'text()']),
        "Personnel/Contact_Address/Postal_Code": _eval_builder('textList', ['Personnel', 'Contact_Address', 'Postal_Code', 'text()']),
        "Personnel/Contact_Address/Country": _eval_builder('textList', ['Personnel', 'Contact_Address', 'Country', 'text()']),

        # Parameters
        "Keyword": _eval_builder('textList', ['Keyword', 'text()']),

        # Termporal coverage
        "Temporal_Coverage/Start_Date": _eval_builder('textList', ['Temporal_Coverage', 'Start_Date', 'text()']),
        "Temporal_Coverage/Stop_Date": _eval_builder('textList', ['Temporal_Coverage', 'Stop_Date', 'text()']),

        # Data_Set_Progress
        "Data_Set_Progress": _eval_builder('textList', ['Data_Set_Progress', 'text()']),

        # Spatial_Coverage
        "Spatial_Coverage/Southernmost_lat": _eval_builder('textList', ['Spatial_Coverage', 'Southernmost_Latitude', 'text()']),
        "Spatial_Coverage/Northernmost_lat": _eval_builder('textList', ['Spatial_Coverage', 'Northernmost_Latitude', 'text()']),
        "Spatial_Coverage/Westernmost_lon": _eval_builder('textList', ['Spatial_Coverage', 'Westernmost_Longitude', 'text()']),
        "Spatial_Coverage/Easternmost_lon": _eval_builder('textList', ['Spatial_Coverage', 'Easternmost_Longitude', 'text()']),

        # Project
        "Project/Short_Name": _eval_builder('textList', ['Project', 'Short_Name', 'text()']),
        "Project/Long_Name": _eval_builder('textList', ['Project', 'Long_Name', 'text()']),

        "Access_Constraints": _eval_builder('textList', ['Access_Constraints', 'text()']),
        "Use_Constraints": _eval_builder('textList', ['Use_Constraints', 'text()']),
        "Data_Set_Language": _eval_builder('textList', ['Data_Set_Language', 'text()']),
        "Originating_Center": _eval_builder('textList', ['Originating_Center', 'text()']),

        # Data center
        "Data_Center/Data_Center_Name/Short_Name": _eval_builder('textList', ['Data_Center', 'Data_Center_Name', 'Short_Name', 'text()']),
        "Data_Center/Data_Center_Name/Long_Name": _eval_builder('textList', ['Data_Center', 'Data_Center_Name', 'Long_Name', 'text()']),
        "Data_Center/Data_Center_URL": _eval_builder('textList', ['Data_Center', 'Data_Center_URL', 'text()']),
        # Personnel
        "Data_Center/Personnel/Role": _eval_builder('textList', ['Data_Center', 'Personnel', 'Role', 'text()']),
        "Data_Center/Personnel/First_Name": _eval_builder('textList', ['Data_Center', 'Personnel', 'First_Name', 'text()']),
        "Data_Center/Personnel/Last_Name": _eval_builder('textList', ['Data_Center', 'Personnel', 'Last_Name', 'text()']),
        "Data_Center/Personnel/Email": _eval_builder('textList', ['Data_Center', 'Personnel', 'Email', 'text()']),
        "Data_Center/Personnel/Phone": _eval_builder('textList', ['Data_Center', 'Personnel', 'Phone', 'text()']),
        "Data_Center/Personnel/Contact_Address/Address": _eval_builder('textList', ['Data_Center', 'Personnel', 'Contact_Address', 'Address', 'text()']),
        "Data_Center/Personnel/Contact_Address/City": _eval_builder('textList', ['Data_Center', 'Personnel', 'Contact_Address', 'City', 'text()']),
        "Data_Center/Personnel/Contact_Address/Postal_Code": _eval_builder('textList', ['Data_Center', 'Personnel', 'Contact_Address', 'Postal_Code', 'text()']),
        "Data_Center/Personnel/Contact_Address/Country": _eval_builder('textList', ['Data_Center', 'Personnel', 'Contact_Address', 'Country', 'text()']),

        # Reference
        # TODO: Might contain 'Author' etc.
        "Reference": _eval_builder('textList', ['Reference', 'text()']),

        # Summary
        "Summary/Abstract": _eval_builder('textList', ['Summary', 'Abstract', 'text()']),

        # Related URLs
        "Related_URL/URL_Content_Type/Type": _eval_builder('textList', ['Related_URL', 'URL_Content_Type', 'Type', 'text()']),
        # TODO: Add empty subtype if it does not exist.
        #       How to connect Type and its corrent Subtype. The indices are currently wrong.
        "Related_URL/URL_Content_Type/Subtype": _eval_builder('textList', ['Related_URL', 'URL_Content_Type', 'Subtype', 'text()']),
        # TODO: Same for this
        "Related_URL/URL": _eval_builder('textList', ['Related_URL', 'URL', 'text()']),
        # TODO: Same for this
        "Related_URL/Description": _eval_builder('textList', ['Related_URL', 'Description', 'text()']),

        # IDN Node
        # TODO: Usually not displayed to the user
        "IDN_Node/Short_Name": _eval_builder('textList', ['IDN_Node', 'Short_Name', 'text()']),

        # Etc
        "Metadata_Name": _eval_builder('textList', ['Metadata_Name', 'text()']),
        "Metadata_Version": _eval_builder('textList', ['Metadata_Version', 'text()']),
        "DIF_Creation_Date": _eval_builder('textList', ['DIF_Creation_Date', 'text()']),
        "Last_DIF_Revision_Date": _eval_builder('textList', ['Last_DIF_Revision_Date', 'text()']),
        "Private": _eval_builder('textList', ['Private', 'text()']),
        "ISO_Topic_Category": _eval_builder('textList', ['ISO_Topic_Category', 'text()']),

        # Missing...
        #  "Distribution": ('textList', "//*[name()='Distribution']/text()"),
        #  "Extended_Metadata": ('textList', "//*[name()='Extended_Metadata']/text()"),
        #  "Location": ('textList', "//*[name()='Location']/text()"),
        #  "Metadata_Association": ('textList', "//*[name()='Metadata_Association']/text()"),
        #  "Metadata_Dates": ('textList', "//*[name()='Metadata_Dates']/text()"),
        #  "Multimedia_Sample": ('textList', "//*[name()='Multimedia_Sample']/text()"),
        #  "Originating_Metadata_Node": ('textList', "//*[name()='Originating_Metadata_Node']/text()"),
        #  "Platform": ('textList', "//*[name()='Platform']/text()"),
        #  "Product_Flag": ('textList', "//*[name()='Product_Flag']/text()"),
        #  "Product_Level_Id": ('textList', "//*[name()='Product_Level_Id']/text()"),
        #  "Quality": ('textList', "//*[name()='Quality']/text()"),
        #  "Reference": ('textList', "//*[name()='Reference']/text()"),
        #  "Science_Keywords": ('textList', "//*[name()='Science_Keywords']/text()"),
        #  "Version_Description": ('textList', "//*[name()='Version_Description']/text()"),
        #  "DIF_Revision_History": ('textList', "//*[name()='DIF_Revision_History']/text()"),
        #  "Collection_Data_Type": ('textList', "//*[name()='Collection_Data_Type']/text()"),
    },
    namespaces={
        'dif': 'https://gcmd.nasa.gov/Aboutus/xml/dif/'
    }
)
