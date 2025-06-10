# CKAN harvesting free configurator for OAI-PMH endpoints / importer


## Harvesting OAIPMH endpoints in DataCite format
The CKAN-importer extension allows for a CKAN-instance to harvest a given endpoint.  
In this particular case in the DataCite metadataformat.  

This extension brings extra flexibility as it can be configured as such, through the use of a specific configuration file, that it can map the harvested information to CKAN-fields.  

The following describes the way this can be realized.


## CKAN schema's and fields
A CKAN-instance can be configured/extended/programmed in such a way that it holds its own datastructure. This can be attained by setting up a application specific CKAN schema (as well as the corresponing SOLR-schema. Be aware that these must coincide!).  

In essence, there are 2 types of CKAN fields  
1) Singular fields i.e. simply holding one piece of information.  
2) Multiple fields i.e. holding a list of pieces of same information.


### Configurator file
Purpose of the configuration file is to direct incoming data from the OAIPMH endpoint to the proper CKAN fields in the correct format.  

Per data item the following is required:

-name of the field within CKAN (schema). I.e. the target field(s)
-name of the field(s) within the DataCite-XML called "select_base". In essence the origin of the data within the XML.
-Type of data (single or multiple)


#### Single field configuration example
In order to do so, the following structure is set up in JSON format:

```
"ckan_field_publication_year": {
   "type": "single",
   "select_base": "publicationYear"
  }
```

In the above example "ckan_field_publication_year" is the target field as defined within the CKAN-schema.
"select_base" defines where the data originates from within the datacite-XML.  
As in this example:


```
<publicationYear>2014</publicationYear>
```

The result:  
In this example the entire harvesting process will add 2014 to the ckan field "ckan_field_publication_year"

#### Special single case: FIXED
Sometimes extra information is required that is not present in the requested data.  
The configuration file allows for 'fixed' data to be added to a specific ckan field.

Example:

```  
 "ckan_year_fixed": {
 "type": "fixed",
 "select_base": "2050"
},

```
This will result in addition of a field ckan_year_fixed holding 2050 as a value.


#### Special single case: First value only
Within Datacite the field 'titles' can hold multiple values.  
Within CKAN however, this is a singular field.

```
"ckan_field_title": {
   "type": "single",
   "select_base": "titles>title"
  }
```
The above configuration will ensure that of a multiple field (within Datacite) only the first mentioned title will be taken into account.  
This brings an interpretation


#### Special single case: point to a specific field in list of sets
Within Datacite the field 'authors' can hold multiple authors where each author is described as a set of descriptive fields. creatorName for instance.

```
"author": {
   "type": "single",
   "select_base": "creators>creator>creatorName"
  }
```
The above configuration will ensure that of a multiple field (within Datacite) only the first mentioned title will be taken into account.  
This brings an interpretation

#### Special single case: prefix the received result
Within Datacite the received data is not fully descriptive.  
For instance, when a doi is received, this has to be prefixed with an endpoint in order to make it meaningful.

```
"url": {
  "type": "single",
  "select_base": "identifier",
  "prefix": "https://doi.org/"
}
```
The above configuration will ensure that of a multiple field (within Datacite) only the first mentioned title will be taken into account.  
This brings an interpretation



### Multiple field definitions

When a CKAN field has the capability to hold a list of values, the following structure in the configuration supports this:

```  
"ckan_formats_array": {
      "type": "array",
      "select_base": "formats>format"
  }

```
In this case, select_base ("formats>format") points to the lowest level where repeating takes place in the XML of the data.  
For example:

```
<formats>
  <format>application/xml</format>
  <format>application/json</format>
</formats>
```
In this particular case, the CKAN field "ckan_formats_array" will be filled with a format-list like:  

 ```
 ["application/xml", "application/json"]
```


### Fields with extended data structure: SET
Another level of data structuring is when a data item consists of several related data-items. I.e. a SET of data.  
An example of this could be:
```
<titles>
  <title xml:lang="en-US">Full DataCite XML Example</title>
  <title xml:lang="nl-NL" titleType="Subtitle">Subtitle in het Nederlands</title>
</titles>
```

The below configuration has an extra "set_definition".  
It defines that, in this case, from the level of each individual title its  language is taken into account as well and added to the CKAN datastructure as defined in the current CKAN-schema.

```
"ckan_titles_set": {
    "type": "array",
    "select_base": "titles>title",
    "set_definition": {
        "ckan_title_name": "#text",
        "ckan_title_language": "@xml:lang"
    }
}
```
This will result in the following output:
```
    "ckan_titles_set": [
        {
            "ckan_title_language": "en-US",
            "ckan_title_name": "Full DataCite XML Example"
        },
        {
            "ckan_title_language": "nl-NL",
            "ckan_title_name": "Demonstration of DataCite Properties."
        }
    ]
```


### Set definition with a deeper level
In below DataCite example of creators it brings more detailed information about a creator. This in the form of extra (deeper lying) elements:

```
<creators>
  <creator>
    <creatorName nameType="Personal">Miller, Elizabeth</creatorName>
    <givenName>Elizabeth</givenName>
    <familyName>Miller</familyName>
    <nameIdentifier schemeURI="http://orcid.org/" nameIdentifierScheme="ORCID">0000-0001-5000-0007</nameIdentifier>
    <affiliation>DataCite</affiliation>
  </creator>
</creators>
```

In order to reach the elements that reside one level deeper than the list element itself, the following configuration can be used:

```
"ckan_creator_set": {
    "type": "array_new",
    "select_base": "creators>creator",
    "set_definition": {
        "ckan_surname": "familyName",
        "ckan_firstname": "givenName",
        "ckan_creatorname": "creatorName#text",
        "ckan_creatorname_type": "creatorName@nameType"
    }
}			
```

This will result in the following output:
```
"ckan_creator_set": [
    {
        "ckan_creatorname": "Miller, Elizabeth",
        "ckan_creatorname_type": "Personal",
        "ckan_firstname": "Elizabeth",
        "ckan_surname": "Miller"
    }
],
```



### Filter on the attribute of a list
In the below example information on a creator has been extended with 3 nameIdentifiers.  

```
<creators>
  <creator>
    <creatorName nameType="Personal">Miller, Elizabeth</creatorName>
    <givenName>Elizabeth</givenName>
    <familyName>Miller</familyName>
    <nameIdentifier schemeURI="http://orcid.org/" nameIdentifierScheme="ORCID">0000-0001-5000-0007</nameIdentifier>
    <nameIdentifier schemeURI="https://ror.org/" nameIdentifierScheme="ROR">https://ror.org/03yrm5c26</nameIdentifier>
    <nameIdentifier nameIdentifierScheme="anyScheme">anyID</nameIdentifier>
    <affiliation>DataCite</affiliation>
  </creator>
</creators>
```
As an example, within a specific CKAN-application, only ORCID is of relevance.  
This requires the capability to select a specific nameIdentifier element based on a specific attribute, like such:

```
"ckan_creator_set": {
    "type": "array",
    "select_base": "creators>creator",
    "set_definition": {
        "ckan_surname": "familyName",
        "ckan_firstname": "givenName",
        "ckan_creatorname": "creatorName#text",
        "ckan_creatorname_type": "creatorName@nameType",
        "ckan_nameIdentifier_ORCID": "nameIdentifier[@nameIdentifierScheme=ORCID]#text"
    }
```
In this example "nameIdentifier[@nameIdentifierScheme=ORCID]#text" the interest goes out to the nameIdentifier#text (i.e. the value of the nameIdentifier itself).  
The part between [] defines which nameIdentifierScheme should be selected. In this case ORCID.  
Only this nameIdentifier is added to the information of this particular creator.

This results in the following:

```
"ckan_creator_set": [
    {
        "ckan_creatorname": "Miller, Elizabeth",
        "ckan_creatorname_type": "Personal",
        "ckan_firstname": "Elizabeth",
        "ckan_nameIdentifier_ORCID": "0000-0001-5000-0007",
        "ckan_surname": "Miller"
    }
],
```
