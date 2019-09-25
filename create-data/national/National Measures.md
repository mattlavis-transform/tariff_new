# National measure extracts

- The following page details the files that are stored in this folder. There are five files in this folder, as follows. Three of the files are facsimiles of the data that is in the latest cut of the Trade Tariff Service (extracted from 31st Jan).

- Unfortunately, it looks like some quite substantial changes to VAT and excise, introduced in the last budget of 2018 went live of 1st February, therefore some additional updates may be required against this data.

**Important** - before loading these files, please read the section below on **load order**, otherwise objects may be missed that later data loads depend upon.

## 4 * files that are extracted in Taric 3 National format

### national_pandr_retain.xml

- national_pandr_retain.xml is a full extract of all national P&R (in its entirety) from the Trade Tariff Service which validates against Taric3National.xsd.

- This contains all current P&R measures that are currently (pre-Brexit) managed nationally and not in Europe.

### excise.xml

- This contains all excise measures as they currently stand (31st January)
- Validates against Taric3National.xsd, therefore uses negative SIDs and should be loaded as a national file
- `called by excise.py`

### vat_vts.xml

- There are 2 * VAT files, necessary to split in order to keep file size down and allow the DIT validator to validate the files against the XSD without crashing.
- This file contains all Standard Rate VAT measures.
- `called by vat.py vts`
- Validates against Taric3National.xsd, therefore uses negative SIDs and should be loaded as a national file

## vat_vtz_vta_vte.xml

- This contains a full list of all (non-VST) VAT measures, i.e.
  - VTA - VAT reduced rate 5%
  - VTE - VAT exempt
  - VTZ - VAT zero rate
- `called by vat.py other`
- Validates against Taric3National.xsd, therefore uses negative SIDs and should be loaded as a national file

## 1 * file that is extracted in Taric 3 EU format

### national_pandr.xml

This is a single file that has been converted such that it now validates against the EU flavour of the Taric 3 XSD. In so doing, the following changes have been made:

- Measures of type CVD (Common Veterinary Entry Document (CVED)) have been removed entirely from the file
- Measures of all other national P&R types have been migrated to the measure types that were loaded last week, as follows:

​                to_list     = ['350', '351', '352', '353', '354', '355', '356', '357', '358', '359', '360', '361', '362', '363']

​                from_list   = ['AHC', 'AIL', 'ATT', 'CEX', 'COE', 'COI', 'CVD', 'EQC', 'HOP', 'HSE', 'PHC', 'PRE', 'PRT', 'QRC']

- where the item in the from_list maps to the equivalent in the to list

- All measures (via the measure_generating_regulation_id) field point to the regulation entitled **I1900040**. This regulation is also included in the **national_pandr.xml** file.

- The file contains, in order, the following elements:

  - 1 * **certificate type** of type 9 (National Documents)
  - Multiple **certificates** of type 9 that are accessed by the conditions attached to the measures (below)
  - 1 * **base regulation** (I1900040), that is the MGR for all measures included in this document
  - 5 * **geographical areas,** which are Taric equivalents of 5 existing geographical area groups, as follows:

  ​        self.geographical_area_id_from_list     = ['F006', 'D065', 'D064', 'D063', 'D010']

  ​        self.geographical_area_id_to_list       = ['N006', 'N065', 'N064', 'N063', 'N010']

  - with equivalent SIDs and description.period SIDs in Taric 3 (not national) format.
  - Multiple **footnotes** of type "FM", footnote type introduced last week (= Footnote for measures). These footnotes are all equivalents of national footnotes that have been relabelled as non-national footnotes by applying to type "FM" and pre-pended "99" before the footnote ID, therefore they use 5-digit footnote IDs.
  - Multiple **measures** that map to the existing national P&R measures. The following is to be noted:
    - All measure transactions also contain the relevant measure condition elements.
    - All measure transactions also contain the relevant measure footnote associations
    - measure transactions also contain the relevant geographical exclusions (however there are none)
    - Similarly, there are no measure components, as the measure types do not require them
    - The CVD measures have all been omitted - these actually did have geographical exclusions, however they were considered to be incorrect, therefore they are being removed and replaced by the equivalent 410-measure type (EU's Veterinary control), which is the same.
    - Measure IDs start from **5,000,000+**, which should ensure that these are namespaced away from the EU's measures that could come between now and Brexit (current max = 3.7 million)
    - Measure condition IDs start from **2,000,000+**. The EU's current max is c. 1.3 million.

# Load order - EU files

- Please load the file national_pandr.xml to the production staging server, but combined within the  file DIT190001.xml, such that the envelope ID sequence is not broken.
- This is the only EU file

# Load order - national files

Please can you load in this order:

*Please note - the envelope ID is set as [ID] in all of these files: you may / will need to alter this, I just don't know what to.*

- **excise.xml**
  - This contains, in this order (*please note, there are some VAT-relevant objects here, but have split into the excise file as VAT data is so vast*)
    - all footnote types relevant to the national VAT and excise measures
    - all measure types relevant to VAT and excise
    - all footnotes relevant to excise (not VAT)
    - the single UK base regulation that supports all VAT and excise measures
    - all excise measures, measure components and measure footnote associations
      - there are no conditions and no geographical exclusions associated with excise (all measures point at 1011: ERGA OMNES)
- **vat_vtz_vta_vte.xml**
  - This contains, in this order:
    - all footnotes related to VAT measures of type VTZ, VTA and VTE
    - all measures related to these VAT types, inclusive of measure components and footnote associations
    - As with excise, there are no conditions and no geo. exclusions
- **vat_vts.xml**
  - This contains, in this order:
    - all footnotes related to VAT measures of type VTS (Standard VAT)
    - all measures related to these VAT types, inclusive of measure components and footnote associations
    - There are no conditions and no geo. exclusions