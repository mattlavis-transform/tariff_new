# migrate_quota
Application to migrate EU quotas into UK quotas and to migrate UK measures into UK measures

# Usage - migrating measures #

```s
	# Argument 0 is the script
	# Argument 1 is scope type (measuretype, regulation)
	# Argument 2 is action type (terminate or split)
	# Argument 3 is measure type(s) or regulation ID, dependent on argument 2
	# Argument 4 is future regulation ID
	# Argument 5 is the country limiter (if required)
```
## To migrate by measure type ##

Use the following format:

`py migrate_measures.py m r supp N1010101 ZA`

which:

* migrates measures (*the script*) - **argument 0**
* using scope "measure", represented by "m" (can also be "r" for regulations) - **argument 1**
* terminate the existing EU measures and restart under the UK jurisdiction, represented by "r" (*for restart*). This can also be "t" for terminate, in which case EU measures are terminated, but not cloned - **argument 2**
* of all measure types that are of type supplementary unit, here represented by "supp". There are two alternatives to this currently: "cred", which uses the credibility check-related measures and "susp", which uses the measures related to suspensions - **argument 3**
* *and* cloning the measures in to the regulation "N1010101" - **argument 4**
* *and* limiting it such that only measures belonging to the country / geographical area "ZA" (or South Africa are included) - **argument 5**



## To migrate by country

Look in the Excel file "*All regulations - absolute master.xlsx*" for a full list of conversion scripts:

Use the following format:

`py migrate_measures.py c r switzerland P1900340 `

`py migrate_measures.py c r palestine P1900110 `

`py migrate_measures.py c r fiji P1900080 `

`py migrate_measures.py c r papuanewguinea P1900080 `

which:

- migrates measures (*the script*) - **argument 0**
- using scope "country", represented by "c" - **argument 1** - be sure to get the case right (all lower case)
- terminate the existing EU measures and restart under the UK jurisdiction, represented by "r" (*for restart*). This can also be "t" for terminate, in which case EU measures are terminated, but not cloned - **argument 2**
- limiting it such that only measures belonging to the country / geographical area "ZA" (or South Africa)  - **argument 4**. This actually references the JSON config file, which lists all geo. areas that relate to a trade agreement (there may be multiple, such as EEA having specifics for Iceland and Norway as well)
- *and* cloning the measures in to the regulation "N1010101" - **argument 5**

## To migrate by regulation ID

Very similar options to those mentioned above, with the primary change being the use of "r" instead of "m" as argument 2, as follows:

`py migrate_measures.py r t D1700370 N1010101 ZA`

which:

* migrates measures (*the script*) - **argument 0**
* using scope "regulations", represented by "r" - **argument 1**
* terminate the existing EU measures, but do not restart under the UK jurisdiction, represented by "t" (*for terminate*) - **argument 2**
* copy all measures from the regulation whose ID is "D1700370". If this argument uses 7 digits, then the script will use any of the regulations that begin with those 7 digits (0-Z as 8th digit). If 8 digits are used, then only that specific regulation ID will be referenced - **argument 3**
* *and* cloning the measures in to the regulation "N1010101" - **argument 4**
* *and* optionally limiting it such that only measures belonging to the country / geographical area "ZA" (or South Africa are included) - **argument 5**
* There is a special case, **GSP** (Generalised System of Preferences), which requires that the term "gsp" is specific, instead of the regulation ID - this will then go and create data based on the regulation spreadsheet provided in folder **\source\GSP**.

### Other examples

#### Migrating MFNs

This migrates the MFNs to the new regulation M1900010

`py migrate_measures.py m r mfn M1900010`

where in the example above "m" refers to measures and "r" refers to the need to restart those measures.

#### Migrating supplementary units

py migrate_measures.py m r supp I1900010

Supplementary units are required, however there is no clear vehicle to give these items legislative base. In the interim, we will assign all supplementary unit measures to a single measure (and probably not display the legal base on the UKTT)

#### Migrating credibility checks

`py migrate_measures.py m r cred I1900020`

There is some doubt as to whether credibility checks will be required in the UK world, as cred checks should be managed in CDS only and these measures obviously have no impact on CHIEF, however we are keeping them in place anyway for day one - they can always be stopped if necessary.

#### Migrating GSPs

`py migrate_measures.py r r R150982 N1010101`

where the first "r" refers to the fact that we are migrating regulations, and the second refers to the fact that we are restarting measures. The next 2 parameters are the regulation that is being copied from and the final one is the regulation into which is being copied - this only works if GSPs are being carried over as-is, which is unlikely.

# Usage - bulk migrating regulations

Use the script `bulk_migrate.py` to migrate multiple regulations' worth of measures from old EU world to new UK world. This is used as follows:

`py bulk_migrate.py <profile_name>`

where `<profile_name>` references one of the profiles set up in the local config file (`config_migrate_measures_and_quotas.json`) . The two best examples are as follows:

`py bulk_migrate.py import_export` which migrates all import / export data from EU to UK regulations

`py bulk_migrate.py trade_remedies` which migrates all trade remedies regulations from EU to UK regulations.

# Usage - migrating quotas

asdsa