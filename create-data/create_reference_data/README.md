# migrate_reference_data
Application to migrate reference data to the UK

Individual Python scripts to create reference data files to be appended to standard EU uploads that have been modified to remove post EU Exit measures

Currently works on:

* additional code types
* certificates
* footnote types
* geographical areas
* base_regulations
* goods_nomenclature
* footnotes

Each of these uses an Excel master file in the **source** folder.

Created files are placed in the folder :  **\migrate_and_import_taric\xml_in\custom**



### Naming convention for UK regulations

| **Prefix** | **Regulation type**          |
| ---------- | ---------------------------- |
| **P**      | Preferential Trade Agreement |
| **U**      | Unilateral preferences (GSP) |
| **S**      | Suspensions and reliefs      |
| **X**      | Import and Export control    |
| **N**      | Trade remedies               |
| **M**      | MFN                          |
| **Q**      | Quotas                       |
| **A**      | Agri                         |