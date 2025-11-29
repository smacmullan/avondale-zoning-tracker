# Avondale Zoning Tracker
A series of scripts used to keep tabs on zoning and development changes in Avondale, Chicago.

## Using this repository
1. Setup a virtual environment if desired
2. Run `pip install -r requirements.txt` to install dependencies.
3. Run scripts from the script folder (e.g., `python -m scripts.check_zoning_ordinances`)

### Scripts
* **check_zoning_ordinances.py** - check city ordinances for any new and upcoming zoning changes around Avondale

### Disclaimers
* This repository is not guaranteed to identify all zoning proposals in a neighborhood. It relies on geocoding to map city ordinances to neighborhoods, which can fail if the script can't parse a street address or if the geocoder fails to find a match for an address.

## Zoning Data & Community Engagement
Zoning changes are a complex process, and it is difficult for your average citizen to keep track of all the changes happening around them. Different parts of the zoning process produce digital records, which create an opportunity to automatically track new developments and alert the community. Some of these records are **lagging indicators**, meaning they only appear near the end of the zoning process when changes are almost finalized or have already been approved. Some records are **predictive indicators**, meaning they give advance notice of zoning changes or can be used to identify zoning opportunites for the community.

> MPC has a great ["In the Zone" guide](https://metroplanning.org/in-the-zone-a-chicagoans-guide-to-zoning-and-land-use/) that explains how zoning and land use processes work in Chicago.

### Lagging indicators
* **Zoning ordinances** - zoning changes in Chicago are enacted by passing zoning reclassification ordinances.
    * To become law, an ordinance is introduced, referred to committee, and eventually passed. Sometimes ordinances are left indefinitely in a referred state (i.e., they will not be passed). Zoning ordinance approval follows **aldermanic prerogative**, meaning each ward's alder controls which zoning changes are passed in their ward.
    * All city ordinances are recorded on the [City Clerk website](https://chicityclerkelms.chicago.gov/) and accessible via its eLMS API. From an ordinance's webpage, you can view PDFs for the zoning application and plans for a site.
    * Ordinances are often introduced after an alder has decided to approve them. Community meetings are often scheduled ahead of ordinance introduction.
* **Building permits** - city permits are required for signicant changes to a property (new construction, demolitions, modifications).
    * Building permits are available in the Chicago Open Data Portal. These include new construction, demolitions, and modifications to properties.
    * Permits are only available in the data portal after they have been approved. Permits may be issued several months in advance of actually work occuring. Many construction projects are "as-of-right", meaning there's no opportunity for community input.
* **Community meetings** - many alders organize community meetings to help them decide if they should approve a zoning change.
    * There's is no central data source for community meetings. Many alders have weekly newsletters, which can be manually reviewed to look for upcoming community meetings. Alders also usually provide an online zoning input form for those that can't make the in-person meeting.
    * For most zoning changes, it is ultimately up to the alder's discretion if they will have a community meeting. Input from a community meeting is not legally binding (i.e., an alder can disregard feedback from a community meeting if they so choose). While alders generally respect community input and neighbors have successfully blocked zoning changes this way, it should be noted that community meetings are a bit of a false choice. By only collecting feedback near the end the process, communities only have the option to approve or block projects. They have limited say in deciding which types of projects are suggested in the first place.

### Predictive indicators
Some records provide advance notice of potential zoning changes or can be used to identify properties that could be redeveloped for the community (e.g., vacant lots).

TODO
