## Standardised Precipitation and Evapotranspiration index

The program downloads the latest NetCDF file from a server to the local folder, subsets a time slice, and uses it to create a geotif file in the local folder. The name convention of the file indicates the origin data, and the date that the data relates to.

Install the Python requirements:

``
pip install -r requirements.txt
``

Run the Python script

``
python spei_nc2tif.py
``
