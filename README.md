# Sat-Image-Processor

Extracts MGRS grid from AWS Sentinel-2 urls in fires.json. Then, it converts the MGRS grid to Lat/Lon and uses SentinelSat to query Copernicus Hub to get the satellite images. It then processes the relevant bands to create an Infrared image to make it easier for a CNN to distinguish wildfires.
