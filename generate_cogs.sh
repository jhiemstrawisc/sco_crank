#!/bin/bash
# Script to convert input tif image into cloud optimized format and export jpeg version
#
# Inputs Expected:
# $1 = input file to process
# $2 = output cog file
# $3 = output jpeg file
# $4 = number of bands in original file

# convert original tif to COG format
gdal_translate $1 $2 -of COG -CO compress=lzw
rm $1
mv $2 $1
numbands=$4

if [ "$numbands" = "1" ] || [ "$numbands" = "3" ]; then
	# Generate jpeg from whatever bands are present... rgb color (3) or greyscale (1)
	gdal_translate $1 $3 -of jpeg -CO quality=75
else
	# create 3-band jpeg from original 4-band input
	gdal_translate $1 $3 -of jpeg -CO quality=75 -b 1 -b 2 -b 3
fi

rm *.xml
