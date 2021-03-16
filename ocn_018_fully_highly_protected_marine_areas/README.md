## Fully and Highly Protected Marine Areas Dataset Pre-processing
This file describes the data pre-processing that was done to the Marine Protection Atlas dataset for display on Resource Watch. **Note that this processing was done manually, but will transition to regular, automated updates when MPAtlas finalizes their release practices.**

The source provided the data as a shapefile, which contained _all_ [marine protected areas (MPAs) in the World Database on Protected Areas (WDPA)](https://www.protectedplanet.net/en/thematic-areas/marine-protected-areas). Because [the full WDPA](https://resourcewatch.org/data/explore/bio007-World-Database-on-Protected-Areas_replacement) and [the MPA subset](https://resourcewatch.org/data/explore/bio007b-Marine-Protected-Areas) are already displayed on Resource Watch, this full set was filtered to include only those where the `no-take` attribute was "All" or "Part", retaining only MPAs containing some sort of no-take zone. These entries were saved to a separate shapefile, which was [uploaded to Carto](https://resourcewatch.carto.com/u/rw-nrt/dataset/ocn_018_fully_highly_protected_marine_areas).

You can view the processed Fully and Highly Protected Marine Areas dataset [on Resource Watch](https://resourcewatch.org/data/explore/4429cf8f-7537-485f-b98a-5e67c56290b9).

###### Note: This dataset processing was done by [Peter Kerins](https://www.wri.org/profile/peter-kerins).
