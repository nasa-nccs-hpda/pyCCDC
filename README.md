---
title: pyCCDC
purpose: synthetic imagery generation based on CCDC model results
---

# pyCCDC

Python module provides a set of functions for working with Earth Engine (ee) 
and performing time series analysis using the CCDC algorithm

## Objectives

- Generate synthetic imagery at given date & ROI

## Containers

### ilab-vhr-toolkit 

## Quickstart

```bash
module load singularity
singularity shell -B <WORKING_DIR>:<DATA_PATH> <path-to>/ilab-vhr-toolkit
cd <path_to>/pyCCDC
python simpleCCDC.py
```

## Development Example

When testing and developing with different configurations, the following
command can serve as a base to test new changes.

```bash
python view/ccdc_cli.py --gee_config gee_config.json \
--footprint_file "/explore/nobackup/projects/ilab/scratch/vhr-toolkit/WV02_20150911_M1BS_1030010049148A00_sr_02m.tif" \
--output_path /explore/nobackup/projects/ilab/scratch/vhr-toolkit/ccdc-test
```

## Contributors

### jli-99@github
