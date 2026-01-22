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

You can download the container to run this tool from:

```bash
singularity pull docker://nasanccs/vhr-toolkit:latest
```

A downloaded copy has been made available in the Explore cloud under:

```bash
/explore/nobackup/projects/ilab/containers/vhr-toolkit-latest
```

## Quickstart

Using the pyCCDC workflow from a system with Singularity:

```bash
module load singularity
singularity shell -B <WORKING_DIR>:<DATA_PATH> <path-to>/ilab-vhr-toolkit
cd <path_to>/pyCCDC
python simpleCCDC.py
```

An example of running this workflow in the Explore cloud:

```bash
singularity exec -B $NOBACKUP,/explore/nobackup/projects /explore/nobackup/projects/ilab/containers/vhr-toolkit-latest python /explore/nobackup/people/jacaraba/development/VHR-TOOLKIT-FRAMEWORK/pyCCDC/XXXX
```

## Development Example

When testing and developing with different configurations, the following
command can serve as a base to test new changes.

```bash
python view/ccdc_cli.py --gee_config gee_config.json \
--footprint_file "/explore/nobackup/projects/ilab/scratch/vhr-toolkit/WV02_20150911_M1BS_1030010049148A00_sr_02m.tif" \
--output_path /explore/nobackup/projects/ilab/scratch/vhr-toolkit/ccdc-test
```

### Single File

```bash
singularity exec -B $NOBACKUP,/explore/nobackup/projects --env PYTHONPATH=/explore/nobackup/people/$USER/development/VHR-TOOLKIT-FRAMEWORK /explore/nobackup/projects/ilab/containers/vhr-toolkit-latest python /explore/nobackup/people/jacaraba/development/VHR-TOOLKIT-FRAMEWORK/pyCCDC/view/ccdc_cli.py --footprint_file /explore/nobackup/projects/ilab/scratch/vhr-toolkit/WV02_20150911_M1BS_1030010049148A00_sr_02m.tif
```

### Directory of Files

```bash
singularity exec -B $NOBACKUP,/explore/nobackup/projects --env PYTHONPATH=/explore/nobackup/people/$USER/development/VHR-TOOLKIT-FRAMEWORK /explore/nobackup/projects/ilab/containers/vhr-toolkit-latest python /explore/nobackup/people/jacaraba/development/VHR-TOOLKIT-FRAMEWORK/pyCCDC/view/ccdc_cli.py --footprint_dir /explore/nobackup/projects/ilab/scratch/vhr-toolkit
```
