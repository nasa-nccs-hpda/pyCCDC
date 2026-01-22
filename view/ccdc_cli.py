import sys
import logging
import argparse
from pyCCDC.model.CCDCPipeline import CCDCPipeline


def main():

    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Generate synthetic image using CCDC')
    parser.add_argument(
        '--gee_config',
        type=str,
        default=None,
        help='Path to GEE config file that contains account name and key'
    )

    parser.add_argument(
        '--footprint_file',
        type=str,
        default=None,
        required=False,
        help='Path to the file containing the scene footprint ' +
        'coordinates for CCDC generation (*.tif)'
    )

    parser.add_argument(
        '--footprint_dir',
        type=str,
        default=None,
        required=False,
        help='Path to directory containing the scene footprint ' +
        'coordinates for CCDC generation'
    )

    parser.add_argument(
        '--output_path',
        type=str,
        default='./',
        help='Directory path to save the generated synthetic image'
    )

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    # Parse arguments
    args = parser.parse_args()

    # verify arguments
    if args.footprint_file is None and args.footprint_dir is None:
        sys.exit(
            'You need to specify either --footprint_file or --footprint_dir')

    # Initialize pipeline
    ccdc = CCDCPipeline(
        input_dir=args.footprint_dir,
        output_dir=args.output_path
    )

    if args.footprint_file is not None:
        ccdcFile = ccdc.run(args.footprint_file)[0]
        logging.info(f'Finished producing: {ccdcFile}')
    else:
        ccdcFiles = ccdc.run()
        logging.info(
            f'Finished producing {len(ccdcFiles)} files. '
            f'Outputs saved at {args.output_path}.'
        )

    return


if __name__ == "__main__":
    main()
