def process_single_file(fpath, output_dir):
    base_fn = os.path.basename(fpath)

    # Assuming raster file name follows the pattern: QB02_YYYYMMDD_?1BS_*.tif
    date_str = base_fn.split('_')[1]

    # Validate date string format
    if not re.match(r'^\d{8}$', date_str):
        raise ValueError("Date string must be in the format YYYYMMDD")
    # Convert date string to YYYY-MM-DD format for gen_single_image function
    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    #Extract bounding box
    coords = _get_coords(fpath)

    #Output file path
    out_fn = base_fn.split('.')[0]+'_ccdc'+'.tif'
    out_fpath = os.path.join(output_dir, out_fn)
    print(out_fpath)

    # Instead of calling self.gen_single_image, we'll return the parameters
    return (date_str, coords, out_fpath)


import multiprocessing

class CCDCPipeline:
    # ... (other methods)

    def process_files(self, wv_list):
        # Determine the number of CPU cores to use
        num_cores = multiprocessing.cpu_count()

        # Create a pool of worker processes
        with multiprocessing.Pool(processes=num_cores) as pool:
            # Use starmap to pass multiple arguments
            results = pool.starmap(
                process_single_file, 
                [(fpath, self.output_dir) for fpath in wv_list]
            )

        # Now process the results sequentially
        for date_str, coords, out_fpath in results:
            self.gen_single_image(date_str, coords, outfile=out_fpath)

    # ... (other methods)
