import json
from pathlib import Path

import fire
import gemmi
from loguru import logger
import numpy as np

class Constants:
    PANDDA_PROCESSED_DATASETS_DIR = "processed_datasets"


def get_event_map_from_dataset_dir(dataset_dir: Path):
    event_maps = dataset_dir.glob("*event*.ccp4")

    return list(event_maps)


def get_event_map_files(pandda_dir: Path, excluded_files: list[Path]):
    processed_datasets_dir = (
        pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR
    )

    dataset_dirs = processed_datasets_dir.glob("*")

    event_map_files_nested = map(get_event_map_from_dataset_dir, dataset_dirs)

    event_map_files = [
        event_map_file
        for event_map_files in event_map_files_nested
        for event_map_file in event_map_files
        if event_map_file.resolve() not in excluded_files
    ]

    return event_map_files


def update_event_map_spacegroup(event_map_file: Path):
    logger.debug(f"Updating the header of the event map at: {event_map_file}")
    try:
        ccp4 = gemmi.read_ccp4_map(str(event_map_file))
    except Exception as e:
        error_message = f"""
        Map file {event_map_file} cannot be opened. This means that the map is 
        corrupted. This can be verified by opening it in coot and seeing whether 
        CCP4 library errors are thrown in the console. Part of the map may still 
        display even if this is the case, but the map is nonetheless not suitable 
        for further analysis and should not be used. Once the descision has been 
        made to exclude the map, this error can be prevented by adding the map
        to the "excluded_files" key of the input json.
        """

        logger.error(error_message)

    ccp4.grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")

    ccp4.setup(float("nan"))
    # ccp4.setup(0.0)

    grid = ccp4.grid
    arr = np.array(grid, copy=False)
    arr[np.isnan(arr)] = 0.0

    ccp4.update_ccp4_header(2, True)

    ccp4.write_ccp4_map(str(event_map_file))
    logger.debug(f"Updated the event map at: {event_map_file}")


def update_event_map_spacegroups(options_json_path: str):

    if Path(options_json_path).suffix == ".json":
        with open(options_json_path, "r") as f:
            options = json.load(f)
    else:
        options = {
            "pandda_dir": options_json_path,
            "excluded_files": []
        }


    pandda_dir = options["pandda_dir"]
    exclude = [
        Path(excluded_file).resolve()
        for excluded_file in options["excluded_files"]
    ]

    pandda_dir = Path(pandda_dir)
    logger.info(f"PanDDA dir is: {pandda_dir}")
    logger.info(
        "Excluding files: {excluded_files}".format(
            excluded_files=[str(excluded_file) for excluded_file in exclude]
        )
    )

    event_map_files = get_event_map_files(pandda_dir, excluded_files=exclude)
    logger.info(f"Got {len(event_map_files)} event map files")

    logger.info(f"Updating...")
    for event_map_file in event_map_files:
        update_event_map_spacegroup(event_map_file)

    logger.info(f"Done!")


if __name__ == "__main__":
    fire.Fire(update_event_map_spacegroups)
