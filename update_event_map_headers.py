from pathlib import Path

import fire
import gemmi
from loguru import logger


class Constants:
    PANDDA_PROCESSED_DATASETS_DIR = "processed_datasets"


def get_event_map_from_dataset_dir(dataset_dir: Path):
    event_maps = dataset_dir.glob("*event*.ccp4")

    return list(event_maps)


def get_event_map_files(pandda_dir: Path):
    processed_datasets_dir = (
        pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR
    )

    dataset_dirs = processed_datasets_dir.glob("*")

    event_map_files_nested = map(get_event_map_from_dataset_dir, dataset_dirs)

    event_map_files = [
        event_map_file
        for event_map_files in event_map_files_nested
        for event_map_file in event_map_files
    ]

    return event_map_files


def update_event_map_spacegroup(event_map_file: Path):
    logger.debug(f"Updating the header of the event map at: {event_map_file}")
    ccp4 = gemmi.read_ccp4_map(str(event_map_file))

    ccp4.grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")

    ccp4.setup(float("nan"))

    ccp4.update_ccp4_header(2, True)

    ccp4.write_ccp4_map(str(event_map_file))
    logger.debug(f"Updated the event map at: {event_map_file}")


def update_event_map_spacegroups(pandda_dir: str):
    pandda_dir = Path(pandda_dir)
    logger.info(f"PanDDA dir is: {pandda_dir}")

    event_map_files = get_event_map_files(pandda_dir)
    logger.info(f"Got {len(event_map_files)} event map files")

    logger.info(f"Updating...")
    for event_map_file in event_map_files:
        update_event_map_spacegroup(event_map_file)

    logger.info(f"Done!")


if __name__ == "__main__":
    fire.Fire(update_event_map_spacegroups)
