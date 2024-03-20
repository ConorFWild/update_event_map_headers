import json
from pathlib import Path

import fire
import gemmi
from loguru import logger
import numpy as np
import pandas as pd


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


def recalculate_event_map(dtag_dir, bdc, event_idx):
    dtag = dtag_dir.name

    mean_map_path = dtag_dir / f'{dtag}-ground-state-average-map.native.ccp4'
    xmap_path = (dtag_dir / 'xmap.ccp4').resolve()

    mean_map_ccp4 = gemmi.read_ccp4_map(str(mean_map_path), )
    mean_map_ccp4.setup(0.0)
    xmap_ccp4 = gemmi.read_ccp4_map(str(xmap_path), )
    xmap_ccp4.setup(0.0)

    mean_map_grid = mean_map_ccp4.grid
    xmap_grid = xmap_ccp4.grid

    mean_map_array = np.array(mean_map_grid, copy=False)
    xmap_array = np.array(xmap_grid, copy=False)

    event_map_array = (xmap_array - ((1-bdc) * mean_map_array)) / (bdc)

    event_map_grid = gemmi.FloatGrid(*[xmap_grid.nu, xmap_grid.nv, xmap_grid.nw])
    event_map_grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")
    event_map_grid.set_unit_cell(xmap_grid.unit_cell)
    event_map_grid_array = np.array(event_map_grid, copy=False)
    event_map_grid_array[:, :, :] = event_map_array[:, :, :]

    event_map_path = dtag_dir / f'{dtag}-event_{event_idx}_1-BDC_{bdc}_map.native.ccp4'

    ccp4 = gemmi.Ccp4Map()
    ccp4.grid = event_map_grid
    ccp4.update_ccp4_header()
    ccp4.write_ccp4_map(str(event_map_path))


def update_event_map_spacegroups(pandda_dir):
    # if Path(options_json_path).suffix == ".json":
    #     with open(options_json_path, "r") as f:
    #         options = json.load(f)
    # else:
    #     options = {
    #         "pandda_dir": options_json_path,
    #         "excluded_files": []
    #     }

    options = {
        "pandda_dir": pandda_dir,
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


def recalculate_event_maps(pandda_dir):
    pandda_inspect_table_path = pandda_dir / 'analyses' / 'pandda_inspect_events.csv'
    pandda_inspect_table = pd.read_csv(pandda_inspect_table_path)

    for _idx, _row in pandda_inspect_table.iterrows():
        dtag = _row['dtag']
        dtag_dir = pandda_dir / 'processed_datasets' / _row['dtag']
        bdc = _row['1-BDC']
        event_idx = _row['event_idx']
        print(f'{dtag} : {event_idx} : {bdc} : {round(1-bdc,2)}')
        recalculate_event_map(dtag_dir, bdc, event_idx)

def _get_pandda_dir_type(pandda_dir):
    if (pandda_dir / 'pandda.done').exists():
        return 'pandda_1'
    else:
        return 'pandda_2'


def dispatch(pandda_dir):
    pandda_dir = Path(pandda_dir)
    pandda = _get_pandda_dir_type(pandda_dir)

    print(f'PanDDA type is: {pandda}')

    if pandda == 'pandda_1':
        update_event_map_spacegroups(pandda_dir)

    elif pandda == 'pandda_2':
        print(f'\tRecalculating event maps!')
        recalculate_event_maps(pandda_dir)
    else:
        raise Exception


if __name__ == "__main__":
    fire.Fire(dispatch)
