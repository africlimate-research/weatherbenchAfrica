import os
import xarray as xr
import numpy as np
from typing import Mapping, Hashable, Optional, Union
from weatherbenchAfrica.data_loaders.base import DataLoader  


class IMERGLoader(DataLoader):
    """DataLoader implementation for IMERG precipitation data."""

    def __init__(
        self,
        data_dir: str,
        variable: str = "precipitation",
        time_dim: str = "time",
        compute: bool = True,
        interpolation=None,
        add_nan_mask: bool = False,
    ):
        """
        Args:
            data_dir: Directory containing IMERG NetCDF files.
            variable: Name of the precipitation variable.
            time_dim: Time dimension name.
            compute: Whether to load into memory.
            interpolation: Optional interpolation object.
            add_nan_mask: Whether to add a NaN mask to each variable.
        """
        super().__init__(interpolation=interpolation, compute=compute, 
                         add_nan_mask=add_nan_mask)
        self.data_dir = data_dir
        self.variable = variable
        self.time_dim = time_dim

        # Load once for indexing efficiency (optional)
        self.ds = xr.open_mfdataset(
            os.path.join(self.data_dir, "*.nc4"),
            combine="by_coords"
        )[self.variable]

    def _load_chunk_from_source(
        self,
        init_times: np.ndarray,
        lead_times: Optional[Union[np.ndarray, slice]] = None,
    ) -> Mapping[Hashable, xr.DataArray]:
        """
        Load chunk of IMERG data based on init_times and optional lead_times.

        Args:
            init_times: Array of initialization times (np.datetime64).
            lead_times: Optional lead times (not used in IMERG, unless needed for aggregation).

        Returns:
            Dictionary with variable name as key and DataArray as value.
        """
        # Select times

        sel_ds = self.ds.sel(time=slice(init_times[0], init_times[1]))

        # If lead_times are defined (e.g. 6h/12h later), offset them from init_times
        # Here it's optional, since IMERG is observation not forecast
        if lead_times is not None:
            raise NotImplementedError("Lead time slicing not implemented for IMERGLoader.")

        return {self.variable: sel_ds}

