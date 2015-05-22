#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ===============================================================================


__author__ = "Vanessa Newey"


import gdal
import luigi
import logging
import numpy
import os
import osr
from datacube.api.model import Ls57Arg25Bands, DatasetType, Fc25Bands, Satellite
from datacube.api.workflow import TileListCsvTask
from datacube.api.workflow.tile import TileTask
from datacube.api.workflow.cell_chunk import Workflow, SummaryTask, CellTask, CellChunkTask
from enum import Enum
from datacube.api.utils import get_dataset_metadata, get_mask_pqa, get_mask_wofs, get_dataset_ndv, log_mem
from datacube.api.utils import get_dataset_data_masked, raster_create, propagate_using_selected_pixel,get_dataset_data
from datacube.api.utils import NDV, empty_array, calculate_ndvi, date_to_integer, propagate_using_selected_pixel


_log = logging.getLogger()
# class Ls57Arg25Bands(Enum):
#     __order__ = "BLUE GREEN RED NIR SWIR1 SWIR2"
#     BLUE = 1
#     GREEN = 2
#     RED = 3
#     NIR = 4
#     SWIR1 = 5
#     SWIR2 = 6



class Dataset(Enum):
    __order__ = "PERCENTILE_75 DATE75 SENSOR75 MEDIAN DATE_MED SENSOR_MED PERCENTILE_25 DATE25 SENSOR25"
    PERCENTILE_75 = "PERCENTILE_75"
    DATE75 = "DATE75"
    SENSOR75 = "SENSOR75"
    MEDIAN = "MEDIAN"
    DATE_MED = "DATE_MED"
    SENSOR_MED = "SENSOR_MED"
    PERCENTILE_25 = "PERCENTILE_25"
    DATE25 = "DATE25"
    SENSOR25 = "SENSOR25"


class ArgStatsWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="ARG Stats - 2015-05-20")

    def create_summary_tasks(self):

        return [ArgStatsSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                   acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                   output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                   mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                   mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                   chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y,
                                   add_on_name=self.add_on_name)]

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        Workflow.setup_arguments(self)


        self.parser.add_argument("--add_on_name",
                                 help="string added to output filename",
                                 action="store",
                                 dest="add_on_name",
                                 type=str,
                                 default="")


    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        Workflow.process_arguments(self, args)

        self.add_on_name = args.add_on_name

class ArgStatsSummaryTask(SummaryTask):

    add_on_name = luigi.Parameter()
    def create_cell_tasks(self, x, y):

        return ArgStatsCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                               mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                               chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y,
                               add_on_name=self.add_on_name)


class ArgStatsCellTask(CellTask):

    add_on_name = luigi.Parameter()
    def create_cell_chunk_task(self, x_offset, y_offset):

        return ArgStatsCellChunkTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                    satellites=self.satellites,
                                    output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                    mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                    mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                    chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y,
                                    x_offset=x_offset, y_offset=y_offset,
                                    add_on_name=self.add_on_name)

    def output(self):

        def get_filenames():
            return [self.get_dataset_filename(b.name) for b in Ls57Arg25Bands]

        return [luigi.LocalTarget(filename) for filename in get_filenames()]

    def get_dataset_filename(self, band):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        satellites = get_satellite_string(self.satellites)

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        return os.path.join(self.output_directory,
            "{satellites}_ARG25_{band}_STATS_{x:03d}_{y:04d}_{acq_min}_{acq_max}{add_on_name}.tif".format(
            satellites=satellites,
            band=band,
            x=self.x, y=self.y,
            acq_min=acq_min,
            acq_max=acq_max,
            add_on_name=self.add_on_name))


    def run(self):

        print "*** Aggregating chunk NPY files into TIFs"
        # Create output raster - which has len(Dataset) bands

        transform = (self.x, 0.00025, 0.0, self.y+1, 0.0, -0.00025)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        projection = srs.ExportToWkt()

        driver = gdal.GetDriverByName("GTiff")
        assert driver

        datasets = [d for d in Dataset]
        shape = (4000, 4000)
        no_data_value = NDV
        results = dict()

        for band in Ls57Arg25Bands:
            results[band.name]=dict()
            for dataset in datasets:
                results[band.name][dataset.name]=empty_array((len(datasets),4000,4000), dtype=numpy.int16, ndv=NDV)

        SATELLITE_DATA_VALUES = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}


        import itertools

        for band in Ls57Arg25Bands:
            for dataset in Dataset:
                for x_offset, y_offset in itertools.product(range(0, 4000, self.chunk_size_x),
                                                        range(0, 4000, self.chunk_size_y)):
                    #Do FC
                    filename = os.path.join(self.output_directory,
                                            self.get_dataset_chunk_filename(band= band.name,
                                                                            dataset=dataset.name,
                                                                            x_offset = x_offset,
                                                                            y_offset = y_offset))

                    _log.info("Processing chunk [%4d|%4d] for [%s] from [%s]", x_offset, y_offset, band.name, filename)

                    # read the chunk
                    data = numpy.load(filename)
                    results[band.name][dataset.name][:, y_offset:y_offset+self.chunk_size_y, x_offset:x_offset+self.chunk_size_x] =data

                    _log.info("data is [%s]\n[%s]", numpy.shape(data), data)
                    _log.info("Writing it to (%d,%d)", x_offset, y_offset)
                    del data

        for band in Ls57Arg25Bands:

            raster = driver.Create(self.get_dataset_filename(band.name), 4000, 4000, len(Dataset), gdal.GDT_Int32)
            assert raster

            raster.SetGeoTransform(transform)
            raster.SetProjection(projection)

            raster.SetMetadata(self.generate_raster_metadata())

            for index, dataset in enumerate(datasets, start=1):
                _log.info("Doing dataset [%s] which is band [%s]", dataset, index)

                band = raster.GetRasterBand(index)
                assert band

                band.SetNoDataValue(NDV)
                band.SetDescription(dataset.name)
                band.WriteArray(results[band.name][dataset.name])

                band.FlushCache()

            band.ComputeStatistics(True)
            band.FlushCache()
            del band

        raster.FlushCache()
        del raster



        # TODO delete .npy files?

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "ARG STATISTICS",
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=self.acq_min, acq_max=self.acq_max),
            "SATELLITE": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "WATER_FILTER": self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or "",
            "STATISTICS": " ".join([s.name for s in Statistic])
        }

    def get_dataset_chunk_filename(self, band, dataset, x_offset, y_offset):
        from datacube.api.utils import get_satellite_string
        from datacube.api.workflow import format_date

        filename = "{satellites}_ARG_{band}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
            satellites=get_satellite_string(self.satellites),
            dataset=dataset, band=band,
            x=self.x, y=self.y, acq_min=format_date(self.acq_min),
            acq_max=format_date(self.acq_max),
            ulx=x_offset, uly=y_offset,
            lrx=(x_offset + self.chunk_size_x),
            lry=(y_offset + self.chunk_size_y)
        )
        return os.path.join(self.output_directory, filename)


class ArgStatsCellChunkTask(CellChunkTask):

    add_on_name = luigi.Parameter()
    @staticmethod
    def get_dataset_types():

        return [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.NDVI]

    def get_dataset_chunk_filename(self, band, dataset):
        from datacube.api.utils import get_satellite_string
        from datacube.api.workflow import format_date

        filename = "{satellites}_ARG_{band}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
            satellites=get_satellite_string(self.satellites),
            dataset=dataset, band=band,
            x=self.x, y=self.y, acq_min=format_date(self.acq_min),
            acq_max=format_date(self.acq_max),
            ulx=self.x_offset, uly=self.y_offset,
            lrx=(self.x_offset + self.chunk_size_x),
            lry=(self.y_offset + self.chunk_size_y)
        )
        return os.path.join(self.output_directory, filename)

    def get_dataset_chunk_filenames(self):
        from datacube.api.utils import get_satellite_string
        from datacube.api.workflow import format_date

        filenames =[]
        import itertools
        datasets = [d for d in Dataset]
        bands = [b for b in Ls57Arg25Bands]
        for band in bands:
            band_name = band.name
            for dataset in datasets:
                dataset_name = dataset.name
                for x_offset, y_offset in itertools.product(range(0, 4000, self.chunk_size_x),
                                                                range(0, 4000, self.chunk_size_y)):
                    filename = "{satellites}_ARG_{band}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
                        satellites=get_satellite_string(self.satellites),
                        dataset=dataset_name, band = band_name,
                        x=self.x, y=self.y, acq_min=format_date(self.acq_min),
                        acq_max=format_date(self.acq_max),
                        ulx=x_offset, uly=y_offset,
                        lrx=(x_offset + self.chunk_size_x),
                        lry=(y_offset + self.chunk_size_y)
                    )
                    filenames.append(filename)

        return filenames
    def output(self):

        return [luigi.LocalTarget(filename) for filename in self.get_dataset_chunk_filenames()]

    def run(self):

        shape = (self.chunk_size_x, self.chunk_size_y)
        no_data_value = NDV

        best_pixel_fc = dict()

        for band in Fc25Bands:
            # best_pixel_fc[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=INT16_MIN)
            best_pixel_fc[band] = empty_array(shape=shape, dtype=numpy.float32, ndv=NDV)

        results = dict()

        for band in Ls57Arg25Bands:
            results[band.name] = dict()
            for dataset in Dataset:
                results[band.name][dataset.name]=empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)



        SATELLITE_DATA_VALUES = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}


        stack_nbar = dict()
        for band in Ls57Arg25Bands:
            stack_nbar[band.name] = []

        stack_sat =[]
        stack_date=[]

        metadata_nbar = None
        metadata_fc = None
        tile_count = 0
        no_wofs =0
        for tile in self.get_tiles():
            pqa = tile.datasets[DatasetType.PQ25]
            nbar = tile.datasets[DatasetType.ARG25]
            wofs = DatasetType.WATER in tile.datasets and tile.datasets[DatasetType.WATER] or None

            _log.info("Processing [%s]", nbar.path)

            data = dict()

            # Create an initial "no mask" mask

            mask = numpy.ma.make_mask_none((self.chunk_size_x, self.chunk_size_y))
            #_log.info("### mask is [%s]", mask[10][10:100])

            # Add the PQA mask if we are doing PQA masking

            if self.mask_pqa_apply:
                mask = get_mask_pqa(pqa, pqa_masks=self.mask_pqa_mask,
                                    x=self.x_offset, y=self.y_offset,
                                    x_size=self.chunk_size_x, y_size=self.chunk_size_y,
                                    mask=mask)
                #_log.info("### mask PQA is [%s]", mask)

            # Add the WOFS mask if we are doing WOFS masking

            if self.mask_wofs_apply and wofs:
                mask = get_mask_wofs(wofs, wofs_masks=self.mask_wofs_mask,
                                     x=self.x_offset, y=self.y_offset,
                                     x_size=self.chunk_size_x, y_size=self.chunk_size_y,
                                     mask=mask)
            else:
                if self.mask_wofs_apply:
                    no_wofs +=1
                    _log.info("### no wofs data is available, skipping tile [%s]", nbar.path)
                    continue
            # Get ARG25 dataset
            log_mem("Before get data")
            data[DatasetType.ARG25] = get_dataset_data_masked(nbar,
                                     x=self.x_offset, y=self.y_offset,
                                     x_size=self.chunk_size_x, y_size=self.chunk_size_y,
                                     mask=mask)
            # _log.info("### ARG25/RED is [%s]", data[DatasetType.ARG25][Ls57Arg25Bands.RED][1000][1000])
            log_mem("After get data")

         # Date "provenance" data
            current_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
            current_date = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

            current_date.fill(date_to_integer(tile.end_datetime))

            # Satellite "provenance" data
            current_satellite.fill(SATELLITE_DATA_VALUES[nbar.satellite])



            for band in Ls57Arg25Bands:
                stack_nbar[band.name].append(data[DatasetType.ARG25][band].copy())

            stack_sat.append(current_satellite.copy())
            stack_date.append(current_date.copy())


            del data
            tile_count += 1


        log_mem("After processing the tiles")
        if self.mask_wofs_apply:
            _log.info("### wofs tiles missing is [%s]", no_wofs)

        for band in Ls57Arg25Bands:

            #calc stats
            data_array = numpy.array(stack_nbar[band.name])

            data_array_nan = numpy.where(data_array==NDV,numpy.nan,data_array)

            results[band.name]["PERCENTILE_25"] = numpy.nanpercentile(data_array_nan, 75,
                                                                     axis=0, interpolation='nearest')

            results[band.name]["MEDIAN"] = numpy.median(data_array_nan, axis=0)

            results[band.name]["PERCENTILE_25"] = numpy.nanpercentile(data_array_nan, 75,
                                                                     axis=0, interpolation='nearest')

        for index in range(tile_count):


            # Now update the other best pixel datasets/bands to grab the pixels we just selected

            for band in Ls57Arg25Bands:

                results[band.name]["DATE25"]= propagate_using_selected_pixel(results[band.name]["PERCENTILE_25"],
                                                                 stack_nbar[band.name][index],
                                                                 stack_date[index],
                                                                 results[band.name]["DATE25"])


                results[band.name]["DATE75"]= propagate_using_selected_pixel(results[band.name]["PERCENTILE_75"],
                                                                 stack_nbar[band.name][index],
                                                                 stack_date[index],
                                                                 results[band.name]["DATE75"])


                results[band.name]["DATE_MED"]= propagate_using_selected_pixel(results[band.name]["MEDIAN"],
                                                                 stack_nbar[band.name][index],
                                                                 stack_date[index],
                                                                 results[band.name]["DATE_MED"])


                results[band.name]["SENSOR25"]= propagate_using_selected_pixel(results[band.name]["PERCENTILE_25"],
                                                                 stack_nbar[band.name][index],
                                                                 stack_sat[index],
                                                                 results[band.name]["SENSOR25"])


                results[band.name]["SENSOR75"]= propagate_using_selected_pixel(results[band.name]["PERCENTILE_75"],
                                                                 stack_nbar[band.name][index],
                                                                 stack_sat[index],
                                                                 results[band.name]["SENSOR75"])


                results[band.name]["SENSOR_MED"]= propagate_using_selected_pixel(results[band.name]["MEDIAN"],
                                                                 stack_nbar[band.name][index],
                                                                 stack_sat[index],
                                                                 results[band.name]["SENSOR_MED"])




        del data_array
        del data_array_nan
        del stack_nbar
        del stack_sat
        del stack_date
        log_mem("After propagating the best pixel")

        stack = numpy.array([results[b.name]["SENSOR75"] for b in Ls57Arg25Bands])
        if len(stack) == 0:
            return

        stack_depth, stack_size_y, stack_size_x = numpy.shape(stack)

        _log.info("stack depth [%d] x_size [%d] y size [%d]", stack_depth, stack_size_x, stack_size_y)

        for band in Ls57Arg25Bands:
            for dataset in Dataset:
                numpy.save(self.get_dataset_chunk_filename(band.name,dataset.name),results[band.name][dataset.name])
                print self.get_dataset_chunk_filename(band.name,dataset.name)
        del results

        log_mem("DONE")

    @staticmethod
    def get_mask_range(input_data, min_val, max_val, mask=None, ndv=NDV):

        # Create an empty mask if none provided - just to avoid an if below :)
        if mask is None:
            mask = numpy.ma.make_mask_none(numpy.shape(input_data))

        # Mask out any no data values
        data = numpy.ma.masked_equal(input_data, ndv)

        # Mask out values outside the given range
        mask = numpy.ma.mask_or(mask, numpy.ma.masked_outside(data, min_val, max_val, copy=False).mask)

        return mask

def get_dataset_filename(self, dataset):

    from datacube.api.workflow import format_date
    from datacube.api.utils import get_satellite_string

    satellites = get_satellite_string(self.satellites)

    acq_min = format_date(self.acq_min)
    acq_max = format_date(self.acq_max)

    return os.path.join(self.output_directory,
        "{satellites}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}{add_on_name}.tif".format(
        satellites=satellites,
        dataset=dataset,
        x=self.x, y=self.y,
        acq_min=acq_min,
        acq_max=acq_max,
        add_on_name=self.add_on_name))




if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    ArgStatsWorkflow().run()

