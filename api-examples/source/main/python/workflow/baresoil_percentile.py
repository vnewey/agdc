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
from datacube.api.model import DatasetType, Fc25Bands, Ls57Arg25Bands, Satellite
from datacube.api.workflow import TileListCsvTask
from datacube.api.workflow.tile import TileTask
from datacube.api.workflow.cell_chunk import Workflow, SummaryTask, CellTask, CellChunkTask
from enum import Enum
from datacube.api.utils import get_dataset_metadata, get_mask_pqa, get_mask_wofs, get_dataset_ndv, log_mem
from datacube.api.utils import get_dataset_data_masked, raster_create, propagate_using_selected_pixel,get_dataset_data
from datacube.api.utils import NDV, empty_array, calculate_ndvi, date_to_integer, propagate_using_selected_pixel


_log = logging.getLogger()

class Dataset(Enum):
    __order__ = "FC ARG25 SAT DATE"
    FC = "FC"
    ARG25 = "ARG25"
    SAT = "SAT"
    DATE = "DATE"


class BareSoilWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Bare Soil - 2015-05-2")

    def create_summary_tasks(self):

        return [BareSoilSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                   acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                   output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                   mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                   mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                   chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y,
                                   percentile=self.percentile, add_on_name=self.add_on_name)]

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        Workflow.setup_arguments(self)

        self.parser.add_argument("--percentile",
                                 help="Percentile used to calculate the final Bare soil from the time series",
                                 action="store",
                                 dest="percentile",
                                 type=int,
                                 choices=range(1, 100 + 1),
                                 default="90")

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

        self.percentile = args.percentile
        self.add_on_name = args.add_on_name

class BareSoilSummaryTask(SummaryTask):

    add_on_name = luigi.Parameter()
    percentile = luigi.IntParameter()
    def create_cell_tasks(self, x, y):

        return BareSoilCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                               mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                               chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y,
                                percentile=self.percentile, add_on_name=self.add_on_name)


class BareSoilCellTask(CellTask):

    add_on_name = luigi.Parameter()
    percentile = luigi.IntParameter()
    def create_cell_chunk_task(self, x_offset, y_offset):

        return BareSoilCellChunkTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                    satellites=self.satellites,
                                    output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                    mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                    mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                    chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y,
                                    x_offset=x_offset, y_offset=y_offset, percentile=self.percentile,
                                    add_on_name=self.add_on_name)

    def output(self):

        def get_filenames():
            return [self.get_dataset_filename(d) for d in ["FC", "ARG25", "SAT", "DATE"]]

        return [luigi.LocalTarget(filename) for filename in get_filenames()]

    def get_dataset_filename(self, dataset):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        satellites = get_satellite_string(self.satellites)

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        return os.path.join(self.output_directory,
            "{satellites}_{dataset}_{percentile}_{x:03d}_{y:04d}_{acq_min}_{acq_max}{add_on_name}.tif".format(
            satellites=satellites,
            dataset=dataset,
            percentile=self.percentile,
            x=self.x, y=self.y,
            acq_min=acq_min,
            acq_max=acq_max,
            add_on_name=self.add_on_name))


    def run(self):

        print "*** Aggregating chunk NPY files into TIFs"
        # Create output raster - which has len(statistics) bands

        # for each statistic

            # get the band

            # for each chunk file

                # read the chunk
                # write it to the band of the output raster

        # tile = self.get_tiles()[0]
        #
        # filename = tile.datasets[DatasetType.TCI]
        # filename = map_filename_nbar_to_baresoil(filename)
        # filename = os.path.join(self.output_directory, filename)
        #
        # metadata = get_dataset_metadata(filename)

        transform = (self.x, 0.00025, 0.0, self.y+1, 0.0, -0.00025)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        projection = srs.ExportToWkt()

        # Create the output TIF
        shape = (4000, 4000)
        no_data_value = NDV

        best_pixel_fc= empty_array((len(Fc25Bands),4000,4000), dtype=numpy.int16, ndv=NDV)

        best_pixel_nbar = empty_array((len(Ls57Arg25Bands),4000,4000), dtype=numpy.int16, ndv=NDV)

        best_pixel_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        best_pixel_date = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

        SATELLITE_DATA_VALUES = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}
        # TODO

        import itertools

        datasets = [d for d in Dataset]


        for x_offset, y_offset in itertools.product(range(0, 4000, self.chunk_size_x),
                                                        range(0, 4000, self.chunk_size_y)):
            #Do FC
            filename = os.path.join(self.output_directory,
                                    self.get_dataset_chunk_filename(dataset="FC",
                                                                    x_offset = x_offset,
                                                                    y_offset = y_offset))

            _log.info("Processing chunk [%4d|%4d] for [%s] from [%s]", x_offset, y_offset, "FC", filename)

            # read the chunk
            data = numpy.load(filename)
            best_pixel_fc[:, y_offset:y_offset+self.chunk_size_y, x_offset:x_offset+self.chunk_size_x] =data

            _log.info("data is [%s]\n[%s]", numpy.shape(data), data)
            _log.info("Writing it to (%d,%d)", x_offset, y_offset)
            del data

            #Do ARG25
            filename = os.path.join(self.output_directory,
                                    self.get_dataset_chunk_filename(dataset="ARG25",
                                                                    x_offset = x_offset,
                                                                    y_offset = y_offset))


            _log.info("Processing chunk [%4d|%4d] for [%s] from [%s]", x_offset, y_offset, "ARG25", filename)

            # read the chunk
            data = numpy.load(filename)
            best_pixel_nbar[:, y_offset:y_offset+self.chunk_size_y, x_offset:x_offset+self.chunk_size_x] =data

            _log.info("data is [%s]\n[%s]", numpy.shape(data), data)
            _log.info("Writing it to (%d,%d)", x_offset, y_offset)
            del data

            #Do DATE
            filename = os.path.join(self.output_directory,
                                    self.get_dataset_chunk_filename(dataset="DATE",
                                                                    x_offset = x_offset,
                                                                    y_offset = y_offset))


            _log.info("Processing chunk [%4d|%4d] for [%s] from [%s]", x_offset, y_offset, "DATE", filename)

            # read the chunk
            data = numpy.load(filename)
            best_pixel_date[y_offset:y_offset+self.chunk_size_y, x_offset:x_offset+self.chunk_size_x] =data

            #_log.info("data is [%s]\n[%s]", numpy.shape(data), data)
            #_log.info("Writing it to (%d,%d)", x_offset, y_offset)
            del data

            #Do Satellite
            filename = os.path.join(self.output_directory,
                                    self.get_dataset_chunk_filename(dataset="SAT",
                                                                    x_offset = x_offset,
                                                                    y_offset = y_offset))


            _log.info("Processing chunk [%4d|%4d] for [%s] from [%s]", x_offset, y_offset, "SAT", filename)

            # read the chunk
            data = numpy.load(filename)
            best_pixel_satellite[y_offset:y_offset+self.chunk_size_y, x_offset:x_offset+self.chunk_size_x] =data

            _log.info("data is [%s]\n[%s]", numpy.shape(data), data)
            _log.info("Writing it to (%d,%d)", x_offset, y_offset)
            del data


        # FC composite
        raster_create(self.get_dataset_filename("FC"),
                      best_pixel_fc,
                      transform, projection,
                      NDV,
                      gdal.GDT_Int32)


        # ARG25 composite
        raster_create(self.get_dataset_filename("ARG25"),
                      best_pixel_nbar,
                      transform, projection,
                      NDV,
                      gdal.GDT_Int32)

        # Satellite "provenance" composites

        raster_create(self.get_dataset_filename("SAT"),
                      [best_pixel_satellite],
                      transform, projection,
                      NDV,
                      gdal.GDT_Int32)

        # Date "provenance" composites

        raster_create(self.get_dataset_filename("DATE"),
                      [best_pixel_date],
                      transform, projection, NDV,
                      gdal.GDT_Int32)



        # TODO delete .npy files?

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "BARESOIL STATISTICS",
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=self.acq_min, acq_max=self.acq_max),
            "SATELLITE": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "WATER_FILTER": self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or "",
            "STATISTICS": " ".join([s.name for s in Statistic])
        }

    def get_dataset_chunk_filename(self, dataset, x_offset, y_offset):
        from datacube.api.utils import get_satellite_string
        from datacube.api.workflow import format_date


        dataset_name = dataset

        filename = "{satellites}_BARESOIL_{dataset}_{percentile}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
            satellites=get_satellite_string(self.satellites), dataset=dataset_name,
            percentile=self.percentile,
            x=self.x, y=self.y, acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max),
            ulx=x_offset, uly=y_offset,
            lrx=(x_offset + self.chunk_size_x),
            lry=(y_offset + self.chunk_size_y)
        )
        return os.path.join(self.output_directory, filename)



class BareSoilCellChunkTask(CellChunkTask):

    add_on_name = luigi.Parameter()
    percentile = luigi.IntParameter()
    @staticmethod
    def get_dataset_types():

        return [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.NDVI]

    def get_dataset_chunk_filename(self, dataset):
        from datacube.api.utils import get_satellite_string
        from datacube.api.workflow import format_date


        dataset_name = dataset

        filename = "{satellites}_BARESOIL_{dataset}_{percentile}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
            satellites=get_satellite_string(self.satellites), dataset=dataset_name,
            percentile=self.percentile,
            x=self.x, y=self.y, acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max),
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
        for dataset in datasets:
            dataset_name = dataset.name
            for x_offset, y_offset in itertools.product(range(0, 4000, self.chunk_size_x),
                                                            range(0, 4000, self.chunk_size_y)):
                filename = "{satellites}_BARESOIL_{dataset}_{percentile}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
                    satellites=get_satellite_string(self.satellites), dataset=dataset_name,
                    percentile=self.percentile,
                    x=self.x, y=self.y, acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max),
                    ulx=x_offset, uly=y_offset,
                    lrx=(x_offset + self.chunk_size_x),
                    lry=(y_offset + self.chunk_size_y)
                )
                filenames.append(filename)

        return filenames
    def output(self):

        datasets = [d for d in Dataset]
        return [luigi.LocalTarget(filename) for filename in self.get_dataset_chunk_filenames()]

    def run(self):

        shape = (self.chunk_size_x, self.chunk_size_y)
        no_data_value = NDV

        best_pixel_fc = dict()

        for band in Fc25Bands:
            # best_pixel_fc[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=INT16_MIN)
            best_pixel_fc[band] = empty_array(shape=shape, dtype=numpy.float32, ndv=NDV)

        best_pixel_nbar = dict()

        for band in Ls57Arg25Bands:
            best_pixel_nbar[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)

        best_pixel_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        best_pixel_date = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)


        SATELLITE_DATA_VALUES = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}

        stack_bare_soil = []
        stack_nbar =[]
        stack_fc = []
        stack_sat =[]
        stack_date=[]

        metadata_nbar = None
        metadata_fc = None
        index = 0
        no_wofs =0
        for tile in self.get_tiles():
            pqa = tile.datasets[DatasetType.PQ25]
            nbar = tile.datasets[DatasetType.ARG25]
            fc = tile.datasets[DatasetType.FC25]
            wofs = DatasetType.WATER in tile.datasets and tile.datasets[DatasetType.WATER] or None

            _log.info("Processing [%s]", fc.path)

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
                    _log.info("### no wofs data is available, skipping tile [%s]", fc.path)
                    continue
            # Get ARG25 dataset
            log_mem("Before get data")
            data[DatasetType.ARG25] = get_dataset_data_masked(nbar,
                                     x=self.x_offset, y=self.y_offset,
                                     x_size=self.chunk_size_x, y_size=self.chunk_size_y,
                                     mask=mask)
            # _log.info("### ARG25/RED is [%s]", data[DatasetType.ARG25][Ls57Arg25Bands.RED][1000][1000])
            log_mem("After get data")
            # Get the NDVI dataset

            data[DatasetType.NDVI] = calculate_ndvi(data[DatasetType.ARG25][Ls57Arg25Bands.RED],
                                                    data[DatasetType.ARG25][Ls57Arg25Bands.NEAR_INFRARED])
            # _log.info("### NDVI is [%s]", data[DatasetType.NDVI][1000][1000])

            # Add the NDVI value range mask (to the existing mask)

            mask = self.get_mask_range(data[DatasetType.NDVI], min_val=0.0, max_val=0.3, mask=mask)
            #_log.info("### mask ndvi is [%s]", mask[10][10:100])

            #mask = self.get_mask_range(data[DatasetType.ARG25], min_val=0, max_val=10000, mask=mask)

            # Get FC25 dataset

            data[DatasetType.FC25] = get_dataset_data_masked(fc,
                                     x=self.x_offset, y=self.y_offset,
                                     x_size=self.chunk_size_x, y_size=self.chunk_size_y,
                                     mask=mask)

            # _log.info("### FC/BS is [%s]", data[DatasetType.FC25][Fc25Bands.BARE_SOIL][1000][1000])

            # Add the bare soil value range mask (to the existing mask)

            mask = self.get_mask_range(data[DatasetType.FC25][Fc25Bands.BARE_SOIL], min_val=0, max_val=8000, mask=mask)
            # _log.info("### mask BS is [%s]", mask[1000][1000])

            # Apply the final mask to the FC25 bare soil data

            data_bare_soil = numpy.ma.MaskedArray(data=data[DatasetType.FC25][Fc25Bands.BARE_SOIL], mask=mask).filled(NDV)

            # Date "provenance" data
            current_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
            current_date = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

            current_date.fill(date_to_integer(tile.end_datetime))

            # Satellite "provenance" data
            current_satellite.fill(SATELLITE_DATA_VALUES[fc.satellite])

            stack_bare_soil.append(data_bare_soil.copy())
            stack_nbar.append(data[DatasetType.ARG25].copy())
            stack_fc.append(data[DatasetType.FC25].copy())
            stack_sat.append(current_satellite.copy())
            stack_date.append(current_date.copy())


            del data
            index += 1


        log_mem("After processing the tiles")
        _log.info("### wofs tiles missing is [%s]", no_wofs)
        # Compare the bare soil value from this dataset to the current "best" value
        stack_bare_soil_np = numpy.array(stack_bare_soil)
        stack_bare_soil_nan = numpy.where(stack_bare_soil_np==NDV,numpy.nan,stack_bare_soil_np)

        best_pixel_fc[Fc25Bands.BARE_SOIL] = numpy.nanpercentile(stack_bare_soil_nan, self.percentile,
                                                                 axis=0, interpolation='nearest')

        for index in range(len(self.get_tiles())):

            #_log.info("### best pixel bare soil is [%s]", best_pixel_fc[Fc25Bands.BARE_SOIL][100][100])

            # Now update the other best pixel datasets/bands to grab the pixels we just selected

            for band in Ls57Arg25Bands:
                best_pixel_nbar[band] = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BARE_SOIL],
                                                                       stack_bare_soil[index],
                                                                       stack_nbar[index][band],
                                                                       best_pixel_nbar[band])

            for band in [Fc25Bands.PHOTOSYNTHETIC_VEGETATION, Fc25Bands.NON_PHOTOSYNTHETIC_VEGETATION, Fc25Bands.UNMIXING_ERROR]:
                best_pixel_fc[band] = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BARE_SOIL],
                                                                     stack_bare_soil[index],
                                                                     stack_fc[index][band],
                                                                     best_pixel_fc[band])

            # And now the other "provenance" data

            best_pixel_satellite = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BARE_SOIL],
                                                                  stack_bare_soil[index],
                                                                  stack_sat[index],
                                                                  best_pixel_satellite)

            # Date "provenance" data
            best_pixel_date = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BARE_SOIL],
                                                             stack_bare_soil[index],
                                                             stack_date[index],
                                                             best_pixel_date)


        del stack_bare_soil
        del stack_bare_soil_np
        del stack_bare_soil_nan
        del stack_nbar
        del stack_fc
        del stack_sat
        del stack_date
        log_mem("After propagating the best pixel")

        stack = numpy.array([best_pixel_fc[b] for b in Fc25Bands])
        if len(stack) == 0:
            return

        stack_depth, stack_size_y, stack_size_x = numpy.shape(stack)

        _log.info("stack depth [%d] x_size [%d] y size [%d]", stack_depth, stack_size_x, stack_size_y)

        numpy.save(self.get_dataset_chunk_filename("FC"), stack)
        del stack

        stack = numpy.array([best_pixel_nbar[b] for b in Ls57Arg25Bands])
        if len(stack) == 0:
            return
        numpy.save(self.get_dataset_chunk_filename("ARG25"), stack)
        del stack
        numpy.save(self.get_dataset_chunk_filename("SAT"), [best_pixel_satellite])
        numpy.save(self.get_dataset_chunk_filename("DATE"), [best_pixel_date])

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
        "{satellites}_{dataset}_{percentile}_{x:03d}_{y:04d}_{acq_min}_{acq_max}{add_on_name}.tif".format(
        satellites=satellites,
        dataset=dataset,
        percentile=self.percentile,
        x=self.x, y=self.y,
        acq_min=acq_min,
        acq_max=acq_max,
        add_on_name=self.add_on_name))



def map_filename_nbar_to_baresoil(filename, dataset):

    filename = os.path.basename(filename)

    filename = filename.replace(".vrt", ".tif")
    filename = filename.replace(".tiff", ".tif")
    filename = filename.replace(".tif", "_" + dataset + ".tif")

    return filename


# def read_dataset_data(path, bands, x=0, y=0, x_size=None, y_size=None):
def read_dataset_data(path, bands, x=0, y=0, x_size=None, y_size=None):

    """
    Return one or more bands from a raster file

    .. note::
        Currently only support GeoTIFF

    :param path: The path of the raster file from which to read
    :type path: str
    :param bands: The bands to read
    :type bands: list(band)
    :param x: Read data starting at X pixel - defaults to 0
    :type x: int
    :param y: Read data starting at Y pixel - defaults to 0
    :type y: int
    :param x_size: Number of X pixels to read - default to ALL
    :type x_size: int
    :param y_size: Number of Y pixels to read - defaults to ALL
    :int y_size: int
    :return: dictionary of band/data as numpy array
    :rtype: dict(numpy.ndarray)
    """

    #print "#=#=", path, bands

    # out = dict()

    from gdalconst import GA_ReadOnly

    raster = gdal.Open(path, GA_ReadOnly)
    assert raster

    if not x_size:
        x_size = raster.RasterXSize

    if not y_size:
        y_size = raster.RasterYSize

    # for b in bands:
    #
    #     band = raster.GetRasterBand(b.value)
    #     assert band
    #
    #     data = band.ReadAsArray(x, y, x_size, y_size)
    #     out[b] = data
    #
    #     band.FlushCache()
    #     del band

    band = raster.GetRasterBand(1)
    assert band

    data = band.ReadAsArray(x, y, x_size, y_size)
    # out[b] = data

    band.FlushCache()
    del band

    raster.FlushCache()
    del raster

    # return out
    return data



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    BareSoilWorkflow().run()

# if __name__ == '__main__':
#     import luigi.contrib.mpi as mpi
#     mpi.run()

###
# Was playing with this code to avoid the all nan slice issue
###
#
# stack = list()
#
# data = ...
#
# stack.append(data)
# ...
# stack.append(data)
#
# stack = numpy.array(stack)
#
# stack = numpy.rollaxis(stack, 0, 3)
#
# stack[numpy.all(numpy.isnan(stack), axis=2)] = -999
#
# stack = numpy.rollaxis(stack, 2, 0)
